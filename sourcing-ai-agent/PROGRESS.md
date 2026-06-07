# Sourcing AI Agent Dev Progress

> Status: Living tracker. Use the latest entries as the source of truth, and assume older bullets may describe superseded intermediate states.

## 2026-06-08 (Asia/Shanghai)

### Service-grade workflow closure planning checkpoint

- Re-reviewed the current project direction after the OpenClaw/Codex adapter discussion. The conclusion is that adapter work must wait for bottom-layer service-grade closure: candidate acquisition, profile fetch, CRM Public Web, provider task runtime, serving projection/result views, CRM/export, and Operation Workbench need stable typed service boundaries before a general Agent can safely call them.
- Updated `README.md` to remove stale MVP-era framing and the old 2026-04-25 validation snapshot as current stability evidence. The README now describes the project as a PG-only durable workflow/product workbench moving toward OpenClaw/Codex-callable typed actions, with GitHub sync warnings for the current dirty worktree.
- Promoted `docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md` from historical design/reference status to the current architecture planning entry for Phase 13 pre-adapter work. It now records the 2026-06-08 refactor inventory: workflow/command spec manifest, Provider Task Runtime, Candidate Acquisition Service, Profile Fetch Service, CRM Public Web, model-native Search, Serving Projection, CRM/export, frontend/Operation Workbench, documentation, and GitHub process.
- Updated `docs/INDEX.md` and `docs/NEXT_TODO.md` so new sessions start from service-grade workflow closure rather than older MVP/session handoff framing. The new milestone order is M0 GitHub checkpoint discipline, M1 workflow/command manifest, M2 Provider Task Runtime, M3 Candidate Acquisition Service, M4 Profile Fetch Service, M5 CRM Public Web quality closure, and M6 OpenClaw/Codex adapter after service closure.
- Current repository risk: the worktree is very dirty and includes many modified/untracked files across docs, frontend, runtime, scripts, tests, configs, and logs. A broad `git add .` is not safe. The next GitHub sync should be a scoped checkpoint branch/PR after reviewing staged file scope, excluding runtime/log/secret artifacts, and preserving Independent Review Gate artifacts.
- Follow-up architecture decision: service closure should be Agent-native from M1 onward, not a traditional backend followed by a late adapter. The target service shape is tool manifest + JSON schema + event-visible progress + Activity/Attempt/EntityDelta evidence + budget/approval/control contracts. Candidate Acquisition and Public Web may use both deterministic/API providers such as DataForSEO and reviewed Agent Search/fetch/browser sources, but all sources must enter one ProviderTask/Evidence adjudication path before promotion/export/CRM mutation. Creating a new checkpoint/development branch from the current local state is acceptable because the prior branch has been stale, but only scoped commits/PRs preserve state safely; a broad dirty-tree commit remains unsafe.
- Local asset governance timing decision: place Google/Reflection AI/early mixed production-test asset cleanup after M0 docs checkpoint and before M1/M2 as `M0.5 Local Asset Governance`. It should reuse the existing W5 asset-consolidation pattern: read-only audit, authoritative pointer/reference identity proof, scoped shard retention decision, cold archive manifest, reviewed apply, and rebuild/projection verification. It must not be a manual filesystem deletion because historical snapshots may still be needed for rebuild, audit, projection recovery, or scoped reuse.

## 2026-06-07 (Asia/Shanghai)

### Target Candidates, CRM Public Web, and Operation Workbench checkpoint

- Closed the current local-asset / target-candidate frontend product slice. The target-candidates page now keeps the local asset tab shell, candidate cards use fixed-height three-line headline and Public Web progress regions, Public Web link chips sit in a fixed-height row, and long review guidance is shown through adjacent `?` help instead of standalone card text.
- Tightened CRM Public Web workspace contracts end to end. Frontend body-style poll/start/cancel/retry/export requests now require explicit backend-owned `workspace_id`, and the backend CRM Public Web body-style entrypoints fail closed on missing workspace instead of silently falling back to `default`. Missing or mixed workspace state clears stale Public Web run/detail caches in the UI. Shared frontend adapter/schema now preserves Public Web run `workspace_id`, backend-owned `phase_command_display_line`, `run_control_state`, `run_display_contract`, `phase_commands`, and `created_at`.
- Productized the operation queue surface as a task approval/execution workbench. The page now presents待确认操作、执行队列、执行详情、执行证据 as product concepts, keeps raw ids/types/statuses in expandable technical details, and routes run/command controls only through Operation / workflow command APIs.
- Independent Review Gate was run after targeted tests and before browser validation. Final artifact `runtime/reviews/20260606T200102Z_final-public-web-workspace-stale-state-run-contract-gate.md` returned `GO`; earlier `NO-GO` findings around workspace fallback, shared adapter/schema drift, stale Public Web state, and frontend-derived run control were fixed before signoff.
- Browser validation was completed against the local frontend/backend. Screenshots: `output/playwright/target-candidates-card-layout.png`, `output/playwright/jackie-public-web-detail.png`, `output/playwright/yuwei-public-web-detail.png`, and `output/playwright/operations-product-surface.png`. Reports: `output/playwright/frontend-validation-report.json`, `output/playwright/frontend-tooltip-visibility-report.json`, and `output/playwright/frontend-detail-tooltip-visibility-report.json`. Metrics confirmed equal first-card heights, fixed headline/progress/link regions, hidden long help text, and removal of old internal Operation Queue copy.
- Validation passed: `uv run pytest tests/test_frontend_candidate_filters.py -k target_candidate_public_web_detail_uses_reviewable_signal_sections -q`; `uv run pytest tests/test_pre_agent_contract_review.py -k 'frontend_public_web_uses_crm_canonical_endpoints_only or operations_page_is_operation_api_only_control_surface' -q`; `uv run pytest tests/test_frontend_local_asset_pages.py -q`; targeted Public Web/results/runtime preflights; `npm run build` in `frontend-demo`; `git diff --check` for the touched files.
- Current CRM Public Web data observation: Yuwei Qin's current PG-owned Public Web state contains a manually promoted Google Scholar link and a rejected X link; no Yuwei GitHub promotion is present in current PG state. Jackie Bow has durable X and GitHub promotions, including `https://github.com/jbow`.
- Remaining risks: real provider/model live validation is still pending for workspace mismatch responses, export watermark reuse, model-provider circuit behavior, and Public Web source quality. Historical Markdown deletion was intentionally not performed in this slice; cleanup/deletion needs a separate Independent Review Gate because older docs still carry audit evidence and contract strings.

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
- Added `docs/APIFY_BILLING_INCIDENT_POSTMORTEM_2026-05-07.md` with the timeline, root cause, evidence chain, user-reported `$15` cost impact, quarantine evidence, and prevention gates. `docs/NEXT_TODO.md`, `docs/WORKFLOW_PROGRESS_CONTRACT.md`, and `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` were updated with the final incident and profile-delta/full-materialization boundary.
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

Closeout of `docs/CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md`. All eight implementation slices landed; backend and frontend builds pass.

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

- Added `docs/CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md` as the focused Claude Code handoff for the current candidate-board streaming / local-apply orchestration incident.
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
  - Updated `docs/DATA_ASSET_GOVERNANCE.md`, `docs/STREAMING_WORKFLOW_REBUILD_PLAN.md`, `docs/NEXT_TODO.md`, and `docs/INDEX.md`.
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


## 2026-04-30 (Asia/Shanghai)

### Legacy backfill implementation and CLI hardening (rebuild slice 8)

- Implemented `backfill_job_result_lifecycle` in `src/sourcing_agent/job_result_lifecycle_backfill.py` to create lifecycle rows for historical jobs without validated rows.
- Backfill logic:
  - Scans all jobs via `store.list_jobs()`.
  - Skips jobs that already have `source_validation_status='validated'` lifecycle rows.
  - For legacy jobs, builds projection using quarantined `_build_result_view_lifecycle_payload` with `_allow_for_legacy_backfill=True`.
  - Persists via `_persist_canonical_lifecycle` with `source_validation_status='validated'` so backfilled rows are trusted by fast path reader.
  - Supports dry-run mode for safe preview, batch processing, and progress callbacks.
  - Idempotent: re-running backfill on jobs with validated rows is safe and skips them.
- CLI integration: added `backfill-job-result-lifecycle` command with `--dry-run` flag. Uses `build_orchestrator()` for correct initialization.
- 2026-05-02 hardening after real CLI dry-run exposed two service-path gaps:
  - Added schema preflight before the first lifecycle read. The command now force-syncs the single `job_result_lifecycle` table into PG with validation, avoiding the prior `UndefinedTable` failure when PG's sync fingerprint predated the new table.
  - Fixed the CLI progress callback contract and added a command-level test that exercises verbose progress output.
  - Replaced the remaining public-read `get_job_candidate_page` call to deprecated `_compute_and_record_stage1_progress` with non-mutating `_build_linkedin_stage1_progress_payload`; the deprecated helper now has no call sites and is retained only until production backfill completes.
- Test coverage (7 backfill tests plus CLI command coverage, all passing):
  - `test_backfill_creates_lifecycle_rows_for_legacy_jobs` — verifies backfill creates rows for jobs without lifecycle rows.
  - `test_backfill_dry_run_does_not_persist` — verifies dry-run mode does not persist changes.
  - `test_backfill_handles_multiple_jobs` — verifies batch processing of mixed legacy/validated jobs.
  - `test_backfill_skips_jobs_with_validated_lifecycle_rows` — verifies validated rows are not overwritten.
  - `test_backfilled_rows_marked_as_validated` — verifies backfilled rows use `source_validation_status='validated'` for fast path trust.
  - `test_backfill_idempotency` — verifies re-running backfill does not modify existing validated rows.
  - `test_backfill_runs_schema_preflight_before_first_lifecycle_read` — verifies schema preflight happens before the first lifecycle read.
  - `test_backfill_job_result_lifecycle_cli_dry_run_delegates_with_progress_callback` — verifies CLI dry-run delegates correctly and verbose progress callback signature matches the backfill API.
- Validation:
  - `.venv-tests/bin/pytest tests/test_job_result_lifecycle_backfill.py -q` -> `7 passed`
  - `.venv-tests/bin/pytest tests/test_cli.py -q -k 'backfill_job_result_lifecycle or control_plane_storage_banner'` -> `3 passed, 29 deselected`
  - `.venv-tests/bin/python -m sourcing_agent.cli backfill-job-result-lifecycle --dry-run --batch-size 2` -> completed, `249` jobs would be backfilled, `0` errors, schema preflight `status=synced`
  - `.venv-tests/bin/python -m sourcing_agent.cli backfill-job-result-lifecycle --dry-run --batch-size 200 --verbose` -> completed with progress events and `0` errors
  - `.venv-tests/bin/pytest tests/test_results_api.py -q -k 'validated_stage1_row_is_not_mutated_by_public_reads'` -> `1 passed`
  - `.venv-tests/bin/pytest tests/test_results_api.py -q` -> `97 passed, 1 teardown cleanup failure`; the failed test passed when rerun alone, and the failure was an `OSError: Directory not empty` while deleting a temp `company_assets/openai` directory, not an assertion failure. Track as a test cleanup/flakiness risk before final release validation.
- Legacy helpers retained: `_build_result_view_lifecycle_payload`, `_lifecycle_row_needs_stage1_repair`, and `_compute_and_record_stage1_progress` are kept as safety fallback until production backfill runs. These methods are quarantined with `_allow_for_legacy_backfill` guard and documented for deletion after backfill execution.
- Next steps:
  - Run `backfill-job-result-lifecycle --dry-run` in production to preview changes.
  - Execute production backfill: `backfill-job-result-lifecycle`.
  - After backfill completes, delete the three legacy helper methods outright.

### Failed-state lifecycle writer wired at terminal failure paths (rebuild slice 7)

- Wired `mark_job_result_lifecycle_terminal(outcome="failed")` at three terminal workflow failure paths:
  1. `_mark_workflow_failed` (line 24930) — the primary workflow failure handler that transitions job status to "failed" and stage to "failed"
  2. Excel intake failure (line 18384) — when Excel parsing or validation fails during workflow execution
  3. Retrieval job failure (line 26039) — when a retrieval job fails terminally
- All three call sites now write `phase="failed"`, `state="failed"`, `phase_status="terminal"` to the canonical lifecycle row before returning failure to the caller.
- Fixed critical bugs discovered during validation:
  1. `_normalize_lifecycle_for_terminal_phase` was incorrectly overwriting `state="failed"` with materialization state for failed jobs. Added `status != "failed"` guard to preserve failed state.
  2. `_lifecycle_row_needs_stage1_repair` was triggering repair for terminal rows. Added terminal phase detection to skip repair logic for `phase='failed'` or `phase='completed' with phase_status='terminal'`.
  3. Fixed indentation bug in `get_job_candidate_page` cache block (introduced in commit 60f4a7c) that caused `UnboundLocalError`.
  4. Fixed `allow_materialization_fallback` logic inversion: when `source_path` is `candidate_documents.json`, materialization fallback should be **enabled**, not disabled. Changed line 6450 from `not allow_candidate_documents_fallback` to `allow_candidate_documents_fallback`.
  5. Fixed Stage 1 repair detection to use correct field names from `linkedin_stage_1_progress`: `profile_fetch_required_count` and `profile_fetched_count` (not `delta_profile_*`).
  6. Fixed state normalization to check job stage: when `stage=acquiring`, return `delta_applying` instead of `current_snapshot_materializing`.
- Added comprehensive failed-state test coverage:
  - `test_job_result_lifecycle_failed_workflow_writes_terminal_row` — proves that failed workflows write terminal lifecycle rows with `phase="failed"`, `state="failed"`, and that `/dashboard` correctly returns `state="failed"`.
  - `test_job_result_lifecycle_excel_intake_failure_writes_terminal_row` — proves Excel intake terminal failure writes a failed lifecycle row.
  - `test_job_result_lifecycle_retrieval_job_failure_writes_terminal_row` — proves retrieval job terminal failure writes a failed lifecycle row.
  - `test_job_result_lifecycle_failed_state_consistent_across_endpoints` — proves `/dashboard`, `/progress`, and `/candidates` all report identical failed lifecycle state for failed jobs.
  - `test_job_result_lifecycle_failed_public_read_does_not_mutate` — proves validated failed rows are never mutated by public reads; the terminal writer is the sole source of truth.
- Validation:
  - `.venv-tests/bin/pytest tests/test_results_api.py -q` -> `98 passed in 83.29s`
  - `.venv-tests/bin/ruff check tests/test_results_api.py` -> `All checks passed!`
- All lifecycle tests pass, including the three previously failing tests:
  - `test_asset_population_overlay_count_overrides_full_snapshot_manifest_count` — fixed by correcting overlay loading logic
  - `test_delta_progress_exposes_search_and_profile_fetch_counts` — fixed by correcting repair detection field names and state normalization
  - `test_job_results_auto_materialize_snapshot_candidate_documents_for_asset_population` — fixed by correcting materialization fallback logic
- Historical failure now impossible: failed workflows cannot appear as "unavailable" or other incorrect states in `/dashboard`, `/progress`, or `/candidates` because the terminal failure writer commits the canonical failed state before any public read. Cross-endpoint consistency is guaranteed by all three APIs reading from the same canonical row.
- Remaining gaps after this slice:
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair`, `_compute_and_record_stage1_progress`, and the fallback projection outright.

### Canonical reader fallback projection retired (rebuild slice 6)

- Retired `_build_result_view_lifecycle_payload` from the canonical reader fast path. Public reads (`/dashboard`, `/progress`, `/candidates`) now trust the validated `job_result_lifecycle` row as the source of truth, even when no stored `result_view` exists yet.
- Fixed `_lifecycle_row_needs_stage1_repair` to not require Stage 1 counters when the row has `served_snapshot_id` (materialization writer is authoritative). This allows materialization-writer-updated rows to hit the fast path without falling back to projection.
- Quarantined `_build_result_view_lifecycle_payload` for legacy/backfill only: added `_allow_for_legacy_backfill` parameter that raises `ValueError` if called for validated rows without explicit flag. The slow path in `_load_job_result_lifecycle` passes `_allow_for_legacy_backfill=True` when calling the fallback projection.
- Enabled full cross-API consistency assertions in `test_job_result_lifecycle_baseline_delta_overlay_for_health_whisper_shape`. The test now asserts that `/dashboard`, `/progress`, and `/candidates` return identical lifecycle payloads for the Health/Whisper baseline+delta overlay shape (baseline serving + incomplete delta = not stable current serving).
- Added three new regression tests:
  - `test_job_result_lifecycle_validated_row_without_result_view_uses_canonical_row` — proves validated row without stored result_view is projected from canonical row, not rebuilt
  - `test_job_result_lifecycle_terminal_invariant_baseline_served_incomplete_delta` — enforces terminal state invariant (baseline serving + incomplete delta = not stable current serving)
  - `test_job_result_lifecycle_fallback_projection_quarantined_for_validated_rows` — proves guard prevents accidental fallback use for validated rows
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'lifecycle'` -> `22 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
  - `./.venv-tests/bin/ruff format src/ tests/` -> formatted 9 files
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Historical failure now impossible: the Health/Whisper baseline+delta overlay regression (where `/dashboard`, `/progress`, and `/candidates` returned divergent lifecycle payloads) cannot happen because all three APIs read from the same canonical row, and the fallback projection is quarantined for legacy/backfill only.
- Remaining gaps after this slice:
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair`, `_compute_and_record_stage1_progress`, and the fallback projection outright.

### Materialization/repoint lifecycle writers wired at event-time (rebuild slice 5)

- Wired `update_job_result_lifecycle_from_materialization` at two key materialization event-time hooks:
  1. `_recover_delta_only_result_view_to_baseline_delta_overlay` line 4807 — after the current-snapshot repoint upsert. When a delta-only result view is detected and repaired to serve the full current snapshot, the materialization writer updates the canonical lifecycle row with `served_snapshot_id = current`, `served_candidate_count = current total`, and `current_snapshot_id = current`.
  2. `_recover_delta_only_result_view_to_baseline_delta_overlay` line 4888 — after the baseline+delta overlay upsert. When a delta-only result view is overlaid with the baseline to serve `baseline + delta`, the materialization writer updates the canonical lifecycle row with `served_snapshot_id = current`, `served_candidate_count = baseline + delta`, and the overlay serving phase.
- Both event-time hooks run immediately after `upsert_job_result_view`, so the canonical lifecycle row is updated before any public read can observe the new result view.
- Added `test_job_result_lifecycle_baseline_delta_overlay_for_health_whisper_shape` — proves that the materialization writer records `served_snapshot_id = current` and `served_candidate_count = baseline + delta` for the Health/Whisper shape (baseline+delta job where current lane returns 0 and former lane returns >0). The test documents the current gap: the canonical reader's fast path requires a stored result_view to exist, so the public-read assertions are commented out with a TODO. Once the fallback projection is retired, those assertions will pass.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle or health_whisper'` -> `32 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `85 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6439` in `_build_job_asset_population_payload` — verified independent of this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Historical failure partially addressed: baseline+delta jobs cannot complete into raw delta-only result views at the event-time level (the materialization writer records the overlay serving state). However, the canonical reader's fallback projection can still rebuild from old sources when no stored result_view exists, so the full cross-API consistency invariant is not yet enforced. This gap is documented in the test and will be closed when the fallback projection is retired.
- Remaining gaps after this slice:
  - **Canonical reader fallback projection retirement.** `_build_result_view_lifecycle_payload` is still called from the slow path when the persisted row doesn't exist or isn't validated. Once all event-time writers are wired and legacy backfill ships, this fallback can be deleted.
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair`, `_compute_and_record_stage1_progress`, and the fallback projection outright.

### Stage 1 lifecycle writer moved to event-time (rebuild slice 4)

- Wired `update_job_result_lifecycle_from_stage1` at the real workflow event-time hook: `_persist_running_job_inline_reconcile_state` (line 22380+). This method is called when `acquisition_progress.latest_state` is updated with a new snapshot_id and search_seed_snapshot after search-seed/roster workers complete and their results are applied. The Stage 1 writer runs immediately after `save_job`, building the coherent projection from the fresh `latest_state` and persisting it into the canonical `job_result_lifecycle` row. Public reads (`/dashboard`, `/progress`, `/candidates`) now find the row already up to date.
- Removed public-read mutation: reverted the three public-API call sites (`_build_job_results_context`, `get_job_candidate_page`, `get_job_progress`) from `_compute_and_record_stage1_progress` back to the non-mutating `_build_linkedin_stage1_progress_payload`. Public reads no longer write to the canonical lifecycle row for Stage 1 fields.
- Deprecated `_compute_and_record_stage1_progress`: marked as deprecated with a comment explaining it's retained only for compatibility during the transition. It now just calls `_build_linkedin_stage1_progress_payload` and returns the payload without any lifecycle write.
- Narrowed `_lifecycle_row_needs_stage1_repair` to pure legacy/backfill semantics: it now only triggers for rows with no `projection_source_snapshot_id` AND no Stage 1 counters. The event-time writer always sets both, so new jobs hit the validated fast path. The helper is documented as deletable once a one-shot legacy backfill ships.
- Added `test_job_result_lifecycle_stage1_event_time_write_before_public_read`: proves that once the Stage 1 writer has run, subsequent public reads do NOT mutate the canonical row's Stage 1 fields. This is the slice-4 invariant.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'stage1 or job_result_lifecycle or lifecycle_consistent or lifecycle_writer'` -> `14 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle'` -> `31 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `84 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6408` in `_build_job_asset_population_payload` — verified independent of this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Historical failure now impossible: the OpenAI Infra mixed-source bug (`current=0, former=10, profile_required=77`) cannot happen in public lifecycle because the event-time writer commits one coherent projection (lane counts + profile denominator from the same snapshot), and public reads no longer rebuild or mutate Stage 1 fields.
- Remaining gaps after this slice:
  - **Materialization-repoint call-site wiring.** `update_job_result_lifecycle_from_materialization` exists but is not yet called from `_repair_delta_only_result_view_to_overlay` and current-snapshot repoint paths.
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair` and `_compute_and_record_stage1_progress` outright.
  - **Fallback projection retirement.** Once materialization writers are wired and legacy backfill ships, `_build_result_view_lifecycle_payload` can be deleted from the public-read flow.

### Stage 1 lifecycle writer landed (rebuild slice 3)

- Added `SourcingOrchestrator.update_job_result_lifecycle_from_stage1`. The writer commits a coherent Stage 1 projection — current/former/all lane counts, deduped candidate count, deduped profile URL count, profile fetch required, profile fetched, plus mirrored `delta_profile_required/fetched_count` for delta workflows — in one row update. `projection_source_snapshot_id` is recorded so concurrent older lane reads cannot regress newer fields (snapshot ids are timestamp-prefixed and therefore monotonic).
- The writer enforces the OpenAI Infra mixed-source invariant directly: when the lane snapshot returns no rows, `stage1_profile_fetch_required_count` stays 0 even if workers report a queued URL count. Worker-derived URL totals are accepted via `queued_profile_url_count_from_workers` and stored in `metadata_json.workers.queued_profile_url_count` for observability — never in the public-facing denominator. This makes the `current=0, former=10, profile_required=77` failure mode impossible by construction.
- Phase progression: when an existing row is at `planning` or `baseline_serving` and Stage 1 evidence is observed, the writer transitions to `delta_applying` (delta workflows) or `current_snapshot_serving` (full local reuse / live roster). It never regresses out of more advanced phases.
- `expected_candidate_count` for in-progress delta workflows is derived from `baseline_candidate_count + coherent_required_count`, capped by the served count.
- Added `_compute_and_record_stage1_progress` as the seam between the existing `_build_linkedin_stage1_progress_payload` and the canonical writer. The three primary public-API callsites (`_build_job_results_context`, `get_job_candidate_page`, `get_job_progress`) now go through it, so Stage 1 fields land in the canonical row before `_load_job_result_lifecycle` runs.
- Narrowed `_lifecycle_row_needs_stage1_repair` to a legacy/backfill repair detector. New jobs hit the validated fast path because the writer has run; it only falls through for rows with no `projection_source_snapshot_id` AND no Stage 1 counters AND evidence in the supplied progress payload that Stage 1 has progressed. Documented as deletable once a one-shot legacy backfill ships.
- Added four Stage 1 writer regression tests in `tests/test_results_api.py`:
  - `test_job_result_lifecycle_writer_stage1_persists_atomic_projection`
  - `test_job_result_lifecycle_writer_stage1_rejects_stale_snapshot_writes`
  - `test_job_result_lifecycle_writer_stage1_rejects_mixed_source_infra_shape`
  - `test_job_result_lifecycle_validated_stage1_row_is_not_mutated_by_public_reads`
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'stage1 or job_result_lifecycle or lifecycle_consistent or lifecycle_writer'` -> `13 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle'` -> `30 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `83 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6408` in `_build_job_asset_population_payload` — verified independent of this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py tests/test_results_api.py` -> passed
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Remaining gaps after this slice:
  - **Materialization-repoint call-site wiring.** `update_job_result_lifecycle_from_materialization` exists but is not yet called from `_repair_delta_only_result_view_to_overlay` and current-snapshot repoint paths.
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair` outright.
  - **Workflow-event-time Stage 1 writes.** The writer is currently invoked at public-API entry. Moving the call into `enrichment.py` / `seed_discovery.py` so the row updates without any public read would let the projection-fallback inside `_load_job_result_lifecycle` retire entirely.

### Event-driven lifecycle writers + reader fast-path landed

- Added four event-driven canonical lifecycle writers on `SourcingOrchestrator`: `initialize_job_result_lifecycle`, `publish_baseline_job_result_lifecycle`, `update_job_result_lifecycle_from_materialization`, `mark_job_result_lifecycle_terminal`. Each writes through `ControlPlaneStore.upsert_job_result_lifecycle` with patch-style merge, mapping concrete transition inputs (baseline snapshot id, baseline count, served snapshot id, delta counts, terminal outcome) onto canonical row fields. The terminal writer encodes the architectural invariant directly: a finished job whose served snapshot equals the baseline with materialization incomplete is recorded as `current_snapshot_materializing` (or `delta_applying`), not stable `baseline_serving`.
- Wired the workflow-create writer at `_create_workflow_job` (initial planning row) and the baseline-publication writer at `_persist_delta_baseline_serving_result_view` (transitions to `baseline_serving` + view_id binding). Both run before any public API touches the job.
- `_load_job_result_lifecycle` now has a validated fast path: if the persisted row has `source_validation_status='validated'` and is not in a transition the current writer set does not yet keep current, public reads return the persisted row unchanged (terminal-phase invariant still applied at render time only). Writes only happen on the slow/repair path. This retires the "rebuild and persist on every public read" loop.
- `_lifecycle_row_needs_stage1_repair` is the narrow stale-detection helper: a row at `phase=planning` or `baseline_serving` falls through to projection when `linkedin_stage_1_progress` shows actual work has happened (profile fetched, profile required, search returned). This is the explicit gap until the Stage 1 progress writer lands.
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` extended to assert the canonical row exists post-read and is pinned to the correct baseline.
- Four new writer tests in `tests/test_results_api.py`:
  - `test_job_result_lifecycle_writer_initialize_persists_planning_row_without_public_read`
  - `test_job_result_lifecycle_writer_publish_baseline_persists_row_before_public_read` (also asserts validated public reads do not mutate canonical fields)
  - `test_job_result_lifecycle_writer_materialization_repoint_updates_row`
  - `test_job_result_lifecycle_writer_terminal_normalizes_stale_baseline_infra_shape`
- Triaged the two pre-existing lifecycle-adjacent failures (`test_result_view_lifecycle_counts_profile_detail_materialized_to_board_from_state`, `test_result_view_lifecycle_infers_baseline_count_from_stage1_preview`). Both encoded contracts retired by the canonical reader/projection: the first asserted profile-richness completeness as the materialization metric (contradicts `WORKFLOW_PROGRESS_CONTRACT.md` board-serving definition); the second asserted on-disk `candidate_documents.json` row count as `served_candidate_count` (contradicts the canonical-result-view-summary contract that makes cross-API consistency possible). Updated both expectations with explicit pointers to the design and contract docs; both now pass.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'job_result_lifecycle or lifecycle_consistent or lifecycle_writer'` -> `5 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle'` -> `26 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `79 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6408` in `_build_job_asset_population_payload` (uncommitted dirty-tree caching write that runs before `payload` is bound; verified independent of the lifecycle slice in the previous handoff).
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py tests/test_results_api.py` -> passed
- Remaining gaps in this slice (called out in `docs/JOB_RESULT_LIFECYCLE_DESIGN.md` §"Implementation status"):
  - **Stage 1 projection writer.** Lane counters (`stage1_current_search_returned_count`, …, `stage1_profile_fetched_count`) and `delta_profile_*` fields on the row are still 0 unless the materialization writer fills them. The current behavior is that `_lifecycle_row_needs_stage1_repair` falls back to projection so users see fresh counters; once the dedicated Stage 1 writer lands, the row stays current without a fall-through.
  - **Materialization-repoint wiring.** The writer exists but is not yet called from `_repair_delta_only_result_view_to_overlay` / current-snapshot repoint paths. Today these paths still write the lifecycle dict into `result_view.metadata`; the canonical row picks the change up via the slow path.
  - **Failed-state writer.** Only `outcome=completed` is wired into the workflow create / baseline publication paths; failed jobs are normalized at render time but no failure-event writer call exists yet.
  - **One-shot backfill.** Existing jobs without a row work via the slow path. A startup migration would precompute rows for legacy jobs.

### Canonical job_result_lifecycle slice landed

- Implemented the persisted canonical lifecycle from `docs/JOB_RESULT_LIFECYCLE_DESIGN.md`. New table `job_result_lifecycle` (PG-first via control-plane allowlist; SQLite mirror; primary key `job_id`) holds baseline/current/served snapshot ids, baseline/served/expected counts, delta required/fetched/applied/materialized/board-visible counts, Stage 1 lane projection counters, phase, projection-source snapshot id, and `source_validation_status`.
- Added `ControlPlaneStore.upsert_job_result_lifecycle` (patch-style merge) and `get_job_result_lifecycle` plus the `_job_result_lifecycle_from_row` helper, modeled after `job_result_views`.
- Added `SourcingOrchestrator._load_job_result_lifecycle` as the single canonical reader. It builds the in-flight projection (still via `_build_result_view_lifecycle_payload` for now), applies a terminal-phase invariant, persists the row, and returns the public-API payload through `_project_canonical_lifecycle_payload`. Architectural invariant enforced at the lifecycle layer: a terminal job whose served snapshot equals the baseline with `delta_profile_required > delta_profile_materialized` is reported as `current_snapshot_materializing` (or `delta_applying`), never stable `baseline_serving`.
- Routed `/dashboard`, `/candidates` (both asset_population and ranked_results branches), and `/progress` through the canonical reader. The reader derives `served_candidate_count` uniformly from `job_result_views.summary.candidate_count` rather than from caller-specific `asset_population` payloads — this is the change that makes the OpenAI Infra cross-API regression pass.
- Anchor regression `tests/test_results_api.py::ResultsApiTest::test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` now passes. Before this slice it failed with `/dashboard served_candidate_count=5` vs `/progress served_candidate_count=1200` for hosted OpenAI Infra job `c5248ea4b3b4`; after the slice all three public APIs return identical canonical fields and the terminal-state invariant is satisfied.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent'` -> `20 passed, 2 failed`. Both failures are pre-existing dirty-tree expectations against direct calls to `_build_result_view_lifecycle_payload` (`...counts_profile_detail_materialized_to_board_from_state`, `...infers_baseline_count_from_stage1_preview`); they are unaffected by the canonical reader and have been failing since before this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/orchestrator.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py` -> passed
- Pre-existing dirty-tree failures unrelated to the lifecycle slice (verified by reverting the canonical reader call sites and confirming the same failures): `test_asset_population_candidate_page_exposes_seed_query_as_matched_keyword`, `test_job_results_auto_materialize_snapshot_candidate_documents_for_asset_population`, `test_job_results_default_to_asset_population_for_full_company_snapshot_even_when_delta_is_required`, `test_job_results_default_to_asset_population_for_scoped_snapshot_even_when_delta_is_required`, `test_job_results_expose_asset_population_for_snapshot_reuse_even_when_plan_payload_is_missing`. Most trip an `UnboundLocalError: payload` at `orchestrator.py:6393` introduced by the dirty-tree `_asset_population_payload_cache` write that runs before `payload` is bound.
- Remaining slice scope (deferred to follow-up commits):
  - Move writers from "rebuild on every read" to dedicated workflow-event hooks (workflow create / baseline publication / Stage 1 projection / materialization-repoint). Today the canonical row is refreshed on every public read; this is correct but not yet event-driven.
  - Populate Stage 1 lane projection columns (`stage1_current_search_returned_count`, …) — the schema is in place but the writer still leaves them at 0.
  - Migrate existing jobs (a one-shot backfill) and flip `source_validation_status` semantics from "always validated on read" to "validated only after a writer commits".
  - Extend the anchor regression to also assert lifecycle persistence (currently only cross-API consistency is asserted).

### Canonical job_result_lifecycle design + failing Infra cross-API regression

- Mapped every current writer and reader of `result_view_lifecycle`. Findings: there is no persisted lifecycle row; `_build_result_view_lifecycle_payload` (`src/sourcing_agent/orchestrator.py:7485-7859`) is rebuilt per request with a five-source fallback ladder for `delta_profile_materialized_count`, and `/dashboard` (5735), `/candidates` (5841/5889), and `/progress` (8608) feed it different inputs. State today lives only inside `job_result_views.metadata_json`, `jobs.summary_json`, runtime stage files, and `candidate_materialization_states`.
- Added `docs/JOB_RESULT_LIFECYCLE_DESIGN.md` covering the canonical persisted `job_result_lifecycle` table (schema, indexes, phase enum), the four-writer ownership model (workflow create / baseline publication / Stage 1 projection / materialization-repoint), the read path via a new `_load_job_result_lifecycle` helper, the derived sources retired, the failure classes made impossible, and the non-blocking migration plan that backfills existing jobs as `pending_validation`.
- Added the anchor regression `tests/test_results_api.py::ResultsApiTest::test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape`. It reproduces hosted job `c5248ea4b3b4`: terminal workflow, baseline-serving result view at `20260430T130836`, `delta_profile_required/fetched=89/89`, `delta_profile_materialized=19`, `expected=1200`. Asserts `/dashboard`, `/progress`, `/candidates` observe identical lifecycle rows and that a terminal job cannot expose stable `results` while serving a stale baseline-only board with materialization incomplete.
- Test currently fails as designed: `/dashboard served_candidate_count=5` (counts on-disk baseline candidate_documents.json rows) vs `/progress served_candidate_count=1200` (reads the persisted lifecycle dict). Same job, same instant, two public APIs disagree by 1195 — direct evidence the per-request rebuild path must be retired.
- Linked the design from `docs/INDEX.md` and recorded the slice + failing test in `docs/NEXT_TODO.md` under the highest-priority rebuild track.

### Service-grade streaming workflow rebuild handoff documented

- Promoted the repeated OpenAI Infra/Health/Whisper/ChatGPT streaming failures from local bug backlog into a service-grade workflow rebuild track.
- Added `docs/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md` for the next Claude Code session: product expectations, anchor failure `c5248ea4b3b4`, earlier incident classes, required canonical lifecycle architecture, scheduler/materialization boundaries, scripted/browser guardrails, anti-patterns, and a ready-to-use starting prompt.
- Added `docs/STREAMING_WORKFLOW_REBUILD_PLAN.md` with the implementation order: persisted `job_result_lifecycle`, atomic Stage 1 progress, provider completion/next-submit/materialization decoupling, partial delta board streaming, and upgraded service-level scripted/browser tests.
- Updated `docs/NEXT_TODO.md` so this rebuild is the first Highest Priority item before more workflow code changes, and updated `docs/INDEX.md` so the new docs are canonical entry points.
- Clarified the target scheduler shape after reviewing the OpenAI Infra small-batch/provider-slot failure: durable item queues are the retry/dedupe/progress/materialization units, while provider batches are only adaptive remote-run envelopes. Tiny live batches such as `2` or `3` profiles should auto-merge into ready/near-ready work by default because each remote run has fixed webhook/watcher/recovery/dataset/local-apply overhead. Exceptions require queue-quiescence proof after a coalescing window, or explicit retry-isolation/low-volume-company policy.
- Clarified the target candidate board shape: the ideal serving unit is a continuous projection, not a complete snapshot switch. The board should serve baseline generation + ordered delta patches + row-level materialization state, while full snapshot/retrieval/index rebuild becomes background compaction rather than the first moment new candidates can be visible.
- Updated `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`, `docs/WORKFLOW_PROGRESS_CONTRACT.md`, `docs/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md`, `docs/STREAMING_WORKFLOW_REBUILD_PLAN.md`, and `docs/NEXT_TODO.md` to make queue-first scheduling, adaptive batch packing, per-item retry/dedupe, continuous serving projection, and incremental board-visible apply part of the canonical rebuild scope.
- Added a stricter planning critique to the Claude Code context and rebuild plan: each implementation slice must name the durable state it introduces, the stale source/fallback it retires, the historical failure it makes impossible, and the negative scripted/browser assertion that proves the behavior.

### Hosted OpenAI Infra live diagnosis added to TODO

- User case `ae907436-5996-4e04-a109-c9943cdfa399` / job `c5248ea4b3b4` (`帮我找OpenAI做Infra方向的人`) was diagnosed read-only on ECS; no code or service changes were made.
- The plan was correctly `delta_from_snapshot` from baseline `20260430T130836` because Infra current/former shard coverage is missing from selected OpenAI source snapshots.
- Final snapshot files for `20260430T155907` show `current/entries=83`, `former/entries=10`, deduped `89`, so the UI state `新取回在职候选人0 / 新取回离职候选人10 / 需补取 LinkedIn Profile77` was a progress-source atomicity bug, not a true provider result shape.
- Provider scheduling did not keep the 4-slot profile-scraper budget full: batches were `8`, then `33/34`, then a small cached `3`; local watcher often beat provider webhook, and callback-side profile prefetch/cache-marker confirmation spent about `295-302s` with `dispatched_url_count=0`.
- The job later reached `completed/completed`, but `job_result_views` still served only the initial baseline view (`20260430T130836`); lifecycle showed `delta_profile_required/fetched=89/89` but `delta_profile_materialized_count=19`, and `/dashboard` / `/candidates` timed out during the baseline-serving post-fetch state.
- Added concrete TODO bullets under progress atomicity, result-view lifecycle completion/repoint, and profile-scraper slot utilization so this hosted Infra shape can become a regression fixture.

### Authoritative registry now preserves reusable source shard snapshots

- Root cause from hosted OpenAI Agent planning regression: the user request still parsed `Agent` correctly, but newer OpenAI Health/Whisper serving snapshots narrowed authoritative `organization_asset_registry.selected_snapshot_ids`, so planner could no longer see previously migrated Agent/ChatGPT shard coverage.
- Added registry-write inheritance for reusable source snapshots: when a newer authoritative serving snapshot is promoted, the registry row now preserves previous selected source snapshots that have acquisition-shard registry rows. This keeps OpenAI Agent/ChatGPT/Health/Whisper and future scoped shards from overwriting each other while still allowing the newest serving snapshot to be primary.
- Disabled default `current_snapshot_only_large_org` materialization selection. Large-org materialization no longer silently drops historical source snapshots just because the current snapshot is 1000+ candidates. Explicit `preferred_source_snapshot_ids` remains the bounded path for safe subset materialization until snapshot hygiene/clean-source selection is formalized. There is no separate `former_snapshot_only_large_org` mode in the codebase.
- Synced the fix to ECS and repaired OpenAI source coverage:
  - copied the missing Agent source snapshot `20260427T141011` to `/srv/sourcing-ai-agent/runtime/company_assets/openai/20260427T141011`
  - reran no-provider OpenAI shard registry backfill; authoritative OpenAI remains serving `20260430T130836`, with reusable selected source snapshots now including `20260427T141011` plus the existing ChatGPT/Health/Whisper/Coding/Reasoning/Multimodal/Audio/Pre-train sources
  - hosted `/api/plan` for `帮我找OpenAI做Agent方向的人` now returns `planner_mode=reuse_snapshot_only`, `requires_delta_acquisition=false`, `baseline_snapshot_id=20260430T130836`, and current/former Agent coverage counts `1/1`
  - repaired the already-created hosted plan history `9a497df4-25ee-4ced-a212-d5703a8054e6` to the same reuse-only plan so refreshing that record no longer shows the stale delta strategy
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_organization_execution_profile.py -q -k 'inherits_reusable_shard_source_snapshots or large_org'` -> `4 passed, 7 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_candidate_artifacts.py -q -k 'large_org'` -> `2 passed, 50 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_organization_execution_profile.py tests/test_candidate_artifacts.py -q -k 'inherits_reusable_shard_source_snapshots or large_org or materialized_view_large_org'` -> `6 passed, 57 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/asset_registration.py src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/candidate_materialization.py tests/test_organization_execution_profile.py tests/test_candidate_artifacts.py` -> passed
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/asset_registration.py src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/candidate_materialization.py` -> passed

### Hosted OpenAI Whisper delta-only result-view drift fix prepared

- Live ECS case: `bcc09c89db36` / `a03597f9-e7e7-4dba-9715-b83b55454683` (`帮我找OpenAI在Whisper组的人`) was user-visible as `failed / Failed to fetch`, but direct PG inspection showed the workflow was `completed/completed` and frontend history phase was `results`.
- Root cause found: the job result view was serving the current delta snapshot `20260430T130836` as an `asset_population` view with `candidate_count=2`, while the intended serving population was baseline `20260430T090520` plus 2 delta profiles. The delta snapshot also lacked normalized artifact manifest files, and completed-workflow reconcile was blocked by a stale terminal workflow lease, so background materialization/repoint did not close the gap.
- Fix prepared locally:
  - baseline+delta direct finalization now falls back to a job-scoped `asset_population` overlay when durable generation patch creation is unavailable, instead of dropping back to a raw delta-only result view
  - result-view read repair now detects completed delta-only result views with a `delta_baseline_snapshot_id`, loads baseline and delta candidate documents, writes a job-scoped overlay, and persists the repaired result view before serving dashboard/candidate pages
  - follow-up hot-path fix: read repair now loads only existing baseline/delta `candidate_documents` and explicitly disables materialization fallback, so public dashboard/candidate requests cannot rebuild full candidate artifacts while trying to repair a result view. Job-scoped overlay writes also avoid synchronous thematic facet/role-bucket derivation; they preserve existing values and use a cheap fallback only when missing.
  - overlay merge semantics now preserve duplicate baseline rows and baseline ordering, then update or append only the delta rows. This prevents a baseline-serving board from shrinking when a repair overlay is built from member keys.
  - if the current workflow snapshot already has at least the baseline-sized candidate document population, read repair now repoints the result view to those current `candidate_documents` instead of building a baseline+delta overlay. Candidate-document serving paths also disable materialization fallback so public reads do not turn into artifact rebuilds.
  - stale `asset_population_overlay_path` metadata no longer forces overlay serving when the authoritative result view has no valid patch; manifest-backed current snapshots may fall back to sibling `candidate_documents.json` without materialization.
  - `delta_profile_materialized_count` treats a current snapshot that is serving at least the baseline population after all delta profiles were fetched as fully materialized, because delta profiles may update existing candidates rather than increasing net board size. In that state, `expected_candidate_count` is normalized to the actual served candidate count.
  - terminal completed-workflow reconcile leases can be released after their heartbeat is stale even if the long lease TTL has not expired, preventing completed jobs from waiting many minutes before materialization/repoint recovery can run
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'delta_direct_finalization_uses_overlay or delta_only_result_view_recovers or current_candidate_documents or terminal_completed_workflow_releases_stale_reconcile_lease'` -> `4 passed, 75 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_candidate_artifacts.py -q -k 'asset_population_overlay_preserves'` -> `2 passed, 50 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases'` -> `20 passed, 64 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py` -> passed

### ECS production is now serving current code, curated assets, hosted webhook, and latest frontend

- Current ECS backend service has been switched to the canonical code root:
  - active repo: `/srv/sourcing-ai-agent/repo/sourcing-ai-agent`
  - active runtime: `/srv/sourcing-ai-agent/runtime`
  - systemd env file: `/etc/sourcing-ai-agent.env`
  - service status: `sourcing-ai-agent.service=active`
  - the attempted separate worker service is intentionally `inactive`; `serve` owns the built-in `worker-recovery-daemon` and hosted runtime watchdog.
- Production control plane is PG-only:
  - `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
  - `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
  - `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`
  - post-schema-migration journal check showed no `UndefinedTable`, `Traceback`, or `service_failed` in the last hosted gate window.
- Curated asset migration is serving the selected high-value baselines:
  - OpenAI `20260430T090520` -> `1110`
  - Google `20260428T011339` -> `9359`
  - Anthropic `20260416T225318` -> `3455`
  - Google and Anthropic serving-view audits now pass from selected snapshot ids; audit outputs are under `/srv/sourcing-ai-agent/runtime/deployment/final_audit_20260430T030333Z`.
- Anthropic exposed a registry migration edge case: remote ECS still had an older `20260409T045403` row with higher candidate count, so the guarded selector would retain/reselect it even though the local manifest selected `20260416T225318`.
  - Fix applied on ECS without deleting old assets: mark non-selected Anthropic registry rows `superseded` and promote `20260416T225318` as authoritative.
  - Migration lesson: when promoting a curated selected snapshot that is intentionally not the highest-count historical row, supersede/archive non-selected registry rows in PG so runtime selection, execution profiles, and reuse cannot drift back to old snapshots.
- Hosted webhook preflight now passes on ECS:
  - output directory: `/srv/sourcing-ai-agent/runtime/deployment/hosted_gates_20260430T030422Z`
  - `status=ready`
  - hosted callback route accepts connectivity probe with `HTTP 202`
  - submit contract has `webhook_definition_count=1` and `would_attach_webhooks=true`
  - callback URL: `https://api.111874.xyz/api/providers/apify/webhook`
- Public probes passed:
  - `https://api.111874.xyz/health`
  - `https://api.111874.xyz/api/runtime/health`
  - `https://api.111874.xyz/api/providers/health`
  - `https://demo.111874.xyz/health`
  - `https://demo.111874.xyz/api/runtime/health`
  - `https://demo.111874.xyz/api/providers/health`
- Cloudflare Pages frontend has been rebuilt and deployed in same-origin mode:
  - command: `VITE_API_BASE_URL=same-origin npm run build:hosted`
  - production deploy: `npx wrangler pages deploy dist --project-name sourcing-ai-agents-demo --branch main`
  - deployment URL: `https://757c4996.sourcing-ai-agents-demo.pages.dev`
  - custom domain now serves bundle `index-C5fqLPSd.js` / `index-C2fbpkw3.css`
  - quick bundle probe confirms current UI wording is present (`新增 LinkedIn Profile`, `已物化到看板`, `新取回在职候选人`, `打开LinkedIn`) and the old `查看原始资料` wording is gone.
- User-facing test URL: `https://demo.111874.xyz`
- Post-launch OpenAI Health plan check:
  - User-visible plan `8effc850-6a08-4cb4-aa22-83c524a29ee3` initially showed `Baseline 复用 + 缺口增量` even though OpenAI authoritative baseline was already `20260430T090520`.
  - Root cause was not wrong ECS env/DSN: runtime was `production/live`, PG authoritative OpenAI row was `20260430T090520`, and artifact files contained `search_seed_discovery/current|former` summaries for `health`.
  - Missing piece was PG `acquisition_shard_registry`: migration/backfill had not registered the current snapshot's `health` current/former search shards, so `asset_reuse_plan` could not prove shard coverage and set `requires_delta_acquisition=true`.
  - Ran local-only ECS repair, no provider calls:
    - `ensure_acquisition_shard_registry_for_snapshot(openai, 20260430T090520)` -> `shard_records=2`
    - selected-manifest shard backfill for 24 companies -> no failures; output saved under `/srv/sourcing-ai-agent/runtime/deployment/selected_shard_backfill_<timestamp>.json`
  - Replanned the same history id after repair:
    - `baseline_snapshot_id=20260430T090520`
    - `requires_delta_acquisition=false`
    - `baseline_sufficiency=ready`
    - missing current/former profile-search queries are empty.
- Post-launch selected-source snapshot parity repair:
  - Meta initially matched the selected serving baseline only (`20260427T203601` / Audio) on ECS, so `帮我找Meta做Agent方向的人` planned `Baseline 复用 + 缺口增量`. Local authoritative registry also selected source snapshots carrying `Multimodal` and `Agent`.
  - Copied/restored Meta source snapshots `20260427T190455`, `20260423T062947`, and `20260427T153312`; ECS `organization_asset_registry.selected_snapshot_ids` now matches local (`20260427T203601`, `20260427T190455`, `20260423T062947`, `20260427T153312`) and shard registry covers Audio/Multimodal/Agent current+former.
  - OpenAI selected source coverage was restored to include Health, ChatGPT, Reasoning/Reasoning model, Pre-train, Reinforcement Learning, and Coding source snapshots. Hosted plan checks for Health/ChatGPT/Coding now return `requires_delta_acquisition=false`.
  - Follow-up hosted matrix check: OpenAI `Agent` / `Health` / `ChatGPT` / `Coding` all return `reuse_snapshot_only` from baseline `20260430T130836`. OpenAI `Reinforcement Learning` still correctly returns `delta_from_snapshot` because local and ECS shard registries only have completed former-lane `Reinforcement Learning` coverage; the current lane is not proven reusable.
  - Google had the same migration-shape issue: the serving baseline `20260428T011339` was present, but reusable source shard snapshots for Multimodal/Veo/Nano Banana/Video generation/Vision-language were not selected in the hosted registry contract. Restored Google selected source snapshots to `20260428T011339`, `20260410T123708`, `20260411T174325`, `20260411T215236`, `20260413T073549`, and `20260413T100525`.
  - Hosted plan checks now return `requires_delta_acquisition=false` for Meta Agent, Meta Multimodal, Google Veo, Google Nano Banana, and Google Multimodal. Google Gemini still correctly returns `requires_delta_acquisition=true` because the Gemini current/former shard rows are `status=incomplete` (`615/736` current and `568/991` former), so they are not full coverage proof.
  - Migration rule refined: production asset migration must copy the serving snapshot plus source snapshots that provide real shard/profile reuse proof. Repeated no-increment history such as Thinking Machines Lab / Reflection AI should not be copied wholesale; keep the fullest/current baseline hot and move old duplicates to archive/test storage.
- Hosted Meta Agent full-reuse result-view/timeline follow-up:
  - User case: `dfde1d7b8af0` (`帮我找Meta做Agent方向的人`) eventually served candidate page `1727/1727`, but the execution page could show an empty timeline because `/api/jobs/{job_id}/progress` was about `50 MB`.
  - Root cause: progress/dashboard polling endpoints were shipping full workflow summaries and latest metrics from reused source snapshots, including large `background_reconcile` / deferred URL lists and old source snapshot Stage 1 payloads. Candidate paging stayed healthy because `/candidates` is lightweight and paginated.
  - Fix: `/progress` now compacts public workflow payloads before returning milestones/latest metrics/workflow stage summaries/LinkedIn progress, and `/dashboard` compacts `job.summary` instead of sending the full persisted workflow summary. Full local reuse with `delta_profile_progress_applicable=false` and `served_candidate_count >= expected_candidate_count` no longer treats inherited source snapshot LinkedIn profile progress as active `LinkedIn Stage 1` work.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'job_dashboard_is_summary_only_and_candidate_page_paginates_asset_population or job_progress_compacts_large_polling_payloads or execution_phase_contract_does_not_treat_full_reuse_snapshot_progress_as_pending or execution_phase_contract_labels_local_materialization_without_public_web'` -> `4 passed, 71 deselected`
    - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py` -> passed
    - `npm --prefix frontend-demo run build -- --mode development` -> passed
  - Hosted deployment:
    - Synced `src/sourcing_agent/orchestrator.py`, `frontend-demo/src/lib/api.ts`, tests, and docs to `/srv/sourcing-ai-agent/repo/sourcing-ai-agent`.
    - Remote `py_compile` passed; `systemctl restart sourcing-ai-agent.service` completed and service is active.
    - Rebuilt hosted frontend with `VITE_API_BASE_URL=same-origin`; redeployed Cloudflare Pages from `frontend-demo` cwd so Pages Functions were included. First deploy from repo root uploaded only static assets and made `/api/frontend-history/*` fall through to the SPA shell; second deploy fixed same-origin `/api/*`.
    - Post-deploy probes:
      - `https://demo.111874.xyz/api/frontend-history/e5a1ab27-91e1-46a6-a672-3452fc36d812` returns JSON.
      - `https://demo.111874.xyz/api/jobs/dfde1d7b8af0/progress` is about `2.1 MB` and reports `active_phase_id=final_results`, `profile_work_pending=false`.
      - Browser check on `https://demo.111874.xyz/?history=e5a1ab27-91e1-46a6-a672-3452fc36d812&job=dfde1d7b8af0` shows execution timeline instead of empty placeholder, and candidate board sync `1727/1727`.
  - Remaining non-blocking observations:
    - Runtime health can still become very large because daemon `last_summary` may retain historical workflow payloads; add the same public-payload compaction to runtime health/status endpoints later.
    - Full-local-reuse timeline still uses generic historical stage names such as `LinkedIn Stage 1 completed`; the next durable progress-lifecycle pass should replace these with job-scoped reuse/materialization phases.

### ECS code/data migration staged, production switch still blocked

- Added the reusable migration entrypoint `docs/ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md` and the read-only manifest builder `scripts/build_ecs_asset_migration_manifest.py`.
- The manifest builder now emits JSON, Markdown, and a per-file `rsync --files-from` list so future ECS migrations do not depend on ad-hoc shell inventory generation.
- Current manifest copied to ECS:
  - local/remote JSON: `runtime/deployment/ecs_asset_migration_manifest_20260430T015713Z.json`
  - local/remote Markdown: `runtime/deployment/ecs_asset_migration_manifest_20260430T015713Z.md`
  - local/remote rsync list: `runtime/deployment/ecs_asset_rsync_all_files_20260430T015713Z.txt`
- Selected asset copy completed with no deletion:
  - 24 production companies selected
  - selected snapshot size about `2.465 GB`
  - `56,243` selected snapshot files in the rsync list
  - rsync transferred about `2.034 GB`
- ECS read-only validation:
  - hard failures: `0`
  - high-value snapshots present and readable: OpenAI `20260430T090520`, Google `20260428T011339`, Meta `20260427T203601`, Lovable `20260426T193540`, Mistral AI `20260426T163408`, Perplexity `20260424T184223`, Windsurf `20260427T131256`
  - one non-blocking count mismatch remains: `humansand/20260414T162042` manifest expected `25`, remote `candidate_documents.json` has `26`; this is not on the high-value deploy path but should be reconciled before treating the manifest as a strict byte-for-byte audit ledger
  - ECS data disk now has about `144 GB` available under `/srv/sourcing-ai-agent`
- Production service has not been switched:
  - systemd still runs `/opt/sourcing-ai-agent` with old code
  - `SOURCING_RUNTIME_ENVIRONMENT=production` is still missing from the active unit
  - only `/opt/sourcing-ai-agent/.local-postgres.env` was found as an env file; the migration playbook now recommends consolidating secret-bearing production values into `/etc/sourcing-ai-agent.env`
  - local hosted route `POST /api/providers/apify/webhook` on ECS still returns `404`
  - do not restart/replace production until the current-code replacement gate and hosted webhook preflight are run from the intended systemd env
- Staging code validation:
  - current working tree has been synced to `/srv/sourcing-ai-agent/repo/sourcing-ai-agent-stage`
  - `src/sourcing_agent/asset_catalog.py` no longer hard-fails when legacy sibling skill packages (`anthropic-employee-scan`, `investor-chinese-scan`, `biz-visit-onepager`) are absent; production runtime summary only needs the in-repo `local_asset_packages/anthropic` package
  - remote staging `py_compile` and import checks pass with the existing Python 3.12 venv
  - remote staging `show-control-plane-runtime` passes under the intended production env and reports `pg_only` with `shared_memory` compatibility shadow
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_catalog.py tests/test_cli.py -q -k 'asset_catalog or control_plane_storage_banner'` -> `3 passed, 29 deselected`
  - `./.venv/bin/python -m py_compile src/sourcing_agent/asset_catalog.py scripts/build_ecs_asset_migration_manifest.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/build_ecs_asset_migration_manifest.py --company openai --skip-size-scan --output-dir /tmp/sourcing_manifest_test` -> generated JSON/Markdown plus rsync file list
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `git diff --check -- src/sourcing_agent/asset_catalog.py tests/test_asset_catalog.py scripts/build_ecs_asset_migration_manifest.py docs/ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md docs/NEXT_TODO.md PROGRESS.md` -> passed

### Health live smoke closed a stage-summary/result-view drift gap

- Live case: `43179a67ffb5` (`我想要OpenAI在health组的人`) exposed that the board/result view could eventually repair to current snapshot while the user saw old progress in the interim.
- Root cause was not a Health query-width issue. A baseline-serving `job_result_view` pointed to old snapshot `20260429T174612`; before current snapshot `20260430T090520` fully repointed, `_load_workflow_stage_summaries()` used that baseline result view as the stage-summary directory and read old ChatGPT `stage_1_preview/stage_2_final` files. Completed-summary merge then copied stale `stage1_preview/background_snapshot_materialization/background_reconcile` fields into the Health job, so progress briefly showed old counts like `166/100/298`.
- Fixes:
  - workflow stage summary resolution now prefers explicit current workflow summary/acquisition/materialization snapshot hints before falling back to the served result view, so a baseline-serving pointer cannot decide current job stage-summary files
  - completed summary restoration filters stage/runtime fields by the final summary's primary snapshot id before merging preserved/latest summaries
  - `result_view_lifecycle` only uses `stage1_preview.baseline_selection_explanation.baseline_candidate_count` when that stage preview actually matches the lifecycle baseline snapshot
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'workflow_stage_summaries_prefer_current_snapshot_over_baseline_result_view or completed_summary_merge_rejects_stage_fields_from_other_snapshots or result_view_lifecycle_ignores_stale_stage1_baseline_count_after_repoint or result_view_lifecycle or workflow_stage_summaries_ignore_stale'` -> `10 passed, 63 deselected`
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync'` -> `16 passed, 63 deselected`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
- Remaining structural follow-up: `result_view_lifecycle` still needs a single atomic persisted source-of-truth for `baseline/current/served/progress/materialization` instead of being assembled from summary/result-view/candidate-source payloads.
- ECS deploy readiness follow-up recorded:
  - `docs/ECS_PRELAUNCH_CHECKLIST.md` now has a `Current-Code Replacement Gate` for the old hosted `/opt/sourcing-ai-agent` code root: stop old backend/worker first, deploy one canonical current code root, align systemd `WorkingDirectory` / `ExecStart` / venv / env file / Nginx docs, then archive or remove the old root only after probes pass.
  - The same gate explicitly blocks deploy if `show-control-plane-runtime` is not `postgres_only + shared_memory`, if `/api/providers/apify/webhook` is still `404`, if production is not `production + live`, or if Apify webhook preflight would not attach webhooks.
  - `docs/NEXT_TODO.md` now expands the atomic lifecycle refactor into a concrete `job_result_lifecycle` follow-up with one persisted source for baseline/current/served snapshot ids, expected/served counts, delta fetch/materialization counters, lifecycle phase, and public API reads.
  - 2026-04-30 pre-deploy probe reconfirmed the boundary: local hosted submit contract is `ready` and would attach the default webhook, but the current public hosted route still returns `HTTP 404` until ECS is redeployed with current code.

### Heavy scripted browser fixtures now replay real samples with remote-wait timing

- Tightened the OpenAI ChatGPT and Lovable heavy scripted fixtures so provider payloads come from real captured sample files (`configs/scripted/samples/...`) instead of generated placeholders for the default browser/manual path.
- Fixed scripted Harvest timing realism: profile/company actor `execute_sleep_seconds` can now run as submit-after `remote_wait` (`execute_sleep_position=remote_wait`). The worker gets `run_id/dataset_id` immediately, enters `waiting_remote_harvest`, and browser scripted webhook driving waits for `scripted_remote_ready_epoch_ms` before posting the Apify-shaped terminal event. This prevents false `true active slots=4 / remote actor workers=1` submit-blocking readings caused by sleeping before the pending checkpoint existed.
- Clarified result lifecycle semantics: `delta_profile_materialized_count` means "delta rows visible on the candidate board." During partial materialization it can use candidate state or explicit patch counts; once the current snapshot serves the full delta, it uses the served delta count instead of being held back by lower profile-richness/detail counts.
- Browser observer now treats materialization stuck after profile fetch as a budgeted condition (`30s`) rather than failing on a normal short repoint tail. The OpenAI ChatGPT heavy run observed a `~9.6s` fetched-complete baseline-serving window before repointing to current snapshot, so it is diagnostic but not a failure.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py tests/test_harvest_connectors.py -q -k 'result_view_lifecycle or candidate_sync or scripted_remote_wait_after_submit'` -> `13 passed, 160 deselected`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 SOURCING_HEAVY_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=30 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_chatgpt_delta_streaming_contract_observation'` -> `1 passed, 11 deselected`
  - same command with `-k 'lovable_live_roster_streaming_contract_observation'` -> `1 passed, 11 deselected`
- Latest report highlights:
  - OpenAI ChatGPT: `harvest_profile_search=4`, `harvest_profile_scraper_batch=12`, max queued/webhook-eligible profile workers `4`, final sync `550/550`, profile `250/250`, no progress/snapshot/materialization guardrail failures.
  - Lovable: `harvest_company_employees=1`, `harvest_profile_search=2`, `harvest_profile_scraper_batch=7`, final sync `140/140`, profile `140/140`, no progress/snapshot/materialization guardrail failures.

### Live Harvest actor submit now defaults Apify webhook URLs by runtime

- Closed the remaining workflow-level webhook gap: `src/sourcing_agent/harvest_connectors.py` now resolves an Apify ad-hoc webhook URL even when the launcher did not explicitly set `SOURCING_APIFY_WEBHOOK_URL`, as long as the runtime is live and the environment is recognized:
  - `production + live` -> `https://api.111874.xyz/api/providers/apify/webhook`
  - `local_dev + live` -> `https://api.111874.xyz/local-dev/providers/apify/webhook`
- Explicit request/env URLs still win (`apify_webhook_url`, `provider_webhook_url`, `SOURCING_APIFY_WEBHOOK_URL`, `APIFY_WEBHOOK_URL`), and `scripted/simulate/replay` do not auto-attach external Apify webhooks.
- Added `SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED=0` as the deliberate opt-out for no-webhook diagnostics.
- Updated `scripts/apify_webhook_preflight.py` so it checks the effective submit contract, including default URLs, instead of only checking whether a manual URL env var exists.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py -q -k 'webhook or ad_hoc_webhook or default_webhook'` -> `6 passed, 85 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_remote_provider_events.py -q -k 'preflight or remote_provider_event or webhook or local_provider_event_watcher'` -> `16 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py scripts/apify_webhook_preflight.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode hosted --skip-connectivity-probe --json-only` -> `status=ready`, `would_attach_webhooks=true`, default URL `https://api.111874.xyz/api/providers/apify/webhook`
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode local-live --skip-connectivity-probe --json-only` -> `status=ready`, `would_attach_webhooks=true`, default URL `https://api.111874.xyz/local-dev/providers/apify/webhook`
- Remaining boundary: default URL attachment does not prove network delivery. Hosted production still needs deployment verification that `/api/providers/apify/webhook` is current code and returns `202`; local-dev still needs the reverse tunnel running for real Apify callbacks.

### Runtime preflight unified entrypoint added

- Added `docs/RUNTIME_PREFLIGHT.md` as the canonical startup/readiness entrypoint before local dev, scripted/browser test environments, local live smoke, and hosted/ECS workflows.
- The preflight doc consolidates previously scattered guidance from `DEVELOPMENT_GUIDE.md`, `TEST_ENVIRONMENT.md`, `APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`, `ECS_PRELAUNCH_CHECKLIST.md`, and `frontend-demo/README.md`.
- It explicitly records the local process-lifetime failure mode: foreground dev servers, raw `nohup ... &`, and background `&` can be cleaned up with transient tool sessions; use durable launch targets like `make dev-launch-backend` / `make dev-launch-frontend`, or a named `screen` session for scripted interactive environments.
- It also centralizes the webhook boundary: local-dev relay smoke does not prove hosted production route readiness, and actors submitted without `SOURCING_APIFY_WEBHOOK_URL` in the actual backend/worker process do not get Apify ad-hoc webhooks.
- Linked it from `docs/INDEX.md` and added an `AGENTS.md` local-environment guardrail requiring agents to read the preflight before launching runtime services or live smoke.

### Apify webhook preflight and bidirectional duplicate-event guard documented

- Added `scripts/apify_webhook_preflight.py` as a no-cost readiness gate before live Apify/Harvest workflows. It verifies live webhook URL presence, hosted vs local-dev relay path, Harvest profile-scraper token availability, submit-contract ad-hoc webhook attachment, and optional HTTP `202` connectivity probe without submitting actors.
- Locked the watcher/webhook/recovery race contract in tests:
  - watcher-first then webhook-late: completed worker records `remote_provider_event: received_late`, `recovery_count=0`
  - webhook-first then watcher-late: completed worker records `remote_provider_event: received_late`, `recovery_count=0`
  - neither late path can retrigger recovery, provider submit, ingest, materialization, or reconcile
- Documented the operational rule in `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`, `docs/ECS_PRELAUNCH_CHECKLIST.md`, `docs/TESTING_PLAYBOOK.md`, and `docs/NEXT_TODO.md`: the backend/worker process that submits Harvest actors must be started with `SOURCING_APIFY_WEBHOOK_URL`; otherwise the submit contract attaches no Apify ad-hoc webhook and the job may only be completed by local watcher/recovery.
- Reconciled the 2026-04-29 OpenAI ChatGPT live-smoke finding: DB events were all `source=local_provider_event_watcher`, so that run did not validate external Apify webhook delivery. The likely causes are missing webhook env in the submitting process, public relay/backend reachability failure, or token/endpoint mismatch.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_remote_provider_events.py -q -k 'preflight or remote_provider_event or webhook or local_provider_event_watcher'` -> `15 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py tests/test_remote_provider_events.py -q -k 'ad_hoc_webhook or provider_webhook or remote_provider_event or local_provider_event_watcher or preflight'` -> `16 passed, 86 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check scripts/apify_webhook_preflight.py tests/test_remote_provider_events.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --help` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode scripted --json-only` -> `status=ready`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `git diff --check -- PROGRESS.md docs/ECS_PRELAUNCH_CHECKLIST.md docs/TESTING_PLAYBOOK.md docs/NEXT_TODO.md` -> passed
- Remaining boundary: hosted production external webhook still needs deployment verification on `https://api.111874.xyz/api/providers/apify/webhook`; local-dev relay smoke does not prove the hosted production route.

### Execution progress wording and materialized-board count contract tightened

- Frontend execution-process metrics now hide internal `Evidence` and `Workers / lanes` concepts from the user-facing flow. Visible metrics use product wording: `新取回在职候选人`, `新取回离职候选人`, `总候选人数量`, and `需人工审核候选人`.
- `result_view_lifecycle` now exposes `delta_profile_materialized_count`; the candidate sync card prefers that explicit backend contract for `已物化到看板` instead of deriving materialized progress from fetched count or `served_candidate_count - baseline_candidate_count`.
- Added `configs/scripted/openai_agent_and_lovable_streaming.json` and include-merging support in the scripted provider scenario loader. The interactive scripted environment now seeds OpenAI baseline plus Lovable identity and can test both `帮我找OpenAI做Agent方向的人` and `帮我找Lovable的全部成员` from one no-cost backend/frontend.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py tests/test_scripted_provider_scenario.py -q` -> `11 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "delta_profile_progress_uses_profile_batches_for_fetch_required_count or result_view_lifecycle_uses_explicit_materialized_count_for_board_progress or result_view_lifecycle_infers_baseline_count_from_stage1_preview or full_local_reuse_suppresses_delta_profile_progress"` -> `3 passed, 63 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/scripted_provider_scenario.py tests/test_results_api.py tests/test_scripted_provider_scenario.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
- Manual scripted test env restored:
  - frontend: `http://127.0.0.1:4185`
  - backend: `http://127.0.0.1:8785`
  - provider mode: scripted; no live Harvest/DataForSEO calls.

### Delta/browser hard gates cover stage contract, sync lag, and overlay ordering

- Closed the remaining hard-browser regressions after the OpenAI ChatGPT/Agent live-smoke follow-up:
  - `/api/jobs/{job_id}/dashboard` and `/progress` now expose `execution_phase_contract`; the frontend prefers that contract for execution-process stage labels/details so it does not surface `Public Web Stage 2` while LinkedIn acquisition or local asset materialization is still active.
  - Candidate dashboard hydration now carries refreshed `linkedin_stage_1_progress` and `result_view_lifecycle` through page merges. Active delta/materialization result views poll at 1s while stable asset views keep the 5s interval, so the sync card stops lingering on stale `300/300` after the backend knows `expected_candidate_count` has grown.
  - The browser observer still fails on stale completed sync, but now uses a 2.5s budget so one-frame API/UI sampling skew does not hide or create false failures.
  - Delta asset overlays now preserve baseline order and append true delta additions, preventing the temporary overlay first page from jumping from `0103/0104/0105` to `0001/0002/0003` after final materialization.
- Added/updated regression coverage:
  - frontend sync formatter covers baseline-serving delta pending, e.g. `300/388` with `新增 LinkedIn Profile 已取回 0/88`.
  - candidate materialization covers stable baseline-first overlay ordering.
  - browser hard-mode assertions now cover premature public-web labels, stale complete sync beyond budget, materialized-ahead-of-fetched, progress regressions, and snapshot divergence.
- Latest heavy browser pass:
  - OpenAI Agent baseline+delta: `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`, job `557df2315ab8`, `harvest_profile_search=4`, `harvest_profile_scraper_batch=12`, baseline-first and final-current observed, no stale-sync/progress/snapshot/materialization guardrail failures.
  - OpenAI ChatGPT baseline+delta: `output/playwright/frontend-browser-e2e-openai-chatgpt-delta-streaming-report.json`, job `3e936f18d309`, `harvest_profile_search=4`, `harvest_profile_scraper_batch=10`, baseline-first and final-current observed, no stale-sync/progress/snapshot/materialization guardrail failures.
  - Lovable live roster: `output/playwright/frontend-browser-e2e-lovable-live-roster-streaming-report.json`, job `3ca5152d880e`, `harvest_company_employees=1`, `harvest_profile_search=1`, `harvest_profile_scraper_batch=7`, final live-roster sync `145/145`, no guardrail failures.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py -q` -> `5 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_candidate_artifacts.py -q -k "asset_population_overlay_preserves_baseline_order"` -> `1 passed, 50 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/candidate_materialization.py tests/test_candidate_artifacts.py tests/test_frontend_candidate_sync_summary.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k "openai_agent_delta_streaming_contract_observation or openai_chatgpt_delta_streaming_contract_observation or lovable_live_roster_streaming_contract_observation"` -> `3 passed, 9 deselected`
- Boundary:
  - Scripted browser mode validates local webhook endpoint/event ingestion and job-scoped recovery using Apify-shaped terminal events. The real external Apify network callback roundtrip remains an explicit tunnel/hosted live smoke, not part of the default no-cost loop.


## 2026-04-29 (Asia/Shanghai)

### Result-view pointer consistency and heavy scripted browser gates re-run

- Closed the remaining live-roster progress gap exposed by the Lovable heavy browser fixture: `linkedin_stage_1_progress` now consumes completed `harvest_company_employees` roster snapshots in addition to search-seed entries, so full live roster execution metrics count current roster rows instead of showing `已取回在职候选人0`.
- Added a results API regression that builds a Lovable live-roster shape with company-employees current rows, former search-seed rows, and a profile-scraper batch, then asserts `current=3`, `former=2`, `deduped=5`, and profile required/fetched/queued counts from the same contract.
- Tightened the Lovable heavy browser assertion so it fails unless live-roster samples observe at least `100` current returned rows in dashboard/progress, preventing the previous “browser passed while execution metrics were wrong” gap.
- Re-ran both heavy scripted browser paths after the fix:
  - OpenAI ChatGPT baseline+delta: report `output/playwright/frontend-browser-e2e-openai-chatgpt-delta-streaming-report.json`, job `5e96732b7d1c`, provider invocations `14` (`harvest_profile_search=4`, `harvest_profile_scraper_batch=10`), max progress `151 current / 78 former / 229 deduped`, no snapshot divergence, no progress regression, no materialization-stuck-after-fetch.
  - Lovable live roster: report `output/playwright/frontend-browser-e2e-lovable-live-roster-streaming-report.json`, job `16b3ebc2844d`, provider invocations `9` (`harvest_company_employees=1`, `harvest_profile_search=1`, `harvest_profile_scraper_batch=7`), max progress `120 current / 25 former / 145 deduped`, final sync `候选人同步145/145本次 LinkedIn Profile 已取回 145/145，已物化到看板 145/145`, no snapshot divergence, no progress regression, no materialization-stuck-after-fetch.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "live_roster_stage1_progress_counts_company_employee_roster_entries or delta_profile_progress_uses_profile_batches_for_fetch_required_count or baseline_serving_result_view or live_roster_preview"` -> `4 passed, 58 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py -q` -> `3 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py tests/test_frontend_browser_e2e.py` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k "openai_chatgpt or lovable_live_roster"` -> `2 passed, 10 deselected`

### OpenAI ChatGPT live-smoke scheduler/materialization follow-ups fixed

- Completed-job inline reconcile now has a narrow cross-process in-flight slot keyed as `completed_workflow_reconcile:{job_id}:{kind}`. Worker-completion callbacks still avoid the broad workflow job lease, but duplicate concurrent callbacks for the same job/kind now emit a structured `phase=coalesced` skip instead of re-entering materialize/retrieval.
- Callback reconcile re-collects unconsumed inline workers after acquiring the slot, so stale worker lists captured before another callback writes `inline_incremental_ingest` cannot drive repeated materialization.
- Materialize signatures now include worker IDs, and scripted smoke expectations can cap `repeated_materialize_signature_count` and `same_worker_reconcile_repeat_count` at zero.
- Provider completion discovery is more robust in local/hosted live smoke: local provider-event watcher fallback is enabled by default even when an Apify webhook URL is configured. Strict external webhook roundtrip smoke can still disable it with `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=0`.
- Added the OpenAI ChatGPT scoped-delta scripted fixture and matrix (`configs/scripted/openai_chatgpt_scoped_delta_streaming.json`, `configs/scripted/openai_chatgpt_scoped_delta_smoke_matrix.json`) seeded from the 2026-04-29 live-smoke shape: baseline `890`, final `1061`, search `166 current / 84 former`, deduped `250`, profile batch scope `165`.
- Tightened the ChatGPT matrix so it can no longer pass as a generic new job: expectations now require `dispatch_strategy=delta_from_snapshot`, `planner_mode=delta_from_snapshot`, `requires_delta_acquisition=true`, `effective_acquisition_mode=baseline_reuse_with_delta`, and keyword `ChatGPT`.
- The ChatGPT matrix explicitly sets `harvest_profile_actor_global_inflight=4` / `harvest_profile_batch_submit_global_inflight=4`; the latest isolated run observed `queued_worker_count=4`, `waiting_remote_harvest_count=4`, and `active_worker_count=4`.
- Latest isolated ChatGPT scripted run (`output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_report_current.json`) passed with `provider_invocations=14` (`harvest_profile_search=4`, `harvest_profile_scraper_batch=10`), `profile_url_total_count=529`, `fetched_profile_count=529`, `board_total_candidates=529`, `repeated_materialize_signature_count=0`, and `same_worker_reconcile_repeat_count=0`. The synthetic reference runtime uses a smaller OpenAI baseline than the live `890` run, but still exercises the large scoped-delta provider/materialization path.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "wrong_delta_dispatch_contract or repeated_materialize_churn or remote_actor_worker_count"` -> `2 passed, 24 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/openai_chatgpt_scoped_delta_streaming_foundation_current --seed-reference-runtime --provider-mode scripted --scripted-scenario configs/scripted/openai_chatgpt_scoped_delta_streaming.json --matrix-file configs/scripted/openai_chatgpt_scoped_delta_smoke_matrix.json --case openai_chatgpt_scoped_delta_streaming --fast-runtime --runtime-tuning-profile fast_smoke --poll-seconds 0.1 --max-poll-seconds 180 --strict --timing-summary --report-json output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_report_current.json --summary-json output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_summary_current.json` -> passed
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "harvest_prefetch or completed_workflow or inline_incremental or background_snapshot_materialization"` -> `26 passed, 305 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_remote_provider_events.py -q` -> `55 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -q` -> `33 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q` -> `30 passed`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed

### OpenAI ChatGPT live-smoke result-view and progress regressions first-pass fixed

- Live-smoke case:
  - Query: `我想要OpenAI在ChatGPT组的人`
  - Job: `6fccb376681e`, history `e1ed3b16-ae33-4e97-a2ed-c4e9e29388fc`, current snapshot `20260429T174612`
- Backend fixes:
  - `job_result_views` recovery now prefers the final `candidate_source` / current workflow snapshot over stale Stage 1 baseline artifacts. The live job now repairs from baseline `20260428T183413 / 890` to current `20260429T174612 / 1061`.
  - `linkedin_stage_1_progress.profile_fetch_required_count` now uses the actual profile-scraper batch scope when profile workers exist, instead of jumping to all deduped search seed URLs after the search snapshot is restored. For the live job this stabilizes at search returned `166 current + 84 former = 250 deduped`, profile batch required/fetched `165/165`.
  - `result_view_lifecycle` now infers `baseline_candidate_count` from the Stage 1 baseline preview when the served view has already repointed to the current snapshot, so frontend materialized-delta math no longer has to guess.
  - Workflow stage summaries ignore stale runtime fields from reused snapshot summary files when their timestamps predate the current job. This prevents a new delta job from showing another job's old Stage 1 timestamps.
- Frontend/scripted fixes:
  - Candidate sync uses backend lifecycle served/expected counts instead of the number of hydrated frontend candidate pages, avoiding `890 -> 256 -> 96` display regressions during background page hydration.
  - Execution metrics label the profile-detail scope as `需补取 LinkedIn Profile` to distinguish it from all deduped search results.
  - When LinkedIn profile work is still pending, the execution timeline keeps LinkedIn Stage 1 visibly running instead of advancing to Public Web/Final stages based only on stale stage summary status.
  - Browser scripted observation now records candidate-source/result-view/lifecycle snapshot IDs, candidate-sync parsed counts, non-monotonic progress regressions, snapshot divergence samples, and "fetched complete but materialization still baseline" samples.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_results_api.py -k "delta_progress_exposes_search_and_profile_fetch_counts or profile_batches_for_fetch_required_count or baseline_count_from_stage1_preview or stale_snapshot_stage_runtime_fields or full_local_asset_reuse_lifecycle" tests/test_frontend_candidate_sync_summary.py` -> `5 passed, 57 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_pipeline.py -k "final_candidate_source_over_stage1_baseline or materialized_current_job_snapshot_over_stale_result_view or stage_summary" tests/test_results_api.py -k "delta_progress or result_view_lifecycle or full_local_asset_reuse_lifecycle or stale_snapshot_stage_runtime_fields" tests/test_frontend_candidate_sync_summary.py` -> `4 passed, 388 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_results_api.py tests/test_pipeline.py -k "result_view or stage_summary or delta_progress or full_local_asset_reuse or harvest_profile_completion or harvest_prefetch or completed_workflow or inline_incremental"` -> `37 passed, 352 deselected`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
- Remaining follow-ups:
  - Materialization/reconcile repeat coalescing and provider-slot utilization are superseded by the scheduler/materialization follow-up entry above.
  - External Apify/Harvest webhook network roundtrip remains a separate live/tunnel smoke; the scripted browser mode only validates the local webhook endpoint/recovery path.

### Excel action buttons hidden and ChatGPT live-smoke regressions documented

- Frontend safety change:
  - The Excel batch group buttons `导入目标候选人` and `导出候选人包` are hidden from the new-search page until the real product contract is completed.
  - The underlying handlers are left in place behind a closed feature flag so the action can be re-enabled after job-scoped import/export semantics and browser E2E are ready.
- Live-smoke handoff:
  - The OpenAI ChatGPT-group run `6fccb376681e` / history `e1ed3b16-ae33-4e97-a2ed-c4e9e29388fc` exposed non-trivial regressions in execution stage semantics, unstable profile/candidate progress counters, board hydration oscillation, result-view repoint divergence, repeated materialization/reconcile churn, and profile-scraper actor-slot utilization metrics.
  - `docs/NEXT_TODO.md` now has a dedicated high-priority section with the observed failure modes and required scripted/browser regression coverage.
- Additional diagnosis:
  - Postgres showed the completed job summary candidate source at snapshot `20260429T174612` with `1061` candidates, while `job_result_views` still served a recovered baseline view at `20260428T183413`; this supports treating the candidate-board count churn as a result-view lifecycle/repoint bug, not just frontend copy.
- Runtime stability fix:
  - The same validation pass found `worker-recovery-daemon` could crash while writing service status when a callback summary contained a domain object such as `Candidate`.
  - `service_daemon` now serializes status/log payloads through a JSON-safe conversion that respects `to_record()`, so diagnostic summaries cannot kill the daemon.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_service_daemon.py -k "serializes_domain_objects or service_run_writes_status"` -> `2 passed, 12 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Excel intake result views expose a job-scoped import filter

- Product issue:
  - Excel intake now serves the company asset board quickly, but the board can contain the whole company snapshot; users need a direct way to locate the rows imported by the current workbook.
- Contract:
  - Excel workflow records a result-view-scoped `job_scoped_candidate_markers` entry with `marker_id=excel_intake:current_job`, label `本次Excel导入`, and only the candidate IDs matched or created by this Excel job.
  - Dashboard serialization attaches that marker as candidate provenance only for the current job result view; it does not mutate global candidate assets.
  - The frontend recall filter adds a `本次Excel导入` bucket when marked candidates are present, so imported rows can be isolated from the full company board.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_frontend_history_recovery.py -k "excel_intake_workflow"` -> `4 passed, 17 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_frontend_candidate_filters.py` -> `1 passed`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_excel_intake.py` -> `17 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Excel intake result-view serving no longer blocks on full artifact rebuild

- Root-cause finding from the 17:10 Excel upload retry:
  - The three Excel jobs (`ed2560030342` OpenAI, `5dd7314aefc2` Google, `34cd5da542b6` Meta) were left `running/acquiring` after the local dev backend/frontend process died.
  - Before that process exit, Excel intake had already started writing large snapshot artifacts (notably Meta `normalized_artifacts/candidates/*`), because the workflow passed `build_artifacts=True` into `ExcelIntakeService.ingest_contacts`.
  - The orchestrator only wrote `acquiring/completed`, preview, result view, and history `results` after `ingest_contacts()` returned, so a large inline artifact rebuild could make Excel upload look stuck and prevent the candidate board from becoming visible.
- Backend fix:
  - Excel workflow now keeps the user-requested `build_artifacts=True` as intent, but forces the synchronous intake/merge call to `build_artifacts=False`.
  - The workflow serves the candidate board from merged `candidate_documents.json` plus the job-scoped asset-population overlay/result view.
  - The final summary and company-asset stage expose `artifact_build_deferred=true`, making the behavior explicit instead of silently pretending full normalized artifacts were rebuilt inline.
- Frontend polish:
  - Manual-review candidate action text is now `打开LinkedIn`.
  - Manual-review bottom actions align the LinkedIn button with the review-status select control.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_frontend_history_recovery.py -k "excel_intake_workflow"` -> `4 passed, 17 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_excel_intake.py` -> `17 passed`
  - `npm run build` in `frontend-demo` -> passed

### Excel intake LinkedIn identity matching contract tightened

- Root-cause finding from the stuck 16:19 Excel upload jobs: the workbook LinkedIn URLs were present in `linkedin_profile_registry` as `fetched`, but Excel intake built local candidate inventory before checking registry/cache. On a large local asset set this can stall the thread before any result event is written.
- Backend fix:
  - direct LinkedIn URL rows now first load fetched registry raw profile cache by normalized/sanity URL key, without scanning full candidate inventory and without rejecting on stale/wrong Excel company fields
  - URL/slug local exact match is now global identity matching; company fields no longer veto a URL identity hit
  - company+name/email/near match remains a fallback for rows without URL or without URL identity/cache hit
  - `OpenAI & Meta` and `Meta/OpenAI` company cells split into separate OpenAI/Meta route jobs; each route does company+name fallback against its route company
- Ops contract documented in `docs/WORKFLOW_OPERATIONS_PLAYBOOK.md`.
- Validation:
  - `PYTHONPATH=src ./.venv/bin/python -m py_compile src/sourcing_agent/excel_intake.py src/sourcing_agent/storage.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python -m pytest tests/test_excel_intake.py -q -k 'registry_cache or compound_company or exact_local_linkedin or routes_profile'` -> `4 passed, 13 deselected`
  - `PYTHONPATH=src ./.venv/bin/python -m pytest tests/test_excel_intake.py -q` -> `17 passed`

### Candidate sync card separates board load, intent recall, and provider fetch

- Live smoke `de24677af2d1` (`帮我找OpenAI做Agent方向的人`) exposed a frontend population-contract bug:
  - the card displayed `候选人同步 890/890` from the served full OpenAI asset snapshot, not from frontend board hydration progress
  - it also displayed `新增 LinkedIn Profile 已取回 4/4，已物化到看板 4/4`, which conflated this run's provider fetch micro-progress with the Agent recall/filter population (`Agent 46`)
- Frontend fix:
  - added `frontend-demo/src/lib/candidateSyncSummary.ts` as the canonical sync-card formatter
  - `候选人同步` now uses loaded board candidates over expected board candidates, so hydration can visibly progress instead of always inheriting `result_view_lifecycle.served_candidate_count`
  - recall bucket counts now render separately as `当前意图匹配 Agent 46 人`
  - initial split made `4/4` a separate provider-fetch line instead of a materialized-delta line; the follow-up entry below tightens full-local-reuse further and hides that line when no delta/new profile acquisition is applicable
- Validation:
  - `./.venv/bin/python -m pytest tests/test_frontend_candidate_sync_summary.py -q` -> `2 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Full-local-reuse candidate sync no longer shows provider micro-progress

- Follow-up on the same live smoke: full local asset reuse should not show `本次 LinkedIn Profile 已取回 4/4`, because no delta/new profile acquisition is part of the serving contract.
- Backend fix:
  - `result_view_lifecycle` now exposes `delta_profile_progress_applicable` and `delta_profile_progress_reason`.
  - For `full_local_asset_reuse` / `reuse_snapshot_only` with `requires_delta_acquisition=false`, lifecycle `delta_profile_*` counters are zeroed and marked `not_applicable_full_local_asset_reuse`, even if diagnostic `linkedin_stage_1_progress` still has historical/search-seed micro counts.
- Frontend fix:
  - `DashboardData` now carries `effectiveExecutionSemantics`.
  - `candidateSyncSummary` hides profile fetch text when lifecycle marks delta profile progress as not applicable, or when frontend sees `fullLocalAssetReuse && !requiresDeltaAcquisition`.
  - Live roster/full acquisition remains covered: when progress is applicable and full-local-reuse is false, the card still renders `本次 LinkedIn Profile 已取回 fetched/required`.
- Verified on current local page:
  - `http://127.0.0.1:4173/?history=a817f06e-a3d7-4c9a-8a48-3fb1e2a223ee&job=de24677af2d1`
  - visible text is now `候选人同步 890/890` and `当前意图匹配 Agent 46 人`; no `LinkedIn Profile 已取回 4/4` line.
- Validation:
  - `./.venv/bin/python -m pytest tests/test_results_api.py -q -k 'delta_progress_exposes_search_and_profile_fetch_counts or full_local_asset_reuse_lifecycle_suppresses_delta_profile_progress'` -> `2 passed, 54 deselected`
  - `./.venv/bin/python -m pytest tests/test_frontend_candidate_sync_summary.py -q` -> `3 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Scripted webhook-event browser mode and handoff metrics split

- Added a no-cost scripted webhook-event driver to the OpenAI Agent delta-streaming browser observation:
  - `frontend-demo/scripts/run_workflow_e2e.mjs --drive-provider-webhook-events` finds waiting remote provider workers, posts terminal Apify-shaped payloads to `/api/providers/apify/webhook?sync=1`, and records `providerWebhookEvents`.
  - This is deliberately different from the older `--drive-worker-recovery` path: the browser driver no longer calls `/api/workers/daemon/run-once` directly when webhook mode is enabled.
  - Coverage boundary: this validates local webhook endpoint -> `remote_provider_event` -> job-scoped recovery -> next worker submit. It still does not test the external Apify network callback roundtrip.
- Split event-level metric naming so scheduler handoff and provider-attempt time are not conflated:
  - `local_completion_to_next_submit_start_ms` is the pure local handoff from completion apply to next-submit start.
  - `next_submit_provider_attempt_elapsed_ms` is the next-submit attempt duration and can include scripted provider sleep/pending behavior.
  - Existing `local_to_next_submit_start_ms` and `next_submit_attempt_elapsed_ms` remain as compatibility aliases.
- Candidate board sync card layout was adjusted:
  - right card now renders `候选人同步 N/M` on the first line and `新增 LinkedIn Profile 已取回 X/Y，已物化到看板 A/Y` on the second line
  - search field width was reduced so the sync card can fit two aligned lines on desktop
  - browser observer now recognizes the new `LinkedIn Profile 已取回 X/Y` wording for partial-progress assertions
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py tests/test_remote_provider_events.py -q` -> `17 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'provider_case_report or event_level_efficiency'` -> `5 passed, 19 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_efficiency.py tests/test_workflow_efficiency.py tests/test_remote_provider_events.py tests/test_frontend_browser_e2e.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_agent_delta_streaming_contract_observation'` -> `1 passed, 9 deselected`
- Latest browser report:
  - `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`
  - job `61b3d7b4a5de`
  - `providerWebhookDriven=true`, `workerRecoveryDriven=false`, `providerWebhookEvents=4`
  - `baselineFirstBoardObserved=true`, `finalCurrentSnapshotObserved=true`, `candidateSyncProfileProgressObserved=true`
  - observed partial sync text: `候选人同步400/597新增 LinkedIn Profile 已取回 100/297，已物化到看板 100/297`

### Lovable 100+ scripted live-roster smoke added

- Replaced the too-small Physical Intelligence live-roster scripted gate with a Lovable fixture that exercises a 100+ person full-roster path without live provider calls.
- Added `configs/scripted/lovable_live_roster.json`:
  - `harvest_company_employees` returns `120` unique Lovable roster rows.
  - `harvest_profile_search` returns `25` former Lovable rows, preventing broad former-lane zero-result retry from being misread as duplicate dispatch in this smoke.
  - `harvest_profile_scraper_batch` hydrates profile details with realistic work/education fields from requested URLs.
- Updated `configs/scripted/small_company_live_roster_smoke_matrix.json` to run `帮我找Lovable的全部成员` and require:
  - terminal job
  - no duplicate provider dispatch
  - no unexpected Public Web/DataForSEO stage
  - no event-level efficiency violation
  - `harvest_company_employees>=1`, `harvest_profile_scraper_batch>=1`
  - `board_total_candidates>=100`
- First Lovable run failed usefully: all generated roster rows shared the same `fullName` (`Lovable Roster`), so canonicalization collapsed `120` rows into `1` candidate; the former-lane broad profile-search also had no fixture rule and triggered zero-result retry duplicates. The fixture now generates unique names and has an explicit former profile-search rule.
- Passing run:
  - report: `output/scripted_smoke_current/lovable_live_roster_scripted_report.json`
  - summary: `output/scripted_smoke_current/lovable_live_roster_scripted_summary.json`
  - job: `7933a0ff59cf`
  - `expectation_failures=[]`
  - provider invocations: `10` total (`harvest_company_employees=1`, `harvest_profile_search=1`, `harvest_profile_scraper_batch=6`, plus 2 scripted company identity searches)
  - board: `145` total candidates, first page `24`, profile fetch progress `145/145`
  - guardrails: duplicate provider dispatch `false`, default-off public web violation `false`, event-level efficiency violation `false`
  - post-terminal recovery settled in `1` round with no remaining recoverable workers
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py -q -k 'lovable_live_roster_scripted_fixture or company_employees_scripted_completion_exposes_dataset_items_artifact or harvest_profile_batch_execute_with_checkpoint_supports_scripted_pending_rounds'` -> `3 passed, 84 deselected`
  - `./.venv-tests/bin/ruff check tests/test_harvest_connectors.py` -> passed
  - Lovable scripted smoke command in `docs/TESTING_PLAYBOOK.md` -> passed

### OpenAI Agent slow/strict browser observation completed

- Ran the full OpenAI Agent slow/strict scripted browser observation against isolated runtime `runtime/test_env/openai_agent_delta_streaming_slow_strict`.
- Coverage boundary confirmed:
  - This does not test the real external Apify/Harvest webhook network callback.
  - It tests webhook-equivalent local provider completion/recovery/reconcile scheduling after scripted provider completion, including long elapsed waits and post-terminal recovery.
- Result:
  - job: `4f0f5f991133`
  - report: `output/playwright/openai-agent-slow-strict-browser-report.json`
  - `contractReady=true`, `baselineFirstBoardObserved=true`, `finalCurrentSnapshotObserved=true`
  - baseline board first observed at baseline `20260414T120300`, `300/300`
  - final current snapshot observed at `20260429T134659`, expected `597`
  - provider invocations: `17` total (`harvest_profile_search=4`, `harvest_profile_scraper_batch=13`)
  - profile batch workers drained with `recoverable_worker_count=0`
  - full post-terminal tail finalized around `37m41s` after job creation
- Observability gaps found and left as backlog:
  - browser observer currently exits when workflow reaches completed, before very long post-terminal provider/materialization tails are always drained
  - `/health` can clear remote-wait metrics after the job is terminal even if completed-job background workers remain active/recoverable

### Delta candidate sync card progress wording corrected

- Manual interactive scripted testing showed the candidate board could display `候选人同步 597/597` while only `50/297` new LinkedIn profiles had been fetched. Root cause: the card used the discovered/current snapshot candidate count as "served" even while the delta profile tail was still running.
- Frontend sync card now uses `baseline_candidate_count + fetched_delta_profile_count` while new LinkedIn profile fetch is still in progress, then switches back to final `served/expected` once the delta tail is complete.
- User-facing wording changed from `LinkedIn fetched/total，追平，排队，可重试` to `新增 LinkedIn Profile 已取回 fetched/required，已物化到看板 materialized/required` plus retryable only when non-zero. `materialized` is derived from board-visible candidate sync progress so the card separates provider fetch progress from what the user can actually consume on the board.
- Validation:
  - `npm --prefix frontend-demo run build` -> passed

### OpenAI Agent scripted slow/strict worker timing matrix added

- Added a live-like timing matrix to `configs/scripted/openai_agent_scoped_delta_streaming.json` for scheduler/recovery reliability testing:
  - profile-scraper batches now have deterministic varied durations: `45s`, `60s`, `90s`, `120s`, and `180s`.
  - A later current-lane batch is intentionally faster than earlier batches to exercise out-of-order completion.
  - A current mid-tail batch carries a retryable provider delay.
  - A former long-tail batch carries a retryable timeout on the primary OpenAI Agent path, not only a dormant fixture rule.
- Added `scripts/dev_scripted_openai_agent_delta.sh --slow-strict-runtime` / `SCRIPTED_SLOW_STRICT_RUNTIME=1`:
  - default interactive mode still caps scripted Harvest sleeps at `3s`
  - fast mode still caps at `0.1s`
  - slow/strict mode sets `SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=none`, preserving fixture sleep durations for live-like elapsed-time tests
- Scripted Harvest timeout errors can now opt into retryable behavior with `"retryable": true`; non-retryable timeout rules still fail as before.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_harvest_connectors.py -q -k 'openai_agent_scripted_profile_scraper or openai_agent_scoped_delta_streaming_fixture or scripted_profile_scraper_timing_matrix'` -> `3 passed, 86 deselected`
  - `bash -n scripts/dev_scripted_openai_agent_delta.sh` -> passed
  - `bash ./scripts/dev_scripted_openai_agent_delta.sh --slow-strict-runtime --print-config` -> `slow_strict_runtime=1`, `scripted_harvest_sleep_seconds_cap=none`
  - `bash ./scripts/dev_scripted_openai_agent_delta.sh --fast-runtime --print-config` -> `slow_strict_runtime=0`, `scripted_harvest_sleep_seconds_cap=0.1`

### Interactive OpenAI Agent scripted delta environment exposed

- Added a user-facing interactive scripted test entry for the Delta asset board streaming scenario:
  - `make test-env-scripted-openai-agent` seeds the OpenAI reference baseline, starts an isolated scripted backend/worker daemon, and launches the frontend.
  - `make test-env-scripted-openai-agent-backend` starts backend only; `status` / `logs` / `stop` targets are available for the same runtime namespace.
  - Default runtime: `runtime/test_env/openai_agent_delta_streaming`; backend `8785`, frontend `4185`.
- Safety boundary:
  - The entrypoint forces `SOURCING_EXTERNAL_PROVIDER_MODE=scripted` and uses `configs/scripted/openai_agent_scoped_delta_streaming.json`.
  - Default secrets now live under the isolated runtime (`runtime/test_env/openai_agent_delta_streaming/secrets/providers.local.json`) and are initialized as empty JSON, so this interactive path does not read repo live provider secrets by default.
  - It does not test the external Apify webhook network path; it tests scripted provider completion plus local daemon/recovery-driven reconcile and downstream snapshot/result-view behavior.
- Corrected the interactive realism gap found in manual testing:
  - The interactive entry now defaults to `SCRIPTED_FAST_RUNTIME=0`; scripted Harvest sleeps are capped at `3s`, not `0.1s`, so profile-search/profile-scraper pending and recovery are visible in the execution page.
  - `configs/scripted/openai_agent_scoped_delta_streaming.json` now gives profile-search rules explicit sleeps, so candidate-list acquisition is not instant.
  - Reference smoke baseline candidates now include structured `experience_lines` / `education_lines`, `education`, `work_history`, profile capture metadata, and avatar URLs; the baseline-first board no longer renders as 300 empty profiles.
  - Startup now resets the dedicated `runtime/test_env/openai_agent_delta_streaming` by default before seeding, so stale history/job artifacts from earlier scripted runs cannot hide fixture changes. `SCRIPTED_RESET_RUNTIME=0` is available only for old-job debugging.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile scripts/seed_reference_smoke_runtime.py` -> passed
  - `bash -n scripts/dev_scripted_openai_agent_delta.sh` -> passed
  - `make -n test-env-scripted-openai-agent*` command expansion -> passed
  - seed-only validation against `runtime/test_env/openai_agent_delta_streaming_validation` -> passed
  - short backend startup smoke on port `8786` -> `/health` and `/api/runtime/health` returned `ok` with `provider_mode=scripted`, then the temporary service was stopped
  - After the manual realism correction:
    - `bash -n scripts/dev_scripted_openai_agent_delta.sh` -> passed
    - `bash ./scripts/dev_scripted_openai_agent_delta.sh --print-config` -> `fast_runtime=0`, `scripted_harvest_sleep_seconds_cap=3`, `reset_runtime=1`
    - `bash ./scripts/dev_scripted_openai_agent_delta.sh --fast-runtime --print-config` -> `fast_runtime=1`, `scripted_harvest_sleep_seconds_cap=0.1`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_smoke_runtime_seed.py tests/test_harvest_connectors.py -q -k 'smoke_runtime_seed or openai_agent_scripted_profile_search or openai_agent_scripted_profile_scraper'` -> `3 passed, 82 deselected`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
    - `./.venv-tests/bin/ruff check src/sourcing_agent/smoke_runtime_seed.py tests/test_smoke_runtime_seed.py tests/test_harvest_connectors.py` -> passed
    - Restarted `make test-env-scripted-openai-agent`; `/api/frontend-history?limit=5` now returns an empty history after reset, and the seeded OpenAI baseline has `300` candidates with non-empty `experience_lines` / `education_lines`.
- Docs updated:
  - `docs/TEST_ENVIRONMENT.md` now documents the interactive OpenAI Agent scripted delta environment and its coverage/caveats.
  - `docs/TESTING_PLAYBOOK.md` links the interactive entry from the browser/user-interaction driver section.

### Delta asset board streaming hard-mode browser validation passed

- Closed the hosted/browser hard assertion gate for the OpenAI Agent scoped-delta fixture:
  - `frontend-demo/scripts/run_workflow_e2e.mjs` now runs a background delta-streaming observer independent of the main result-wait loop, so it can capture baseline-serving board states before final results are ready.
  - The observer can opt into test-side worker recovery driving (`--drive-worker-recovery`) to mirror the hosted daemon path in isolated browser tests; it waits until the baseline board has been observed before driving recovery, so recovery cannot hide the baseline-first window.
  - Hard assertions now cover baseline-first board visibility, final current-snapshot serving, execution-process metrics, candidate-sync metrics, profile progress from non-zero to completed, and visible UI text for both execution and candidate sync profile counts.
- Latest hard-mode report:
  - report: `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`
  - `contractReady=true`, `baselineFirstBoardObserved=true`, `finalCurrentSnapshotObserved=true`
  - first board sample: `~3.1s`, baseline `20260414T120300`, visible board `300/400`, lifecycle `delta_applying`
  - profile progress samples: execution page observed `已取回 LinkedIn Profile21`, then candidate sync observed `LinkedIn 100/297`, and final sync reached `LinkedIn 297/297，追平 0，排队 0，可重试 0`
  - provider long-tail: `harvest_profile_search=4`, `harvest_profile_scraper_batch=13`, `total=17`
  - current snapshot: `597/597`, first-page returned count `24`
- Validation:
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_agent_delta_streaming_contract_observation'` -> `1 passed, 9 deselected`
- Remaining follow-up:
  - Use the now-green browser/user-interaction fixture to tune remaining UX bottlenecks: current candidate board still reaches the final current snapshot quickly, but materialization/finalization and post-result layer refresh should continue to be measured and improved.

### Delta asset board streaming lifecycle split started

- Implemented the first product-facing `Delta asset board streaming` contract:
  - Delta workflows now persist a baseline-serving `job_result_view` as soon as the workflow job is queued when a reusable `delta_baseline_snapshot_id` exists. This lets `/dashboard` serve the baseline asset population before current snapshot materialization finishes.
  - `/api/jobs/{job_id}/progress`, `/dashboard`, and `/candidates` now expose structured `result_view_lifecycle` with states such as `baseline_serving`, `delta_applying`, `current_snapshot_materializing`, `current_snapshot_serving`, and `post_result_layering`.
  - The same endpoints expose `linkedin_stage_1_progress`: current/former search returned counts, deduped seed/profile URL counts, profile required/fetched/queued/retryable/pending counts.
- Frontend changes:
  - The execution-process card now renders concrete Stage 1 metrics (`已取回在职候选人`, `已取回离职候选人`, `经去重得到`, `经去重需取回 LinkedIn Profile`, `已取回 LinkedIn Profile`) instead of relying only on worker counts.
  - The candidate board sync card now uses lifecycle counts, e.g. baseline/current `候选人同步 825/890`, and keeps LinkedIn profile progress (`LinkedIn fetched/required，追平，排队，可重试`) visible while the delta tail is still running.
  - Dashboard polling now continues while lifecycle state is `baseline_serving` / `delta_applying` / `current_snapshot_materializing` / `post_result_layering`, so later materialization or layering changes can refresh without a manual reload.
- Browser/scripted observation updated:
  - `frontend-demo/scripts/run_workflow_e2e.mjs --observe-delta-streaming` records lifecycle/progress payloads plus visible execution-process and candidate-sync text.
  - `SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1` now also asserts the user-facing process metrics and candidate sync card are observed.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `55 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "result_view or asset_population_fast_path or get_job_progress_uses_count_queries or get_job_progress_uses_materialized_event_summary or progress_auto_recovery or current_job_snapshot_over_stale_result_view"` -> `10 passed, 319 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
- Follow-up hard-mode browser validation is now closed in the entry above.

### OpenAI Agent delta-streaming scripted/browser foundation upgraded to real baseline+delta

- Corrected the OpenAI Agent scoped-delta foundation so it no longer uses a "claimed 1620, actual 2 candidate docs" OpenAI baseline. The reference OpenAI baseline now seeds `300` real synthetic candidate documents (`260` current, `40` former), so the scenario exercises baseline + Agent delta materialization instead of a delta-only artifact.
- Added a scripted/browser observation path for Delta asset board streaming:
  - `frontend-demo/scripts/run_workflow_e2e.mjs --observe-delta-streaming` samples DOM board count plus `/api/jobs/{job_id}/dashboard` and `/progress` while the workflow is still running.
  - `tests/test_frontend_browser_e2e.py::test_browser_openai_agent_delta_streaming_contract_observation_when_enabled` is opt-in via `SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1`.
  - Diagnostic mode records the current product gap; `SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1` flips it into the future hard assertion that baseline board must be visible before current snapshot serving.
  - Browser report path: `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`.
- Added `SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP` for isolated scripted runtimes. This keeps browser/UI-started scripted provider waits bounded without changing live provider behavior; explicit request context still wins.
- Validation:
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_agent_delta_streaming_contract_observation'` -> `1 passed, 9 deselected`
  - Browser diagnosis: first visible board came only after job terminal, current snapshot `597/597`, `baselineFirstBoardObserved=false`, `finalCurrentSnapshotObserved=true`, `contractGapDetected=true`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py ... strict_tail_v7b ...` -> passed with `expectation_failures=[]`.
  - Strict v7b report: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v7b_report.json`; summary: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v7b_summary.json`.
  - v7b key metrics: `profile_url_total_count=597`, `fetched_profile_count=597`, `board_total_candidates=597`, `post_terminal_recovery.settled=true`, `round_count=4`, `preview_to_first_materialize_start_ms=33000`, `preview_to_finalization_completed_ms=58000`, `materialize_sync_duration_ms.max=21000`, event-level efficiency violation `false`.
- Current conclusion:
  - The test foundation now captures the real UX gap: the UI still waits for current snapshot/result-view materialization before showing the board. Delta asset board streaming should next split result-view lifecycle into baseline serving, delta applying/materializing, current serving, and post-result layering.

### OpenAI Agent scoped-delta strict smoke settle contract closed

- Tightened the long-tail OpenAI Agent scripted smoke so it can no longer pass only because the front workflow reaches `completed` while profile-tail/background reconcile work is still recoverable.
- `workflow_smoke._settle_post_terminal_worker_recovery(...)` now returns a first-class `post_terminal_recovery` state:
  - `settled`
  - `remaining_recoverable_worker_count`
  - `remaining_recoverable_worker_ids`
  - `round_count`
  - `max_rounds_exhausted`
  - `no_progress_rounds`
- Added `require_post_terminal_recovery_settled=true` to `configs/scripted/openai_agent_scoped_delta_smoke_matrix.json`, so the OpenAI Agent foundation must prove terminal-job recovery drained recoverable workers before strict mode passes.
- Reclassified `behavior_guardrails.prerequisite_gaps` as diagnostic rather than a hard behavior failure. Large Stage gaps remain visible in report/summary, but a long-tail fixture no longer produces the confusing state "strict expectations passed while `behavior_guardrails.violation_detected=true`" unless an actual hard guardrail fails.
- Re-ran the strict OpenAI Agent scoped-delta smoke:
  - report: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v4_report.json`
  - summary: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v4_summary.json`
  - job: `ac72507be38e`
  - expectation failures: `[]`
  - provider invocations: `17` total (`harvest_profile_search=4`, `harvest_profile_scraper_batch=13`)
  - long-tail counts: `profile_url_total_count=299`, `fetched_profile_count=299`, `board_total_candidates=299`
  - post-terminal settle: `settled=true`, `round_count=3`, `remaining_recoverable_worker_count=0`
  - event-level efficiency: `harvest_completion_event_count=13`, `local_to_next_submit_start_ms.max=0`, `next_submit_attempt_elapsed_ms.max=575`, `repeated_materialize_signature_count=0`
  - behavior hard guardrails: `violation_detected=false`; diagnostic prerequisite gap remains visible (`stage_1_preview_to_stage_2_final_start=16000ms`)
  - service metrics caught the next UX bottleneck: max worker handoff gap `23000ms`, `bottlenecks.top_bottlenecks[0].kind=worker_handoff_gap`
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'post_terminal_worker_recovery or expectations or prerequisite_gap or provider_case_report'` -> `13 passed, 10 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py`
  - JSON validation for `configs/scripted/openai_agent_scoped_delta_smoke_matrix.json` and `configs/scripted/openai_agent_scoped_delta_streaming.json`
  - strict hosted scripted smoke command above -> passed
- Next step:
  - Wire this foundation into isolated hosted/browser E2E for baseline-first board assertions, then start the Delta asset board streaming result-view lifecycle split.

### Scripted smoke can opt into live LLM planning

- Added a bounded `SOURCING_SCRIPTED_LIVE_MODEL_PLANNING=1` contract for scripted provider tests:
  - `SOURCING_EXTERNAL_PROVIDER_MODE=scripted` still keeps Harvest/DataForSEO/profile providers offline/scripted.
  - request normalization, plan review instruction normalization, refinement normalization, intent brief drafting, and search strategy planning may use the configured live model.
  - candidate-level outreach AI verification, public-web asset analysis, profile membership AI judging, and result summarization remain deterministic/offline to avoid model fan-out during large scripted fixtures.
- Added `ScriptedLivePlanningModelClient` as a planning-only wrapper around the configured live model; default scripted/simulate/replay behavior remains `OfflineModelClient`.
- Added `scripts/run_simulate_smoke_matrix.py --live-model-planning` for isolated hosted scripted runs. The flag requires `--runtime-dir` and `--provider-mode scripted`, so it cannot silently affect an already-running server or a live-provider smoke. Because isolated runtimes use their own secrets file, the CLI forwards only model-related env (`DASHSCOPE_*` / `MODEL_PROVIDER_*`) from the current settings into the isolated run.
- Updated `docs/TESTING_PLAYBOOK.md` and `docs/NEXT_TODO.md` with the live-planning scripted command for the OpenAI Agent scoped-delta foundation.
- Ran the OpenAI Agent scoped-delta scripted smoke with live planning enabled:
  - command wrote `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_live_planning_report.json` and `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_live_planning_summary.json`
  - live planning preserved `keywords=['Agent']`, current/former `search_seed_queries=['Agent']`, and `filter_hints.keywords=['Agent']`
  - dispatch stayed `delta_from_snapshot` / `baseline_reuse_with_delta`
  - guardrails showed no duplicate provider dispatch, no unexpected public-web stage, and no event-level efficiency violation
  - remaining foundation gap: hosted smoke still accepts `results_ready_nonterminal` too early for this scenario, so the run only observed `profile_url_total_count=4`, `fetched_profile_count=2`, and `board_total_candidates=4`; it did not yet exercise the intended hundreds-row provider search plus batched profile-scraper tail.
- Next step:
  - tighten the OpenAI Agent scripted smoke settle/expectation contract so this fixture must drive the long-tail profile-search/profile-scraper path before it can pass as the Delta asset board streaming foundation.

## 2026-04-28 (Asia/Shanghai)

### Service-grade scripted metrics foundation added

- Added `src/sourcing_agent/workflow_service_metrics.py` as the reusable smoke-report metrics layer for future Delta asset board streaming and full event-level workflow optimization.
- The report now captures:
  - per-worker start/end, duration, lane/status counts, global next-worker-start gap, same-lane next-worker-start gap, slow workers, and slow handoff gaps
  - user-visible timings: first progress observed, Stage 1 preview, final results, board ready/non-empty, dashboard/candidate-page fetch, loading-feedback violations, board-readiness violations, and long post-preview finalization
  - bottleneck recommendations for slow workers, worker handoff gaps, board readiness lag, and post-preview finalization lag
- `workflow_smoke.run_hosted_smoke_case` now fetches `/api/jobs/{job_id}/trace` and passes `agent_trace_spans` into `provider_case_report.service_metrics`, so hosted/scripted reports can correlate worker rows with trace span timing.
- `summarize_smoke_timings(...)` now rolls service metrics across cases:
  - worker count / trace span count / worker duration
  - global handoff gaps
  - slow worker/gap counts
  - board-ready/non-empty and post-preview finalization UX metrics
  - bottleneck kind/severity counts
- Docs updated:
  - `docs/TESTING_PLAYBOOK.md` now defines service-grade scripted metrics as the default AI-in-loop optimization surface before touching workflow code.
  - `docs/NEXT_TODO.md` records this as the next foundation layer for the larger OpenAI Agent scoped-delta fixture and Delta asset board streaming.
  - `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` adds the service metrics regression target.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q` -> `16 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_harvest_connectors.py -q -k 'openai_agent or scripted_provider_scenario or provider_behavior_matrix or scripted or profile_search or profile_batch or zero_result'` -> `19 passed, 69 deselected`
- Next step:
  - Wire `configs/scripted/openai_agent_scoped_delta_streaming.json` into isolated hosted/browser E2E and use `provider_case_report.service_metrics` to find the first blocking point before implementing result-view lifecycle changes.

### OpenAI Agent scoped-delta scripted test foundation started

- Corrected the Delta asset board streaming test strategy:
  - Do not use the lightweight OpenAI/Pulse two-profile delta as the main driver; it is too small to expose realistic provider scheduling, dedupe, materialization, and board-readiness risks.
  - The canonical scripted driver is now an older OpenAI baseline plus a larger `帮我找OpenAI做Agent方向的人` scoped-search delta.
- Added `configs/scripted/openai_agent_scoped_delta_streaming.json`:
  - models current/former Harvest profile-search probe + scaled calls
  - returns hundreds of synthetic provider rows with duplicate LinkedIn URLs
  - models profile-scraper pending rounds, retryable delay, and actor-slot pressure
  - explicitly records that default Stage 1 DataForSEO/public-web fallback is not allowed
- Extended scripted Harvest fixtures with `generated_body` support so large provider payloads can be modeled without hand-writing hundreds of JSON rows:
  - profile-search rows honor `maxItems`, `takePages`, `startPage`, pagination totals, current/former company filters, and duplicate-url stride
  - profile-scraper rows are generated from requested URLs and include timeline fields used by downstream materialization
  - company-employees synthetic generation is available for later full-roster/employee API scenarios
- Added connector-level regressions:
  - OpenAI Agent current/former profile-search does two calls per lane (`probe` then `scale`) and never submits a live Harvest run in scripted mode
  - OpenAI Agent profile-scraper batches resume through pending/retryable rounds and generate usable profile timeline payloads
  - scenario validation now treats generated bodies as first-class fixture output
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/harvest_connectors.py src/sourcing_agent/scripted_provider_scenario.py`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py src/sourcing_agent/scripted_provider_scenario.py tests/test_harvest_connectors.py tests/test_scripted_provider_scenario.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_harvest_connectors.py -q -k 'openai_agent or scripted_provider_scenario or provider_behavior_matrix'` -> `6 passed, 82 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py -q -k 'scripted or profile_search or profile_batch or zero_result'` -> `15 passed, 69 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py tests/test_scripted_provider_scenario.py -q` -> `5 passed`
- Next step:
  - Wire this fixture into an isolated hosted/browser E2E that seeds the older OpenAI baseline, runs the Agent scoped query, records provider invocation/timing reports, and asserts baseline-first board visibility while current snapshot materialization is still running.

### Infra thematic keyword no longer becomes `infra_systems` role bucket

- Product correction:
  - `Infra` is an AI research/theme shard like `Multimodal` / `Reasoning`, not a primary role bucket.
  - LinkedIn acquisition should use provider keyword `Infra` and broad technical function ids `8/24`; it should not show or require `Infra Systems` as a search keyword.
- Fixes:
  - Removed bare `infra` / `infrastructure` from role-bucket aliasing. Explicit role phrases such as `infrastructure engineer`, `infra engineer`, `platform engineer`, and `systems engineer` still map to `infra_systems`.
  - Added `Infra` to the research-direction default thematic labels, so directional Infra requests still normalize to the default technical population (`researcher` + `engineer`) and function ids `24/8`.
  - Added request-intent sanitization so model-assisted payloads that still emit `must_have_primary_role_buckets=["infra_systems"]` for an Infra directional query are downgraded back to thematic `keywords=["Infra"]`, unless the raw text explicitly contains an infrastructure role phrase.
  - Frontend plan keyword display now filters role-bucket tokens (`infra_systems`, `product_management`, `engineering`, etc.) instead of falling back to showing them as "检索关键词". The field should reflect actual provider/search keywords such as `Infra`, not internal role labels.
- Local reproduction:
  - `帮我找PostHog做infra方向的人` now supplements to `keywords=['Infra']`, `must_have_primary_role_buckets=[]`.
  - Strategy filter hints remain `function_ids=['24','8']`, `keywords=['Infra']`; request preview no longer exposes `infra_systems`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/query_signal_knowledge.py src/sourcing_agent/domain.py src/sourcing_agent/request_normalization.py tests/test_semantic_intent.py tests/test_request_matching.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_semantic_intent.py tests/test_request_matching.py` -> `13 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_planning_modules.py -k 'infra or keyword_priority or scoped_search_provider_keywords_exclude_role_facet_terms'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_semantic_retrieval.py tests/test_post_acquisition_refinement.py -k 'infra or primary_role_bucket'` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'plan_workflow_scoped_search_override_updates_effective_execution_semantics or plan_workflow_infers_openai_scope_from_chatgpt_product_manager_query or facet or infra_systems or current_job_snapshot_over_stale_result_view'` -> `4 passed`
  - `npm --prefix frontend-demo run build` -> passed

### PostHog/Coding scoped Harvest query pollution fixed

- 真实 query/job `4850149b9183`（PostHog, `20260428T134840`）排查结论：
  - 本地 request manifest 确认不是 webhook 回传错误；5 次 Harvest actor run 都来自本地 `search_seed_discovery`
  - 计划内 payload 是 2 个：`searchQuery=Coding` + current company、`searchQuery=Coding` + former company
  - 额外 payload 是本地 query resolution 生成的：`searchQuery=Coding research` + current/former，其中 current 第一次返回 0 后触发 zero-result retry，所以 API 后台看到 5 次 run
  - 错误 summary 行为 `query=PostHog`、`effective_query_text=Coding research`：`scope_keywords=PostHog` 被当作额外 paid query，随后 `_normalize_harvest_query_text` 把公司名剥空并从 `filter_hints.keywords=['Coding','research']` 回填，导致 provider-facing query 被污染
- 上游修复：
  - `acquisition_strategy` 将 `research/researcher/employee/engineering/engineer` 定义为 generic role query keys；这些词仍可驱动 functionIds / role/facet / scoring，但不再进入 provider acquisition keyword hints
  - PostHog/Coding scoped plan 现在输出 `search_seed_queries=['Coding']`、`filter_hints.keywords=['Coding']`，不再把 `research` 拼到 Harvest search text
  - `plan_review` 过滤 `confirmed_company_scope` 中等于 target company 或其 LinkedIn company URL 的值；只确认目标公司本身时会清掉旧 `scope_keywords`，避免 `scope_keywords=PostHog` 再变成额外 paid query
- Provider 边界防御保留：
  - `_resolve_provider_people_search_queries(...)` 忽略等于 target company identity 的 `scope_keywords`
  - `_normalize_harvest_query_text(...)` 的 keyword fallback 会丢弃 generic role terms，避免旧 metadata 或手工 payload 再打出 `Coding research`
- 验证：
  - 直接复现：相同 PostHog/Coding scoped request 现在得到 `scoped_search_roster`、`search_seed_queries=['Coding']`、`filter_hints={'current_companies':['PostHog'],'function_ids':['24','8'],'keywords':['Coding']}`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/acquisition_strategy.py src/sourcing_agent/plan_review.py src/sourcing_agent/seed_discovery.py tests/test_planning_modules.py tests/test_seed_discovery.py tests/test_search_planning.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_planning_modules.py -k 'scoped_search_provider_keywords_exclude_role_facet_terms or confirmed_scope_does_not_write_target_company or keyword_priority or publication_coverage_includes_engineering_and_blog or intent_view_scope_and_keywords or organization_execution_profile_prefers_scoped_search'` -> `7 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_seed_discovery.py -k 'target_company_scope_keyword or keyword_fallback_drops_generic_role_terms or resolve_provider_people_search_queries_dedupes or normalize_harvest_query_text_is_alias_canonical or provider_people_search_dedupes_after_effective_harvest_query_normalization'` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_search_planning.py tests/test_pipeline.py -k 'plan_workflow_scoped_search_override_updates_effective_execution_semantics or plan_review_persists_execution_preferences_into_review_request_and_queued_job or build_sourcing_plan_omits_public_web_stage_by_default or public_interview_bundle_is_compiled_for_interview_queries or relationship_bundle_dedupes_company_scope_terms or scoped_search_plan_keeps_paid_people_search_as_fallback or targeted_people_search_bundle_uses_natural_keyword_queries'` -> `7 passed`
- 手测注意：
  - 需要重启 backend/daemon 才会吃到新代码
  - 下次 PostHog/Coding scoped query 预期最多只有 `Coding` current + `Coding` former 两个不同 Harvest payload；若 provider 真 0，可看到同 payload 的 zero-result retry，但不应再出现 `Coding research`

### Runtime backlog cleanup before manual recovery testing

- 手动测试前做了只读 runtime backlog audit：
  - 清理前 `active_job_count=4`，全部是 `webhook_roundtrip_20260426*` 旧 smoke residue；没有 active provider slot / remote-wait worker
  - `recoverable_worker_count=0`
  - completed-job backlog 有 4 条本地收口任务：OpenAI `185af9c5b847`、OpenAI `7967d481646c`、Safe SuperIntelligence `5d57f71a890e`、Reflection AI `02f139ea791b`
- OpenAI `185af9c5b847` 结论：
  - 用户可见 workflow 已完成，`candidate_source` / `job_result_view` 均指向 job snapshot `20260428T094936`
  - 未完成的是后台本地阶段：`background_snapshot_materialization.status=scheduled` 与 `outreach_layering.status=scheduled`
  - 尝试用完整 completed-workflow reconcile 收口时，snapshot artifact 已写出，但后续 retrieval/outreach 路径仍进入重 CPU 文件读取；为避免手测时抢 CPU，停止长跑并按已落盘 artifact 做显式归档
- 已做 cleanup：
  - `185af9c5b847`: `background_snapshot_materialization` 标为 `completed`，记录 `manifest_path` / `artifact_summary_path` / `candidate_count=887` / `page_count=18`；`outreach_layering` 标为 `skipped`；写入 `search_seed` 与 `harvest_prefetch` background reconcile cursor
  - `7967d481646c`: `outreach_layering` 标为 `skipped`；写入 `harvest_prefetch` cursor。该 job 没有 `job_result_view`，不会改 OpenAI canonical serving pointer
  - `5d57f71a890e`: `outreach_layering` 标为 `skipped`；写入 `company_roster` cursor
  - `02f139ea791b`: 写入 `search_seed` cursor
  - 4 条 `webhook_roundtrip_20260426*` running residue 已 supersede，reason=`manual_cleanup_before_recovery_test`
- 清理后验证：
  - `show-system-progress --force-refresh`: runtime `status=ok`, `active_job_count=0`, `recoverable_worker_count=0`, background reconcile counts all `0`
  - `show-recoverable-workers --stale-after-seconds 1`: `0`
  - `_discover_completed_workflow_jobs_pending_background_reconcile(limit=200)`: `[]`
  - OpenAI `185af9c5b847` 保持 `candidate_source_snapshot=20260428T094936` / `result_view_snapshot=20260428T094936`；未污染公司 canonical pointer

### Plan review scoped-search override execution semantics

- 修复 PostHog plan review 展示/语义漂移：
  - 用户在 Review 的“继续补充或修改要求”里写 `做scoped search` 后，后端 plan 已正确变成 `acquisition_strategy.strategy_type=scoped_search_roster`
  - 但 `effective_execution_semantics` 仍只读取 organization execution profile 的 fallback default (`default_acquisition_mode=full_company_roster`)，导致前端优先显示旧的 `execution_strategy_label=全量 live roster`
- 修复方式：
  - `compile_execution_semantics(...)` 现在读取 `request.execution_preferences.acquisition_strategy_override`
  - 显式 override（如 `scoped_search_roster`）会覆盖 profile default，生成 `effective_acquisition_mode=scoped_live_search` / `execution_strategy_label=定向搜索 roster`
  - payload 同时保留 `profile_default_acquisition_mode` 与 `request_acquisition_strategy_override`，方便审计“组织默认策略”和“本次 review/request 覆盖策略”的差异
- 真实 PostHog 复现：
  - request `帮我找PostHog做Coding方向的人 检索策略使用scoped search`
  - 修复后 `/api/plan` 返回 `strategy_type=scoped_search_roster`、`profile_default_acquisition_mode=full_company_roster`、`request_acquisition_strategy_override=scoped_search_roster`、`effective_acquisition_mode=scoped_live_search`、`execution_strategy_label=定向搜索 roster`
- 验证：
  - 新增 `tests/test_pipeline.py::PipelineTest::test_plan_workflow_scoped_search_override_updates_effective_execution_semantics`，覆盖 `/api/plan` 同层 plan envelope：profile default 为 `full_company_roster` 时，本次 request override 仍必须输出 `定向搜索 roster`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/execution_semantics.py tests/test_execution_semantics.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_execution_semantics.py` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'plan_workflow_scoped_search_override_updates_effective_execution_semantics or compile_plan_review_instruction or build_sourcing_plan_omits_public_web_stage_by_default or current_job_snapshot_over_stale_result_view'` -> `5 passed`
  - `npm --prefix frontend-demo run build` -> passed

### LinkedIn Stage 1 no-DataForSEO seed fallback by default

- Product boundary clarified from live OpenAI Infra job `185af9c5b847`: `LinkedIn Stage 1` must only use LinkedIn-related providers by default (`company-employees`, `linkedin-profile-search`, `linkedin-profile-scraper`). DataForSEO/public-web seed discovery is not a fallback inside Stage 1; it is Stage 2 or explicit opt-in only.
- Fixes:
  - `compile_search_strategy(...)` no longer emits `relationship_web`, `publication_surface`, or `public_interviews` Stage-1 seed bundles unless `execution_preferences.allow_stage1_web_seed_fallback=true` (alias `allow_public_web_seed_fallback`).
  - model-assisted search plans are filtered through the same policy, so an LLM cannot silently reintroduce public-web seed bundles into default Stage 1.
  - `intent_axes.execution_preferences` now supports the same opt-in key, so model-normalized requests do not silently drop an explicit Stage-1 web seed override.
  - `SearchSeedAcquirer.discover(...)` suppresses non-paid web search specs unless `cost_policy/intent_view` explicitly allows Stage-1 web seed fallback. Old `search_seed_queries` still feed Harvest profile-search fallback/primary logic; they no longer imply DataForSEO.
  - `execution_preferences` now recognizes `allow_stage1_web_seed_fallback`; default cost policy records `prefer_low_cost_web_search=false` and `allow_stage1_web_seed_fallback=false`.
  - Historical zero-delta `search_seed_discovery` workers (e.g. DataForSEO returned pages but no LinkedIn seed entries) are no-op reconciled: write final marker with `sync_reason=search_seed_no_candidate_delta`, skip profile prefetch, skip full materialize.
  - Candidate serving API/frontend contract now exposes `source_matches`/`sourceMatches` from materialized artifacts, so one candidate can be consumed as belonging to multiple scoped query/source shards instead of only flattening to `matched_keywords`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/search_planning.py src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition_strategy.py src/sourcing_agent/execution_preferences.py tests/test_seed_discovery.py tests/test_pipeline.py tests/test_planning_modules.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_seed_discovery.py tests/test_planning_modules.py` -> `104 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'search_seed_no_candidate_delta or build_sourcing_plan_omits_public_web_stage_by_default or current_job_snapshot_over_stale_result_view or release_stale_workflow_job_lease or scoped_search or former_search_seed'` -> `21 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py` -> `327 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py tests/test_remote_provider_events.py tests/test_runtime_lease_utils.py tests/test_workflow_event_response.py` -> `25 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_cli.py -k 'audit_company_serving_view or repoint_job_result_view'` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'asset_population_seed_query_keyword or legacy_sqlite_candidate_source or result_view_without_summary_candidate_source or dashboard_is_summary_only or auto_materialize_snapshot_candidate_documents'` -> `4 passed`
  - `npm --prefix frontend-demo run build` -> passed
  - Runtime smoke: `POST /api/workflows/explain` for OpenAI Infra only emits `targeted_people_search/linkedin_people_search/paid_fallback`; `GET /api/jobs/185af9c5b847/candidates` returns `source_matches` for OpenAI serving candidates.

### Event-level workflow review pass 5 — H4 query dedupe + H5 source_matches recall + provider-slot idle metric

- All three follow-ups Codex queued after pass 4 closed in this entry. No live Harvest/Apify calls.
- **H4 — Harvest query dedupe normalization** (`src/sourcing_agent/seed_discovery.py:_normalize_harvest_query_text`).
  - Symptom: hyphen / underscore / case variants of the same scope keyword (`Reasoning-Model`, `Reasoning_Model`, `reasoning model`) deduped at the *signature* level (`_search_query_signature` already collapses `[\s\-_]+` and lowercases) but the *provider-facing* query string was the raw variant. Result: even though only one provider call ran, summaries and per-variant cache keys diverged, and any second variant that happened to slip through dedupe (e.g. via different upstream filtering) would have hit Harvest with a needlessly different query.
  - Fix: extracted `_canonicalize_provider_query_alias` (mirrors the alias resolution `_clean_provider_query_text` already applies on the keyword path: `_PROVIDER_QUERY_CANONICAL_ALIASES` → `thematic_signal_search_query_aliases` → `canonicalize_thematic_signal_label` → `canonicalize_scope_signal_label`). `_normalize_harvest_query_text` now routes its three return paths through it. Hyphen/underscore variants collapse to the space-separated form before alias lookup so `Reasoning-Model` and `Reasoning_Model` both map to canonical `Reasoning`.
  - Tests (`tests/test_seed_discovery.py`):
    - `test_provider_query_signature_collapses_hyphen_underscore_case_variants` — the dedupe-key invariant.
    - `test_provider_query_family_key_aliases_canonicalize_synonyms` — alias-mappable variants share family keys.
    - `test_resolve_provider_people_search_queries_dedupes_alias_and_case_variants` — end-to-end: alias-equivalent inputs produce one provider query.
    - `test_resolve_provider_people_search_queries_dedupes_hyphen_underscore_variants` — non-aliased hyphen/underscore variants still collapse.
    - `test_normalize_harvest_query_text_is_alias_canonical` — provider-facing text uses the canonical form.
- **H5 — recall consumes `source_matches` / `matched_keywords`** (`src/sourcing_agent/scoring.py`).
  - Symptom: scoped sharding records "this candidate came from the `Reasoning` profile-search shard" in `metadata.matched_keywords` and `metadata.source_matches`. But `_candidate_blob` (used by `candidate_matches_structured_filters` and `_score_keyword_pool`) only included `candidate_searchable_text` + facets + role bucket. A candidate whose raw text fields didn't mention `Reasoning` was silently hidden by the recall filter even though the workflow had already paid Harvest to fetch them for that exact keyword.
  - Fix: added `_candidate_source_match_keyword_text(candidate)` extracting the dedupe'd text from `metadata.matched_keywords` + `metadata.source_matches[*].matched_on`. `_candidate_blob` now appends this signal so `must_have_keywords` filtering sees it. Added a new `source_match_keywords` field weight (3) to `SEARCH_FIELDS` so scoring also matches on the provenance keywords; `_candidate_field_value` routes it through the new helper.
  - Tests (`tests/test_scoring.py::ScoringSourceMatchProvenanceTests`):
    - `test_candidate_matches_structured_filters_honors_matched_keywords_metadata` — must_have gate accepts a candidate matched only via source_matches.
    - `test_score_candidates_returns_candidate_when_only_source_matches_carry_keyword` — recall returns the candidate; matched_fields explanation surfaces the provenance keyword.
    - `test_score_candidates_excludes_candidate_when_neither_text_nor_source_matches_match` — negative control to keep H5 from over-recalling.
- **Provider-slot idle metric** (`src/sourcing_agent/enrichment.py`, `src/sourcing_agent/workflow_efficiency.py`).
  - Symptom: a worker stuck in `waiting_remote_harvest` for 10+ minutes with no progress was the documented "actor slot idle" failure mode but only spottable via the Apify console.
  - Fix: enrichment stamps `remote_wait_started_at` on the worker checkpoint when transitioning into `waiting_remote_harvest`. `workflow_efficiency._extract_worker_efficiency_metrics` accepts an optional `now` and computes `remote_wait_age_ms` (per-batch p50/max/min/count) for workers currently in remote-wait, plus `provider_lease_age_ms` from `provider_limiter_lease.created_at` for cross-checking. Both metrics flow through `extract_event_level_efficiency_metrics`, `aggregate_event_level_efficiency_metrics`, and the runtime subset. Diagnostic only — never gates submit / materialize.
  - Tests (`tests/test_workflow_efficiency.py`):
    - `test_event_level_efficiency_surfaces_provider_slot_idle_window` — worker stuck 12 min vs 30 s; aggregated min/max distinguish them.
    - `test_aggregate_event_level_efficiency_metrics_includes_provider_slot_idle` — multi-report aggregation preserves min/max boundaries.
- Validation:
  - `pytest tests/test_pipeline.py tests/test_workflow_efficiency.py tests/test_seed_discovery.py tests/test_scoring.py tests/test_remote_provider_events.py tests/test_runtime_lease_utils.py tests/test_workflow_event_response.py -q` → **405 passed**.
  - `pytest tests/test_enrichment.py -q` → 44 passed (sanity for the `remote_wait_started_at` addition).
- With pass 1–5 and the two pass-4 fixes, every documented backlog item from Codex's reviews is closed in code or carried forward as a post-manual-test follow-up. Pipeline is fully green; ready to start a local backend/frontend manual test pass when desired.

### Event-level workflow review pass 4 — closed two pre-existing pipeline failures

- Pass 3 had left two pipeline tests failing as "pre-existing working-tree state, out of scope". Codex pushed back: those are not noise, they're real semantic regressions that should clear before any live/manual test. Both fixed in this entry; full pipeline now 324/324 with no deselects.
- 1 (exploration vs outreach reconcile priority) — `_reconcile_completed_workflow_if_needed` was hitting `_outreach_layering_requires_background_reconcile(..., allow_missing=True)` *before* checking for pending exploration workers, so a `full_company_asset` job with completed exploration output to apply silently fell through to the outreach-layering branch and returned `reconciled_outreach_layering`. Fix: compute `pending_exploration_workers` first; only call the outreach-layering gate when there are no pending exploration workers. This restores the original "exploration before outreach" priority while keeping the `allow_missing=True` semantics for jobs without exploration output. Test `test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` now passes (`status="reconciled"`).
- 2 (large-org full-company baseline reuse) — `_large_scoped_profile_queries_require_explicit_coverage` was firing for full-company queries on large orgs, flipping `requires_delta_acquisition=True` because the planner's embedded profile queries weren't explicitly covered by the baseline. But a full-company query (e.g. "给我Anthropic的全部成员") explicitly wants the *entire* roster — its embedded profile queries are scoped within the complete population, not a narrower scoped search, so reusing a complete authoritative baseline is the whole point. Fix: short-circuit the rule when `current_strategy_type == "full_company_roster"`. Scoped queries (`scoped_search_roster`, `former_employee_search`) and large-org default `scoped_search_roster` mode still require explicit coverage as before. Test `test_large_org_full_company_query_reuses_complete_baseline_without_former_delta` now passes (`requires_delta_acquisition=False`, `planner_mode="reuse_snapshot_only"`).
- Validation:
  - `pytest tests/test_pipeline.py -q` (no deselects) → **324 passed**.
  - `pytest tests/test_workflow_efficiency.py tests/test_remote_provider_events.py tests/test_runtime_lease_utils.py tests/test_workflow_event_response.py -q` → 23 passed.
  - `pytest tests/test_pipeline.py -q -k "asset_reuse or large_org or full_company or scoped_search or baseline or delta or former"` → 57 passed (sanity for the asset-reuse rule change).
- Pipeline is fully green; H4 query dedupe, H5 source_matches recall regression, and provider-slot idle metrics are the next items in the queue. Live/manual test still gated on those reaching a known-good baseline.

### Event-level workflow review pass 3 — true marker recoverability + Phase B-failure short-circuit

- Codex review of pass 2 surfaced three semantic gaps in the marker contract. All three closed; the pass-2 entry below is left in place for history but its "Phase C 完成后才写 ingest" claim was over-stated for the completed-job paths and the running-job sync-failure tail.
- 1 (completed-job paths still wrote `inline_incremental_ingest` before sync): the pre-sync `_record_inline_incremental_ingest_on_workers_if_missing(..., sync_result={"status":"materializing"|"deferred", ...})` calls in `_reconcile_completed_workflow_after_company_roster`, `_reconcile_completed_workflow_after_search_seed`, and `_reconcile_completed_workflow_after_harvest_prefetch` were removed. The post-sync `overwrite_existing=True` write at the end of each function is now the only place a final ingest marker is written, and it only runs after sync returns `completed`/`skipped`/`deferred`.
- 2 (running-job prefetch failure still wrote final marker): added `_phase_b_prefetch_indicates_retryable_failure(profile_prefetch)` helper. When true, `_process_inline_incremental_worker_batch`, `_reconcile_completed_workflow_after_company_roster`, `_reconcile_completed_workflow_after_search_seed`, and `_reconcile_completed_workflow_after_harvest_prefetch` all short-circuit before Phase C: skip sync, skip the gating ingest marker, return `status="prefetch_failed_retryable"`. The worker keeps only its `inline_incremental_apply` marker, so the next recovery tick re-picks it. Also gated the running-job final per-worker `_record_inline_incremental_ingest_on_worker` loop on `sync_result.status in {completed, skipped, deferred}`; on sync failure (or any non-terminal-good status) the gating marker is not written and an explanatory event is appended.
- 3 (search_seed `apply_already_consumed` lost SearchSeedSnapshot): the synthesized apply_result in both the running-job branch (orchestrator.py:~19458) and the completed-job branch (orchestrator.py:~20705) now restores `search_seed_snapshot` via `_restore_search_seed_snapshot_from_snapshot_dir`, so retry prefetch in Phase B actually runs instead of silently skipping with `search_seed_snapshot_missing`.
- New / updated tests:
  - `test_company_roster_running_job_prefetch_failure_leaves_worker_repickable` rewritten — no manual marker clear. First tick: prefetch raises, sync NOT called, no ingest marker written. Second tick: collector re-picks, Phase A short-circuits on apply marker (apply not called twice), Phase B succeeds, sync runs, ingest marker finally written.
  - `test_completed_company_roster_reconcile_prefetch_failure_leaves_worker_repickable` (new): completed-job company_roster Phase B failure path — sync not called, no ingest marker, apply marker persists.
  - `test_completed_search_seed_reconcile_prefetch_failure_leaves_worker_repickable` (new): completed-job search_seed Phase B failure path — same invariants.
  - `test_search_seed_apply_already_consumed_restores_search_seed_snapshot` (new): exercises the apply-already-consumed branch and asserts the restored `SearchSeedSnapshot` is forwarded to `_queue_background_profile_prefetch_from_search_seed_snapshot`.
  - `test_completed_workflow_harvest_reconcile_marks_consumed_before_materialize_failure` rewritten to match the new contract — sync failure must NOT write `inline_incremental_ingest`; apply marker persists.
- Validation (no live providers):
  - `pytest tests/test_pipeline.py -q -k "writer_lock or completion_event_queues_next_batch or harvest_profile_completion or harvest_prefetch or company_roster or search_seed or completed_workflow or background_reconcile or background_exploration or out_of_order or scoped_search or inline_incremental"` (minus 2 pre-existing failures) → 77 passed.
  - Full pipeline minus the 2 pre-existing working-tree failures → 322 passed.
- H4 / H5 / provider-slot idle metrics and the two pre-existing pipeline failures remain explicitly out of scope until pass 3 is signed off.

### Event-level workflow review findings — C1+M2+H1 fixed, H3/M3/M5 deferred

- Claude Code event-level review identified two contract violations and one observability gap; option (b) implemented per user direction.
- C1 (critical): `_process_inline_incremental_worker_batch` for `company_roster` / `search_seed` ran the snapshot-delta apply, profile prefetch (next-submit opportunity), and downstream sync all inside one per-job writer lock (`_inline_incremental_writer_lock_for_job`). A peer remote completion hitting the same job had to wait for the full materialize/sync. The `harvest_prefetch` path already had a pre-writer prefetch hop and was unaffected.
- M2 (medium): `_queue_background_profile_prefetch_from_search_seed_snapshot` and the `company_roster` inline reconcile callsite both inherited the default `load_cached_profile_payloads=True`, so next-submit gating for those worker kinds parsed raw Harvest profile JSON instead of using registry-only markers.
- H1 (high): no metric exposed how long an inline reconcile waited on the per-job writer lock, so writer-lock contention (a documented top failure mode) was invisible to live smoke / runtime dashboards.
- Implementation:
  - new `_inline_incremental_writer_lock_scope(job_id, scope=...)` context manager records `lock_requested_at` / `lock_acquired_at` / `lock_released_at` plus `writer_lock_wait_ms` / `writer_lock_held_ms`. Diagnostic only — never gates submit/materialize.
  - `_process_inline_incremental_worker_batch` split into three phases:
    - Phase A under per-job writer lock: collect completed workers, completed-job branch, snapshot-delta `_apply_background_*_workers_to_snapshot`, provisional `inline_incremental_ingest` marker so a peer reconcile tick will skip these workers via `_worker_has_inline_incremental_ingest_output`.
    - Phase B outside the lock: profile prefetch (next-submit opportunity). For `company_roster` / `search_seed` callers pass `load_cached_profile_payloads=False`. Failures here only leave the provisional marker; subsequent recovery resubmits prefetch because the workers have an applied delta but no queued/dispatched marker yet.
    - Phase C outside the lock: `_inline_incremental_sync_for_running_job` (full materialize), per-worker final marker, persisted reconcile state, and per-kind structured events. Concurrent same-snapshot syncs continue to serialize through the global `materialization_writer` writer-budget slot.
  - `_queue_background_profile_prefetch_from_search_seed_snapshot` now forwards `load_cached_profile_payloads=False` to the underlying baselines helper.
  - `writer_lock` block embedded in `company_roster` / `search_seed` inline reconcile event payloads + the harvest_prefetch reconcile event; legacy harvest_prefetch pre-writer prefetch hop preserved.
  - `workflow_efficiency.py` adds `_extract_writer_lock_wait_metrics` (per-event aggregation with by-scope breakdown), surfaces it in `extract_event_level_efficiency_metrics`, `aggregate_event_level_efficiency_metrics`, and `event_level_efficiency_runtime_subset`.
- Tests added (`tests/test_pipeline.py`, `tests/test_workflow_efficiency.py`):
  - `test_company_roster_inline_reconcile_runs_prefetch_outside_writer_lock` — apply→prefetch→sync ordering and `load_cached_profile_payloads=False`.
  - `test_company_roster_inline_reconcile_prefetch_does_not_block_peer_remote_completion` — peer can grab the per-job writer lock during the prefetch fire (non-blocking acquire).
  - `test_search_seed_inline_reconcile_runs_prefetch_outside_writer_lock` — same invariants for the search_seed path; explicitly exercises the wrapper helper to assert `load_cached_profile_payloads=False` propagates to the underlying baselines call.
  - `test_inline_writer_lock_emits_writer_lock_wait_metric_event` — holds the lock externally for ~50ms, asserts `writer_lock_wait_ms ≥ 40` in the inline reconcile event payload and that `extract_event_level_efficiency_metrics` rolls it up.
  - `test_event_level_efficiency_aggregates_writer_lock_wait_metric` and `test_aggregate_event_level_efficiency_metrics_includes_writer_lock` — verify per-event extraction, by-scope breakdown, and aggregation across reports.
  - existing `test_harvest_profile_completion_event_queues_next_batch_before_apply_and_defers_materialization` and `test_harvest_profile_completion_prefetch_runs_before_writer_lock` continue to pass; the harvest_prefetch pre-writer prefetch hop is intentionally left intact.
- H3 re-verified, no code change: in `enrichment.py:_execute_harvest_profile_batch_worker`, `acquire_runtime_provider_limiter_slot` is called at line 2139 *before* `begin_worker` at line 2178; on limiter `RuntimeError` the function returns `worker_status="backpressure"` without creating a worker; on `begin_worker` failure the lease is released. The post-`begin_worker` checkpoint immediately moves the worker into `submitting_remote_harvest` with the lease attached. No phantom-without-lease window.
- M3 (foreground_fast stat) and M5 (corrupt-manifest fail-loud) already mitigated for the active code paths; left in NEXT_TODO for future audit / regression coverage.
- H5 (recall/filter prefers `source_matches`/`matched_keywords`) deferred — needs a recall provenance regression first before changing API/frontend behavior.
- Targeted validation (read-only / no provider calls):
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py -q` → 5 passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "inline_incremental or scoped_search or out_of_order or harvest_profile_completion or harvest_prefetch or completed_workflow_harvest or company_roster_inline or search_seed_inline or writer_lock"` → 23 passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q --deselect ...test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` → 316 passed, 1 pre-existing failure deselected
  - Full pipeline run had 2 pre-existing failures unrelated to this change set:
    - `test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` — caused by the working tree's `_outreach_layering_requires_background_reconcile(..., allow_missing=True)` change at orchestrator.py:18055; reverting that single keyword-arg makes the test pass without my edits.
    - `test_large_org_full_company_query_reuses_complete_baseline_without_former_delta` — the working tree's 349-line diff in `asset_reuse_planning.py` flips `requires_delta_acquisition` to True; not touched by this change set.
  - Both pre-existing failures noted in NEXT_TODO so they are not lost.

### Event-level workflow review pass 2 — closed completed-job lock + marker recoverability

- Codex review of pass 1 surfaced five must-fix items before claiming completion. All addressed in this entry; running-job + completed-job paths now share the same Phase A/B/C contract.
- 1 (completed-job lock contention): `_process_inline_incremental_worker_batch`'s completed-job branch was still dispatching `_reconcile_completed_workflow_after_company_roster` / `_reconcile_completed_workflow_after_search_seed` / `_reconcile_completed_workflow_after_harvest_prefetch` *inside* the per-job writer lock, and each of those functions did apply + prefetch + materialize + retrieval + outreach_layering as one block. Refactor: dispatch happens outside the outer lock; each reconcile sub-function takes its own short `_inline_incremental_writer_lock_scope` around just the apply call.
- 2 (load_cached_profile_payloads): completed company_roster prefetch (`_reconcile_completed_workflow_after_company_roster`) now passes `load_cached_profile_payloads=False` so it stays on the registry-only marker path.
- 3 (provisional marker recoverability): the running-job and completed-job paths previously wrote `inline_incremental_ingest` provisionally under the lock, which `_collect_inline_incremental_worker_batch` treats as terminal — so a Phase B prefetch failure permanently skipped the worker. Split into two markers:
  - `inline_incremental_apply` (new): written under Phase A lock immediately after `_apply_background_*_workers_to_snapshot` returns `applied`. Idempotency guard for Phase A re-runs (filter `pending_workers` to those without an apply marker for this snapshot before calling apply). Does NOT block collection.
  - `inline_incremental_ingest` (existing): written only at Phase C completion. Continues to be the gating signal for `_collect_inline_incremental_worker_batch`.
  - Net effect: Phase B/C failures leave the apply marker behind; the worker is re-pickable; the next recovery tick skips Phase A apply and retries Phase B prefetch + Phase C materialize.
- 4 (`workflow_efficiency.report_available`): now includes `writer_lock_event_count`, so a job whose only telemetry is writer-lock metrics is no longer dropped by `aggregate_event_level_efficiency_metrics`.
- 5 (docs): orphan bullets at PROGRESS.md:44 restored under their heading; NEXT_TODO downgraded the C1/M2 over-claimed status from "fully done" to "running-job done in pass 1, completed-job closed in pass 2".
- New tests:
  - `test_completed_company_roster_reconcile_runs_prefetch_outside_writer_lock` — completed-job path: apply→prefetch→sync ordering, peer can grab the per-job lock during prefetch and during sync, prefetch is registry-only.
  - `test_completed_search_seed_reconcile_runs_prefetch_outside_writer_lock` — same invariants for the search_seed completed-job path.
  - `test_company_roster_running_job_prefetch_failure_leaves_worker_repickable` — Phase B prefetch raises; on next recovery tick the apply marker short-circuits Phase A and apply is not called again.
  - `test_event_level_efficiency_aggregates_writer_lock_wait_metric` and `test_aggregate_event_level_efficiency_metrics_includes_writer_lock` already verified per-event extraction and aggregation in pass 1; no change needed for the `report_available` fix to be exercised, but the broader suite now also runs writer-lock-only payloads through `report_available`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py -q` → 5 passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "writer_lock or completion_event_queues_next_batch or harvest_profile_completion_prefetch or company_roster_inline_reconcile or search_seed_inline_reconcile or completed_workflow_harvest_reconcile or completed_company_roster_reconcile or completed_search_seed_reconcile or company_roster_running_job_prefetch_failure"` → 17 passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "inline_incremental or scoped_search or out_of_order or harvest_profile_completion or harvest_prefetch or completed_workflow or company_roster or search_seed or writer_lock or background_reconcile or background_exploration"` (minus the one pre-existing failure) → 69 passed.
  - Full pipeline suite minus the two pre-existing working-tree failures (`test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` and `test_large_org_full_company_query_reuses_complete_baseline_without_former_delta`) → 319 passed.
- No live Harvest/Apify calls.

### Event-level workflow review handoff documented

- 已把流式响应级 workflow 的预期和工程边界补充到 `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`：
  - 产品预期：provider completion 后先 local apply / progress / next-submit，full materialization 异步追平
  - 业务流程：scoped search current/former lane、profile-search、profile registry、profile-scraper、candidate board serving 的端到端状态流
  - 不变量：真实 provider slot lease、`inline_incremental_ingest` marker、next-submit 不等 writer lock、completed reconcile job-level lease、registry-only marker path、zero-result retry vs degraded page coverage
  - 已踩坑：baseline 误覆盖 scoped lane、duplicate normalized Harvest query、phantom active worker、writer lock 空窗、重复 materialize、hot-cache/result-view 漂移、大 snapshot 本地热路径
  - 下一轮 review checklist：按模块审查 orchestrator/enrichment/seed_discovery/workflow_efficiency/candidate_artifacts/api 是否仍有人工测试才会暴露的服务级漏洞
- 已新增 `docs/CLAUDE_CODE_EVENT_WORKFLOW_REVIEW_PROMPT.md`，用于启动 Claude Code 新 session 做 workflow design review；`docs/INDEX.md` 已加入入口。
- `docs/NEXT_TODO.md` 已新增 `Event-level workflow design review handoff`，要求 Claude review 后把 findings、修复和验证及时同步回 `PROGRESS.md` / `NEXT_TODO.md`。
- 这次只更新文档与 handoff prompt，不触发真实 provider 调用。

### Harvest profile-search zero-result retry and serving-view CLI formalized

- 根据 Harvest/API 服务商回复，`linkedin-profile-search` 同一 input 偶发 `0 profiles` 是已知 LinkedIn/provider transient issue；本地 contract 已调整为主要针对“真 0 结果”做可恢复重试，而不是把常见的总量漂移/缺页都视为强 incomplete。
- 已实现 provider search 语义调整：
  - `HarvestProfileSearchConnector.search_profiles(...)` 支持 `zero_result_retry_attempts` / `zero_result_retry_backoff_seconds`
  - live 模式下如果 raw/shared cache 中是 `rows=[]` 且 `pagination.total_elements<=0`，会忽略该 0 结果 cache 并重新请求
  - probe、scale、page chunk、single-page fallback 都透传 zero-result retry 设置
  - 只有重试耗尽后仍是真 0 结果，才写 `status=incomplete` / `incomplete_reason=provider_zero_results_after_retry`
  - provider total 在不同 run 间变化、或 chunk/page coverage 小于 probe total，默认记录 `status=degraded` / `provider_search_degraded=true`，用于审计和后续补跑，不再作为本地开发流程的强阻断条件
- 已补正式 serving-view 运维入口：
  - `build-company-candidate-artifacts` 增加 `--build-profile` 和 repeatable `--preferred-source-snapshot-id`
  - 新增 `rebuild-company-serving-view`，作为 scoped/source provenance projection repair 的显式 rebuild CLI
  - 新增 `audit-company-serving-view`，只读输出 registry、manifest/pages、`source_matches/matched_keywords` 采样、projection version、build profile、job result view drift
  - 新增 `repoint-job-result-view`，默认 dry-run；只有 `--policy serve_latest_company_asset --apply` 才会改变旧 job 指针，`historical_replay` 明确 no-op
- artifact summary/manifest 现在写入：
  - `build_profile`
  - `projection_version`
  - 现有 `timings_ms` / `source_snapshot_selection`
- 真实 Google/Gemini artifact 只读审计已通过：
  - `audit-company-serving-view --company Google --snapshot-id 20260428T011339 --job-id 92eb11472da4 --sample-pages 1`
  - `status=ok`、`candidate_count=9359`、`page_count=188`
  - 旧 artifact summary/manifest 尚无新顶层 `build_profile`，但 audit 可从 candidate shard sample 推断 `projection_version=candidate_artifact_projection_v20260427_source_matches`
  - sample page 读到 `source_matches_records=54`、`matched_keywords_records=54`，job result view drift 为 false
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py src/sourcing_agent/seed_discovery.py src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/cli.py tests/test_harvest_connectors.py tests/test_seed_discovery.py tests/test_candidate_artifacts.py tests/test_cli.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py tests/test_seed_discovery.py` -> `121 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py -k 'records_phase_timings'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_cli.py -k 'audit_company_serving_view or repoint_job_result_view'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'blocks_incomplete_provider_probe_fallback or scoped_search_seed_pool_starts_former_search_lane_in_parallel or default_former_search_seed_preserves_scoped_keywords'` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`

### Company canonical serving view boundary clarified

- 复盘 scoped artifact rebuild 时明确数据边界：
  - `canonical_merged` 是某个 snapshot 下的 artifact view，不是自动吞掉所有历史 snapshot 的 company-wide evergreen index
  - `source_snapshot_selection` 决定一个 artifact view 合并哪些 source snapshots；无边界 all-history union 只能作为 legacy / repair fallback，不能作为 scoped provenance rebuild 默认策略
  - 一人多 sharding 必须通过同一候选人的 `source_matches` / `matched_keywords` 表达，而不是把多个历史 snapshot 的候选人无差别塞进旧 job snapshot
  - `job_result_view` 是 job serving 指针；新 scoped workflow 完成后应指向本次 current snapshot。是否把旧 job repoint 到更新的 company canonical view，需要显式区分 historical replay 与 serve latest company asset
- 已记录方法论：
  - `docs/DATA_ASSET_GOVERNANCE.md` 新增 `Snapshot Selection Is Not Shard Membership`
  - `docs/NEXT_TODO.md` 新增 company canonical serving view / job result view 边界与后续 backlog
- 真实状态修正：
  - 停止了误按旧 all-history union 继续重建 OpenAI snapshots 的批处理
  - `OpenAI 20260422T161258` 已恢复为该 job snapshot 边界的 `352` 人 artifact，避免误写成全历史 `826` 人
  - Anthropic / xAI 这类先有 full authoritative asset、后续 query 复用本地资产的记录不纳入 scoped provenance rebuild；旧 artifact 可继续走文本 fallback，除非明确要发布新版 company canonical serving projection

### Google/Gemini scoped serving artifact rebuild completed

- 收口 query/job `92eb11472da4`（`帮我找Google在Gemini组的人`）的本地 serving 状态：
  - `runtime/company_assets/google/20260428T011339/normalized_artifacts/manifest.json` 已生成，`candidate_count=9359`、`page_count=188`
  - `source_snapshot_selection.mode=current_snapshot_only_large_org`，只选 `20260428T011339`，明确排除旧 Google historical snapshots，避免 848MB 级历史 baseline 预读和历史污染
  - page artifacts 只读复核：`page_rows=9359`、`source_matches_records=3940`、`matched_keywords_records=3940`、`gemini_source_hits=43`
  - PG `job_result_view` 和 job progress latest metrics 已 repoint 到 `snapshot_id=20260428T011339`、`view_kind=asset_population`、`source_kind=company_snapshot`
  - completed event 已写入：`Scoped serving artifacts rebuilt and job result view repointed to Google 20260428T011339.`
- 明确保留 provider incomplete 边界：
  - Harvest profile-search 对 current/former Gemini 的 scale/page chunk 仍存在 provider-empty page range；不把这类 search-seed 视为完整覆盖
  - 相关 search worker 被按 `provider_search_incomplete_retry_later` 处理，不再触发额外 provider 调用；后续同 query 可重新补 current lane / former lane 的缺页
  - 本次 materialization 表示“当前已落地 snapshot 的 serving artifact 完整”，不表示 Harvest provider 已返回 Gemini 全量理论结果
- 手动 rebuild 过程暴露并修复的通用性能边界：
  - `background_snapshot_materialization_reconcile` 现在和 pre-retrieval / background-harvest refresh 一样使用 `foreground_fast` artifact profile
  - `materialize_company_candidate_view(...)` 会先加载当前 snapshot；当前 snapshot 已达大组织阈值或存在显式 preferred snapshot set 时，不再预读全历史 candidate documents
  - `foreground_fast` artifact projection 优先使用 candidate metadata 中已经嵌入的 profile timeline / signals，不为每个候选人先解析 raw profile source path
  - `foreground_fast` fingerprint 不再对 candidate/evidence/profile raw source paths 做 `stat()`，只保留路径字符串进入 fingerprint，避免大 snapshot 重投影时的无收益文件系统探测
  - Google 真实 artifact timing 复核：`prepare_candidates≈54.8s`、`view_write_total≈67.4s`；剩余主要是 9k 人文本/facet projection CPU 成本，后续可继续做增量和缓存优化
- 已跑或已保留的验证信号：
  - `tests/test_pipeline.py::test_snapshot_materializer_background_snapshot_reconcile_uses_foreground_fast_artifact_profile`
  - `tests/test_candidate_artifacts.py::test_materialized_view_large_org_does_not_preload_all_history`
  - `tests/test_candidate_artifacts.py::test_candidate_materialization_fingerprint_can_skip_source_path_stat`
  - 真实 artifact 只读检查确认 manifest/pages/source provenance 与 job result view 指针一致

### Local manual-test startup contract tightened

- 复盘本地手测启动时发现的脚本契约问题：
  - 旧 `scripts/dev_backend.sh --no-daemon` 只是不启动外置 worker daemon，但 `serve` 进程仍会启动 server-side runtime watchdog
  - 这会在“只想打开前端手动看页面”时抢跑 `background_snapshot_materialization.status=scheduled` 的 backlog，和用户预期不一致
- 已修复启动语义：
  - `--no-daemon` 现在默认同时传 `--disable-runtime-watchdog`
  - 如需只关闭外置 daemon 但保留 serve 内置 watchdog，必须显式传 `--enable-runtime-watchdog`
  - 当前只读进程检查显示后端、worker daemon、前端均未运行；只剩 `scripts/ecs_webhook_reverse_tunnel.py` 在运行

### Event-level profile apply/materialize separation hardening

- 复盘 Google/Gemini job `92eb11472da4` 暴露的工程审查缺口：
  - 之前验证了“单次 callback 内 next-submit 在 apply/materialize 之前调用”，但没有验证“下一次 remote completion 到达时，即使 job writer lock 被长时间占用，也能先触发 next-submit”
  - live 证据显示第三波 profile actor 在 `19:22:39-19:22:43` 已完成，但上一轮 callback 仍在 writer lock 内做 apply/delta-sync，第四波补位直到 `19:26:19` 才发生
  - 这是本地工作流状态机和测试标准问题，不是 Apify webhook/provider 问题
- 已修复状态拆分：
  - Harvest profile completion 的 post-ingest next-submit 保持在 writer lock 之前执行
  - completed workflow reconcile 先回填旧 `inline_incremental_ingest` marker，再处理新的 pending worker，避免旧 consumed worker 因 sibling pending 被跳过 backfill
  - provider dataset apply 成功后立即写 `inline_incremental_ingest`；full materialize/retrieval/layering 作为 `background_snapshot_materialization` 的 scheduled downstream 阶段单独恢复
  - 如果 materialize 进程被杀，worker 不会再被视为未消费；后续 recovery 只需继续 scheduled materialization
  - terminal workflow 的 completed-reconcile lease 如果持有者明确是同机已死亡 PID，会自动 safe-release 并重试 reconcile；远端或不可判定 owner 仍不释放
- 已修复 local apply 热点：
  - `apply_harvest_profile_workers_to_snapshot(...)` 以前在 LinkedIn URL 没命中候选人时退化为每个 profile 扫描全量 candidates 做姓名匹配
  - 现在新增 name-index 预筛选；大 snapshot 不再做全表 fallback，避免 9k candidates x profile batch 的 CPU 热点拖慢 marker 写入
- 真实 Google/Gemini 状态已修到安全边界：
  - `17/17` 个 Harvest profile worker 已有 `inline_incremental_ingest` marker
  - `13` 个旧 worker marker 为 `deferred`，最后 `4` 个为 `materializing`
  - 后续已通过独立 scoped artifact builder 收完 `background_snapshot_materialization` 的 serving artifact，并 repoint job result view；未新发 Harvest provider run
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/snapshot_materializer.py tests/test_pipeline.py tests/test_candidate_artifacts.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_harvest_reconcile_marks_consumed_before_materialize_failure or completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent or completed_workflow_reconcile_backfills_consumed_marker_without_rebuild or harvest_profile_completion_prefetch_runs_before_writer_lock or scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize or completed_workflow_reconcile_releases_dead_terminal_lease'` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py -k 'harvest_profile_apply_uses_name_index_instead_of_large_full_scan or build_company_candidate_artifacts_materializes_backlog_and_reusable_docs'` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py tests/test_workflow_event_response.py tests/test_runtime_lease_utils.py` -> `13 passed`

### Google/Gemini Harvest profile-search provider anomaly guard (superseded by zero-result retry contract)

- 排查 query/job `92eb11472da4`（`帮我找Google在Gemini组的人`）时确认一个 provider 级边界：
  - Harvest `linkedin-profile-search` 对 `Gemini + currentCompanies=Google` probe 返回 `736` total，对 `Gemini + pastCompanies=Google` probe 返回 `991` total
  - 同一参数放大到 scale 请求时 provider 多次返回空数组；进一步按 `startPage/takePages` 分块和单页 retry 后，也仍有大量页段空返回
  - 这不是本地 current/former lane 生成缺失；current/former 请求都已发出，问题是 provider 对部分 page range 返回不完整结果
- 当时先停止 live repair，避免继续消耗 Harvest API；已给 job 写入 provider anomaly event：
  - `provider_issue=harvest_profile_search_empty_page_ranges_after_probe_total`
  - `policy=mark_incomplete_do_not_treat_as_completed`
- 当时的状态机防回退已保留，但当前最新语义已收窄：
  - `HarvestProfileSearchConnector.search_profiles(...)` 支持 `start_page`
  - SearchSeed 的 scale-empty fallback 会按 page chunk 补拉；chunk 空时降级到单页 retry
  - 最新 contract 只把重试耗尽后的真 0 结果标记为 `provider_search_incomplete`
  - provider total 漂移、返回数小于 probe total/effective limit、或局部 page range 空返回，默认标记 `status=degraded` 用于审计/后续补跑，不再作为强 blocking 条件
  - former scoped-search lane 默认保留原 scoped keyword（例如 `Gemini`），不再退化成 broad past-company-only pass
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition.py tests/test_seed_discovery.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_seed_discovery.py -k 'page_chunks_when_scaled_harvest_result_is_empty or retries_empty_page_chunk_as_single_pages or probe_fallback_without_chunks_is_marked_incomplete'` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'blocks_incomplete_provider_probe_fallback or default_former_search_seed_preserves_scoped_keywords or scoped_search_seed_pool_starts_former_search_lane_in_parallel'` -> `3 passed`

### Scoped-search streaming E2E hardening

- 补齐了 scripted E2E，覆盖真实 Meta scoped-search + baseline reuse 的乱序完成形态：
  - job 先以 baseline snapshot 完成并服务旧结果
  - 两个 search-seed shard 乱序完成，先完成的 shard 会先 apply 并触发 profile prefetch opportunity
  - 同一 job 仍有 search-seed sibling worker 未 drain 时，completed-job reconcile 不再进入 full candidate materialize
  - 两个 Harvest profile batch 乱序完成，先完成的 profile batch 先 local apply / 写 consumption marker / 触发 next-submit opportunity
  - 同一 job 仍有 profile sibling worker 未 drain 时，full materialize 继续 defer
  - 最后一个 profile batch 完成后，candidate artifacts、job result view、candidate board、outreach layering 都收敛到同一个 current `snapshot_id`
  - 重复触发 search/profile completion batch processor 时，返回 `no_completed_unconsumed_inline_workers`，不重复消费 worker，也不重复 materialize
- 修复 completed-job search-seed reconcile 的调度缺口：
  - `_reconcile_completed_workflow_after_search_seed(...)` 现在复用 same-kind remaining-worker 判定
  - 有 pending search-seed sibling worker 时，只记录 `materialize_deferred` 结构化事件并写 consumption marker，不调用 `_synchronize_snapshot_candidate_documents`
  - sibling search shard drain 后才执行 full candidate artifact sync / retrieval / layering，避免 search shard 每到一个就重建一次
- 调度审视结论：
  - profile prefetch worker 仍是 lease-first：拿不到真实 provider limiter slot 会返回 backpressure，不创建 active worker，不占 actor 口径
  - active profile worker 计数只把 `waiting_remote_harvest`、已有 remote `run_id/dataset_id`、或持有 provider limiter lease 的 worker 计入预算
  - remaining-worker/materialize defer 已覆盖本轮目标的 scoped-search + profile tail；company-roster completed-job 分支仍可继续做同样的泛化收口，已记入 TODO
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'reconcile_completed_workflow_after_background_search_seed or search_seed_worker_completion_prefetches_profiles_before_full_materialize or scripted_scoped_search_baseline_reuse_materializes_layers_and_serves_current_snapshot or scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize or completed_workflow_reconcile or completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent'` -> `7 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_runtime_tuning.py tests/test_harvest_connectors.py -k 'profile_prefetch or harvest_profile_batch or global_inflight_budget or mixed_success or code_22'` -> `20 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py` -> `3 passed`

## 2026-04-27 (Asia/Shanghai)

### Event-level efficiency metrics productization

- 把事件级 workflow 的验收口径从“最终 artifact 正确”继续推进到“状态机和效率也正确”：
  - 新增 `src/sourcing_agent/workflow_efficiency.py` 作为共享聚合器，只读取 job events / workers / summary，不读取 provider raw payload 或 candidate artifacts
  - 指标覆盖 `remote completion -> local event`、`remote completion -> local Harvest completion marker`、`local marker -> next-submit start`、post-ingest prefetch elapsed、重复 reconcile、materialize call count、真实 provider slot occupancy
  - provider slot 指标区分三种状态：持有 limiter lease 的真实 slot、已有 remote actor/run 的 worker、没有 limiter lease 却处于 active/remote-wait 口径的 phantom/pre-submit worker
  - 这些指标已接入 `get_runtime_metrics(...)`，并进入 hosted/scripted smoke 的 `provider_case_report.event_level_efficiency` 与 matrix 汇总
- 工程复盘：
  - 之前测试主要覆盖 eventual correctness，不能发现“结果最终正确但中间重复 full rebuild、next submit 被慢路径拖住、worker active 口径不真实”的问题
  - 现在新增负向断言：重复 reconcile/materialize 会被计数，phantom provider worker 会触发 violation，next-submit lag 与 remote-marker lag 会进入 smoke report
  - 当前指标是观测和回归入口，不作为 submit/materialize gate；后续若要设 SLO，应在 smoke/CI 层按环境 profile 判断，而不是让 runtime 自己阻塞业务流
- 已补回归：
  - `tests/test_workflow_efficiency.py` 覆盖 remote event lag、local completion marker lag、next-submit lag、provider slot occupancy、phantom worker、重复 reconcile/materialize
  - `tests/test_workflow_smoke.py` 覆盖 provider case report 与 smoke timing summary 暴露 event-level efficiency
  - `tests/test_pipeline.py::test_runtime_metrics_reports_refresh_and_reconcile_counters` 覆盖 runtime metrics 暴露 event-level efficiency rollup
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/orchestrator.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -k 'event_level_efficiency or materialization_streaming_rollup or exposes_provider_roster_profile'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'runtime_metrics_reports_refresh_and_reconcile_counters'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or queue_background_profile_prefetch or remote_provider_event or remote_event_lane or event_level_efficiency or completed_workflow_reconcile'` -> `34 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `git diff --check -- src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/orchestrator.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py tests/test_pipeline.py PROGRESS.md docs/NEXT_TODO.md docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md docs/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md`

### Completed reconcile structured observability

- completed workflow reconcile 现在会写结构化 job events，而不是只靠 `detail` 文本排障：
  - `event_family=completed_workflow_reconcile`
  - `phase=lease_acquired|lease_skipped|started|marker_backfilled|materialize_started|materialize_completed|materialize_deferred|completed|failed`
  - `reconcile_kind=coordinator|company_roster|search_seed|harvest_prefetch|outreach_layering|exploration|snapshot_materialization`
  - 统一包含 `snapshot_id`、`worker_ids`、`worker_count`、`materialize_call`、`materialize_signature`、`marker_backfill_count`、`lease_acquired` / `skip_reason` 等机器可读字段
- `workflow_efficiency.py` 已改为结构化优先：
  - 只要 job events 中出现 `completed_workflow_reconcile`，metrics 就完全按结构化字段统计
  - 旧自然语言 `detail` 解析只作为历史 job fallback，避免新旧事件同时存在时重复计数
  - 新增计数包括 `lease_skipped_count`、`marker_backfill_count`、`materialize_started_count`、`materialize_completed_count`、`materialize_deferred_count` 和 `structured_event_count`
- 已补回归：
  - lease 被持有时必须落 `lease_skipped` 结构化事件
  - marker backfill 必须落 `marker_backfilled` 结构化事件，且不触发 rebuild
  - harvest completed reconcile 必须落 `materialize_started/materialize_completed`
  - metrics 必须优先读结构化事件，并忽略同 job 中残留的 legacy materialization 文本事件
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_efficiency.py tests/test_pipeline.py tests/test_workflow_efficiency.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_reconcile or completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent'` -> `3 passed`

### Meta Audio completed-reconcile state monotonicity hardening

- 复盘 query/job `d964b1d42ac5`（`帮我找Meta做Audio方向的人`）暴露的第二层效率问题：
  - provider/webhook 不是当前瓶颈；Stage 1 `806` candidates 的 profile registry 已经 fetched
  - snapshot `runtime/company_assets/meta/20260427T203601/` 已追平：`candidate_count=1727`、`profile_detail_count=1727`、`profile_completion_backlog_count=0`
  - 真正残余是 completed-job reconcile 的状态单调性：worker `394` 已被旧路径 materialize，但 worker output 没有 `inline_incremental_ingest` consumption marker
  - 这会让排障口径依赖 `background_reconcile.last_worker_updated_at` 间接判断，且多个 recovery 入口有机会重复进入 full materialize / outreach layering 慢路径
- 工程原因复盘：
  - 之前 scripted/regression 偏“最终一致性”：能恢复、能去重、能最终生成 artifact
  - 没有把效率与状态机不变量纳入硬断言：同一 provider completion 只消费一次、completed reconcile 必须有 job-level lease、worker 未拿到真实 provider slot 前不能占 actor budget、post-ingest next-submit 不能解析 raw payload/materialize
  - 因此事件级流式改造的部分失败模式表现为“结果最终对，但 tail latency 和重复重建不对”，旧测试不会失败
- 已修复：
  - `_reconcile_completed_workflow_if_needed(...)` 现在使用 store-backed job lease；同一个 completed workflow 的 background reconcile 不会被多个 recovery tick / 手动 recovery 并发执行
  - completed-job reconcile 直接消费 company-roster / search-seed / harvest-prefetch worker 后，会写统一 `inline_incremental_ingest` marker
  - 对旧状态增加轻量 marker backfill：如果 `background_reconcile.<kind>.worker_ids` 已证明 worker 被消费，但 worker output 缺 marker，只补 consumption marker，不重新 apply/materialize
  - 真实 Meta Audio 已用新代码跑 job-scoped recovery：worker `394` 补齐 `harvest_prefetch` marker，worker `347` 补齐 `search_seed` marker，未触发新的 provider run 或 full rebuild
- 新增回归：
  - completed Harvest worker 被 completed-job reconcile 消费后必须写 marker，第二次 reconcile 不能再次 apply/materialize
  - completed workflow reconcile 在 job lease 被持有时必须跳过，不能并发重建
  - 旧 `background_reconcile` 已记录消费但 worker 缺 marker 时，只做 marker backfill，不触发 rebuild
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent or completed_workflow_reconcile_skips_when_job_lease_is_held or completed_workflow_reconcile_backfills_consumed_marker_without_rebuild or harvest_profile_completion_callback_coalesces_completed_workers_before_prefetch'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or queue_background_profile_prefetch or harvest_completion_event or remote_event_lane or handle_remote_provider_event or completed_workflow_harvest_reconcile or completed_workflow_reconcile'` -> `29 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`

### Meta Multimodal scoped-search event-level response hardening

- 排查 query/job `2212a9f2e1c7`（`帮我找Meta做Multimodal方向的人`）里 `linkedin-profile-scraper` 看似只提交 4 个 actor 的问题：
  - 真实 Apify actor run list 显示该 job 的 profile scraper 实际有 `12` 个 `SUCCEEDED` run，时间覆盖 `2026-04-27T11:07:28Z` 到 `11:20:09Z`
  - 本地 `runtime/company_assets/meta/20260427T190455/harvest_profiles/` 也有 `12` 个 `harvest_profile_batch_*.queue_summary.json`
  - snapshot 已完整：`candidate_count=961`、`profile_detail_count=961`、`profile_completion_backlog_count=0`
  - 误导点是部分 cache-hit completion 路径把已有 `run_id/dataset_id` 写成空字符串，导致本地 summary / worker checkpoint 无法和 Apify UI/API 对账
- 已修复 profile batch remote identifier contract：
  - `HarvestProfileConnector.execute_batch_with_checkpoint(...)` 在 shared-cache hit 时会继承已有 checkpoint 的 `run_id/dataset_id`
  - enrichment writer 会从 `run_get/run_post/cache_hit` artifacts 兜底回填 remote identifiers，再写 queue summary 和 worker checkpoint
  - 已对真实 Meta job 回填：12 个 batch summary 和 12 个 worker checkpoint 现在全部有 `run_id/dataset_id`
- 已修复 completion event 后 next-submit 判定的同步慢点：
  - 真实 Meta `20260427T190455` 证明 webhook/event 已被记录，问题不是“没有事件唤醒”
  - 慢点之一是 `_handle_harvest_profile_completion_event(...)` 后续为了找下一批 URL 会从 full baseline 重新检查已 fetched profiles，并解析大量 raw Harvest payload
  - 新增 registry-only cache marker 路径：post-ingest prefetch opportunity 只用 PG `linkedin_profile_registry` 的 fetched/queued 状态判定是否需要 submit，不读取/解析已抓取 raw profile JSON
  - 完整 profile payload 解析仍留给下游 apply/materialize；next-submit path 不再被 materialization payload parsing 拖住
  - 真实 Meta 快路径复核：`471` 个 URL 全部 registry-cache 命中，`dispatched_url_count=0`，耗时约 `270ms`；旧路径本地直接调用超过 `25s` 仍未返回
  - 进一步补齐运行态观测：`remote_provider_event` payload 现在记录 `source`、`remote_completed_at`、`local_event_seen_at`、`remote_to_local_event_lag_ms`；Harvest completion event 记录 `post_ingest_prefetch_candidate_count`、`post_ingest_prefetch_dispatched_url_count`、`registry_cache_marker_count`、`post_ingest_prefetch_elapsed_ms`
  - registry-only 快路径复核更新：同一真实 Meta snapshot `471` 个 URL 全部 registry-cache marker 命中，`dispatched_url_count=0`，本地外层调用约 `126ms`，prefetch 自身约 `117ms`
- 已继续收口 scoped-search current/former lane 编排：
  - scoped search sibling former lane 的并行启动条件从 `task_type == acquire_full_roster` 放宽为基于 `strategy_type=scoped_search_roster + include_former_search_seed + 当前 lane 非 former`
  - 因此即使入口是 `acquire_search_seed_pool`，也可以启动 former sibling lane，不再依赖 acquire-full-roster 这个历史 task shape
  - 如果 former sibling lane 已经在上游并行完成并写入 durable search-seed snapshot，后续独立 `acquire_former_search_seed` task 会复用 existing former lane，避免重复提交 provider search
- 已校正 webhook/local watcher 默认策略：
  - Meta 这次不是 webhook 失效导致；主要原因是旧 scoped-search task shape 串行，以及 cache-hit completion 写空 remote identifiers
  - 当前已存在 ECS 稳定 webhook URL，因此配置了 `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` 时默认不启动本地 long-poll watcher，避免双路唤醒噪声
  - 只有本地 tunnel 调试或怀疑 webhook URL 失效时，才显式设置 `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=1` 启用 watcher fallback
- 真实运行诊断结论：
  - 这次不是 profile scraper 只跑了 4 个，也不是 profile 获取中断；真实慢点来自旧运行态里 current/full task 完成后才启动独立 former task，以及部分 actor 完成发现依赖 recovery cadence
  - 新代码已修复 scoped-search sibling lane 并行入口、duplicate former task 复用和 remote id 对账；webhook 已配置时继续以 provider push 为主
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/acquisition.py src/sourcing_agent/enrichment.py src/sourcing_agent/harvest_connectors.py tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py -k 'scoped_search_roster_starts_former_search_lane_in_parallel or scoped_search_seed_pool_starts_former_search_lane_in_parallel or acquire_former_search_seed_reuses_existing_durable_former_lane or cache_hit_preserves_remote_identifiers or remote_identifiers_fall_back_to_run_artifacts or local_provider_event_watcher'` -> `8 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or queue_background_profile_prefetch or scoped_search or acquire_former_search_seed or local_provider_event_watcher'` -> `34 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py -k 'scoped_search_seed_pool_starts_former_search_lane_in_parallel or acquire_former_search_seed_reuses_existing_durable_former_lane or queue_background_profile_prefetch_can_use_registry_only_cache_markers or defaults_submit_budget_to_actor_budget or profile_search_prefetch or local_provider_event_watcher or handle_remote_provider_event'` -> `11 passed`

### Meta scoped-search stale serving pointer and hot-cache fallback repair

- 后续追查同一 job 的 `华人线索信息分层` 全是 Layer 0：
  - Meta 正确 snapshot `20260427T153312` 已有 515 candidates / 515 profile detail，但没有 canonical `layered_segmentation`
  - 旧 snapshot `20260423T062947` 有 `greater_china_outreach_20260427T075517Z` 分层产物，且只有 2 candidates，证明此前 background outreach layering 使用了 stale serving pointer
  - 前端同时存在误导性兜底：后端返回 `outreach_layer=null` 时，adapter 会转成 `0`，让“未生成分层”看起来像真实 Layer 0
- 本轮补充修复：
  - completed-workflow outreach layering reconcile 现在在缺失分层或分层 snapshot 与 `candidate_source.snapshot_id` 不一致时，会以当前 candidate source snapshot 为准重新分层
  - outreach layering 生成默认读取/写入 canonical `runtime/company_assets`；只有 canonical 不可读时才退回 hot-cache，避免新增分层资产只落 serving cache
  - 分层 context 读取同时检查 canonical 与 hot-cache，并按 run token 选择最新、同 token 优先 canonical，防止 hot-cache 路径字母序遮蔽 authoritative artifact
  - cached outreach summary 现在保留 `final_layer_distribution`
  - 前端 `outreachLayer` contract 改为 nullable；缺失分层显示“分层未生成/未分层”，不再计入真实 Layer 0
- 已修复真实 Meta job：
  - canonical 分层产物：`runtime/company_assets/meta/20260427T153312/layered_segmentation/greater_china_outreach_20260427T104417Z/layered_analysis.json`
  - job summary `outreach_layering` 已指向 canonical path，`candidate_count=515`
  - 重启 backend/frontend 后 API 全量分页复核：`Layer 0=353`、`Layer 1=33`、`Layer 2=17`、`Layer 3=112`
- 排查 query/job `ae500773be42`（`帮我找Meta做Agent方向的人`）候选人看板长期只显示 2 人的问题：
  - provider/search/profile 数据没有丢失：`runtime/company_assets/meta/20260427T153312/` 已有 `search_seed_discovery.entry_count=513`、`candidate_count=515`、`profile_detail_count=515`、`profile_completion_backlog_count=0`
  - `harvest_profile_batch_*.queue_summary.json` 共 `15` 个且全部 `completed`；requested URL sum 为 `503`，剩余候选人来自既有 cache/registry 复用，不是 profile 抓取中断
  - 真正的 serving 漂移是 job `candidate_source` 与 PG `job_result_view(ae500773be42)` 仍指向 4 月 23 日 Excel/import 形成的旧 Meta snapshot `20260423T062947`（2 人）
- 根因拆解：
  - completed-worker reconcile 中，`snapshot_dir` 会优先用 worker metadata 指向的新 snapshot，但 `snapshot_id` 先读 stale `job_result_view`
  - 这会形成不一致：新 worker 输出 apply 到新目录，但 `_execute_retrieval(... workflow_snapshot_id=old)` 又把 job summary/result-view 重写回旧 2 人 view
  - 真实 repair 还暴露出 hot-cache 风险：hot-cache manifest 存在但 shard 丢失时，authoritative loader 会被坏 hot-cache 截断，没能回退 canonical `company_assets`
- 泛化修复：
  - `resolve_reconcile_snapshot_id(...)` 现在在 worker-driven reconcile 中优先使用实际存在的 worker snapshot dir；只有 worker snapshot 不存在时才回退 `job_result_view` / summary
  - 继续保留 summary sparse 场景：如果 worker metadata 里只有缺失目录，仍可用 result-view 找回 snapshot
  - authoritative candidate artifact loader 遇到坏 hot-cache manifest/shard 时，会跳过坏 serving cache，并在 fallback 路径中强制回到 canonical snapshot / candidate documents；hot-cache 不再能遮蔽 canonical asset store
- 已修复真实 Meta job：
  - `job_result_view(ae500773be42)` 已指向 `snapshot_id=20260427T153312`、`view_kind=asset_population`、`candidate_count=515`
  - job summary `candidate_source` 同步指向 `20260427T153312`
  - direct candidate page 复核：`result_mode=asset_population`、`total_candidates=515`、`profile_fetch_progress=515/515`，首屏候选人带 LinkedIn URL
- 已补回归：
  - completed Harvest worker 指向新 snapshot、旧 job_result_view 指向旧 snapshot 时，reconcile 后 job summary/result-view 必须采用 worker snapshot
  - worker snapshot 缺失时仍用 job_result_view 恢复，避免破坏 sparse summary 兼容路径
  - hot-cache manifest 缺 shard 时 authoritative loader 会回退 canonical candidate documents
  - completed job 缺失 outreach layering 或 layering 指向旧 snapshot 时，会用当前 `candidate_source.snapshot_id` 重新 reconcile
  - canonical 与 hot-cache 同时有分层产物时，不再让旧 hot-cache path 遮蔽较新的 canonical analysis
  - 新增 scripted E2E 回归：baseline authoritative snapshot + current scoped search shard + local profile cache reuse + candidate artifact materialization + outreach layering + candidate board serving 必须收敛到同一个 current `snapshot_id`，并断言不会为了已缓存 profile 再提交新的 Harvest profile actor

### OpenAI scoped-search recall provenance and ECS deployment readiness check

- 排查 query/job `828b63063fb3`（`04/27 14:09`，`我想要OpenAI做Agent方向的人`）候选人看板中 `Agent` recall filter 数量与 Stage 1 preview 不一致的问题：
  - Stage 1 scoped-search preview 是 `54` candidates
  - final candidate board 服务的是 OpenAI full asset population，`total_candidates=826`，不是只服务 Stage 1 preview 子集
  - `linkedin-profile-scraper` 只新拉取 `27 + 11 = 38` 人，是因为另外 `16` 个 profile 已从 cache/registry 复用，不是 provider 少抓
  - 真实问题是 serving projection 丢了 query/source provenance：`candidate_documents.json` 里已有 `metadata.seed_query=Agent`，但 normalized/page shard 在 canonical/history merge 或旧 shard 复用后不稳定暴露这类来源字段
- 已做泛化修复：
  - canonicalization 合并时保留 `seed_query/source_query/query/scope_keywords/seed_keywords/intent_keywords/matched_keywords`，并归一进 `metadata.matched_keywords`
  - candidate artifact writer 复用同一 source-match helper，把来源关键词写入 normalized/page records，并额外写入可审计的多条 `source_matches`
  - frontend recall filter 现在优先用 `matchedKeywords` provenance 判断 recall bucket；只有缺 provenance 时才 fallback 到全文匹配
  - shard fingerprint 纳入 `_CANDIDATE_ARTIFACT_PROJECTION_VERSION`，让 serving projection/schema 变化能使同一 snapshot/view 的旧 serving shard cache 失效
- 设计边界已确认：
  - 一个候选人可以同时属于多个 query/shard，这是正常的；projection version 不是“全局删除旧 sharding 记录”
  - serving layer 仍保持“一个 canonical candidate 一个当前 materialized shard file”，但该文件内部的 `source_matches` 是多值列表，不再表达成“候选人只能属于一个 query/source shard”
  - 当前实现只在同一个 `artifact_dir` / `asset_view` 的 serving cache 中清理 stale shard path，避免继续服务旧投影；历史 snapshot、旧 view、其它 shard provenance 不会被全局抹掉
  - 本轮只为测试重建真实 OpenAI snapshot `runtime/company_assets/openai/20260427T141011/normalized_artifacts/`；其它历史 serving artifacts 先保留，后续按 maintenance backlog 分批 repair
- 真实 OpenAI snapshot 已重建并复核：
  - `candidate_documents.linkedin_stage_1.json` 为 `54`
  - full `candidate_documents.json` / `normalized_artifacts/artifact_summary.json` 为 `826`
  - `profile_fetch_progress=826/826`
  - API 复核 `Agent` provenance count 为 `54`，此前缺失的 `Aarash Heydari`、`Chi Jin`、`Tsubasa S.`、`Vincent Zhao` 已能通过 provenance 进入 Agent recall bucket
  - 二次定向 rebuild 已升级到 `candidate_artifact_projection_v20260427_source_matches`
  - page artifacts 复核：`source_matches_records=826`、`multi_source_match_records=147`、`Agent=54`、预期缺失姓名 `[]`
- 重建暴露的 materialization 慢点已记录为后续 backlog：
  - 首次真实重建 `prepare_candidates≈52s`、`state_upsert≈16s`
  - projection-version full dirty rebuild `prepare_candidates≈54s`、`state_upsert≈0.4-0.7s`、`dirty_candidate_count=826`
  - 当前主要慢点在 `prepare_candidates`，不是 provider/API；后续应从 profile timeline/registry lookup、candidate shard payload prepare、并行度和增量 projection 入手，而不是把 provider submit 重新串回 materialize
- Anthropic 复用 query 边界：
  - `帮我找Anthropic做Pre-training的人` 这类 full asset reuse query 仍可快速分页展示 authoritative snapshot；当前 Anthropic `20260416T225318` 有 `manifest/pages`，`candidate_count=3455`
  - 该旧 snapshot 的 pages 还没有新版 `matched_keywords/source_matches` provenance；召回排序会按前端 fallback 走候选人文本匹配，不需要为了手动测试先全量 rebuild Anthropic
  - 如果后续要求 Anthropic recall filter 与 OpenAI 新版一样走可审计 source provenance，再定向 rebuild 对应 Anthropic snapshot，而不是全历史重建
- ECS 部署前只读检查：
  - `https://api.111874.xyz/health` / `/api/runtime/health` / `/api/providers/health` 当前可访问，远端 service 进程 PID `116522` 仍在运行
  - 当前 ECS service unit 使用 `/opt/sourcing-ai-agent` + `/opt/sourcing-ai-agent/venv/bin/python3`，不是文档中推荐的 `/srv/sourcing-ai-agent/repo/sourcing-ai-agent` repo layout
  - 当前进程已有 PG-only 关键 env：`SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`、`SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`、`SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`、`SOURCING_EXTERNAL_PROVIDER_MODE=live`
  - 当前进程缺少 `SOURCING_RUNTIME_ENVIRONMENT=production`、`SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL`、provider webhook token env
  - 当前 ECS hosted route `POST /api/providers/apify/webhook` 返回 `404`，说明远端仍是旧后端代码；部署当前本地代码前，不能假设 hosted provider webhook 已可用
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/domain.py src/sourcing_agent/canonicalization.py src/sourcing_agent/candidate_artifacts.py tests/test_canonicalization.py tests/test_candidate_artifacts.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_canonicalization.py tests/test_candidate_artifacts.py tests/test_results_api.py -k 'source_seed_keyword_provenance or reuses_candidate_shards_and_rebuilds_only_dirty_candidates or search_seed_preview_requires_profile_completion or seed_query_as_matched_keyword'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `cd frontend-demo && npm run build` -> passed
  - 真实 OpenAI artifact 计数复核：Stage 1 `54`、full/summary/manifest `826`、profile detail `826`、`Agent` provenance `54`、缺失预期姓名 `[]`
  - `git diff --check -- PROGRESS.md docs/NEXT_TODO.md docs/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md docs/ECS_PRELAUNCH_CHECKLIST.md src/sourcing_agent/domain.py src/sourcing_agent/canonicalization.py src/sourcing_agent/candidate_artifacts.py frontend-demo/src/lib/candidateFilters.ts tests/test_canonicalization.py tests/test_candidate_artifacts.py`

### Windsurf candidate board serving artifact and profile prefetch scheduling repair

- 排查 query/job `e69cbd03045f`（`04/27 13:12`，`帮我找Windsurf的全部成员`）候选人看板中部分卡片没有“打开 LinkedIn”按钮：
  - `candidate_documents.json` 与 candidate shard 的 `materialized_candidate.linkedin_url` 已有 URL
  - `normalized_artifacts/pages/page-*.json` 只写入 normalized record，normalized contract 只保留 `has_linkedin_url/urls/public_identifier`，没有保留顶层 `linkedin_url`
  - 这是 serving artifact/page materialization contract bug，不是 provider 没返回 URL，也不是前端按钮逻辑本身缺失
- 泛化修复：
  - `_normalize_candidate(...)` 现在把 `candidate.linkedin_url` 写入 normalized record
  - 正常 `build_company_candidate_artifacts(...)` 和 `repair_paginated_candidate_artifacts_from_materialized(...)` 都会从 materialized candidate、normalized record、reusable document、profile completion backlog 回填 serving identity fields
  - 如果旧 shard/page 缺 serving identity，后续 artifact build 会标记 `serving_identity_fields_changed` 并重写，不依赖 Windsurf 特例
- 已修复真实 Windsurf snapshot：
  - `runtime/company_assets/windsurf/20260427T131256/normalized_artifacts/pages/page-0001.json` 中 `Abd samad Chbani`、`Achuil Aguer Akot`、`Ahmad Wanni` 均已有顶层 `linkedin_url`
  - HTTP 复核 `GET /api/jobs/e69cbd03045f/candidates?offset=0&limit=80&lightweight=true&force_refresh=1` 返回 `total_candidates=423`、`profile_fetch_progress=423/423`，样本候选人均返回 LinkedIn URL
- 排查同一 job 的 Harvest profile actor 调度：
  - Apify 后台的批次确实没有持续吃满 4 个 actor；本地事件线显示 actor global limiter budget 为 `4`，但 profile prefetch 的 submit budget 默认被 adaptive window 的 `recommended_max_workers` 压到 `1` 或 `2`
  - 典型事件：active worker 仍为 `1` 时，submit budget 也降为 `1`，导致 tail URL 被判 `harvest_profile_prefetch_backpressure`，只能等下一轮 recovery / webhook 事件再发
  - 修复后 adaptive window 继续决定 batch size 和建议并发，但默认 submit budget 至少等于 `harvest_profile_actor_global_inflight`；显式 `harvest_profile_batch_submit_global_inflight` 仍可覆盖以保护 provider
- 前端自动刷新修复：
  - candidate board refresh polling 不再只看 queued/failed/missing/deferred；当 `fetched + unrecoverable < total` 时也视为 profile 追平未完成并继续刷新
  - LinkedIn 同步文案增加“追平 N”，避免 `LinkedIn 211/423，排队 0，可重试 0` 让用户误以为没有 pending work
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/enrichment.py tests/test_candidate_artifacts.py tests/test_enrichment.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py tests/test_enrichment.py -k 'search_seed_preview or repair_paginated_candidate_artifacts_from_materialized_does_not_promote_registry or queue_background_profile_prefetch_defaults_submit_budget_to_actor_budget or queue_background_profile_prefetch_defers_when_active_batch_worker_exhausts_budget'` -> `5 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_pipeline.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or background_prefetch or remote_event_followup or queue_background_profile_prefetch'` -> `16 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'asset_population_lightweight_page_preserves_linkedin_url_before_profile_detail or profile_fetch_progress or dashboard_is_summary_only'` -> `2 passed`
  - `cd frontend-demo && npm run build` -> passed

### Wispr Flow roster/search-seed lane hydration repair

- 排查 query/job `87fa38d196a8`（`04/27 11:54`，`帮我找Wispr Flow的全部成员`）候选人看板一度显示 `84/84`、随后 `102/102`，但 provider 实际返回 `102 former/search-seed + 85 current roster` 的原因：
  - company roster worker 已把 current roster artifacts 落盘
  - search-seed/former lane 的 in-memory state 仍只有 `102` 人
  - 后续 `enrich_linkedin_profiles` 直接信任 stale in-memory state 写回 `candidate_documents.json`，覆盖了 roster worker 早先落盘的 current roster 结果
  - 这不是 provider 没返回，也不是缺 LinkedIn URL；是并行 lane 完成顺序不同后，下游 enrichment 输入状态没有从 durable snapshot artifacts 重新 hydrate/merge
- 泛化修复：
  - `AcquisitionEngine._enrich_profiles(...)` 入口现在会先从当前 `snapshot_dir` hydrate roster snapshot 与 search-seed snapshot，再与内存 state 合并后进入 canonicalization/enrichment
  - 新增 `load_search_seed_snapshot_from_snapshot_dir(...)`，复用 search-seed registry 的 lane/aggregate artifacts，而不是在 acquisition 里写 Wispr 特例
  - 修复 search-seed snapshot loader 的多 worker 乱序风险：即使 root `search_seed_discovery/entries.json` 已存在，也会继续合并 lane 子目录里的 entries；例如 `Agent` seed 已在 root、`Multimodal` worker 只写入 `search_seed_discovery/current/entries.json` 时，enrichment 入口会看到两组 seed
  - roster 和 search-seed 任一 lane 先完成或后完成，都以 snapshot artifacts 作为 durable handoff 重新合并；修复边界在 enrichment 输入层，不把 profile enrichment、provider submit、delta apply、candidate artifact materialization 重新串成单条同步链
- 已修复真实 Wispr snapshot：
  - `runtime/company_assets/wisprflow/20260427T115446/candidate_documents.json` 已恢复到 `185` candidates
  - current/former 口径为 `84 current + 101 former`；`85 current + 102 former/search-seed` 中有 `2` 个重复人员被 canonicalize 合并
  - `normalized_artifacts/artifact_summary.json` 现在为 `candidate_count=185`、`profile_detail_count=185`、`profile_completion_backlog_count=0`、`missing_linkedin_count=0`
  - direct orchestrator 复核 job `87fa38d196a8` candidate page：`result_mode=asset_population`、`total_candidates=185`、`profile_fetch_progress=185/185`
- 看板追平修复：
  - asset population cache key 已包含 `artifact_summary.json` / `manifest.json` 文件 fingerprint，异步 artifact rebuild 后 backend 不应继续返回旧 profile progress summary
  - 目标候选人看板 hydration hook 增加 bounded asset-population refresh polling，异步 materialization 追平时前端不再完全依赖手动刷新
  - 就业状态 filter 不再展示非预期的 `线索` 选项；未知/lead rows 只在“在职+已离职”同时选中时进入结果
  - “打开 LinkedIn”按钮仍按 `candidate.linkedinUrl` 渲染；修复后 API 首屏候选人已返回 canonical LinkedIn URL 和 profile detail
- 当前本地 backend/worker/frontend 已重启；HTTP 复核 `GET /api/jobs/87fa38d196a8/candidates?offset=0&limit=5&lightweight=true` 返回 `total_candidates=185`、`profile_fetch_progress=185/185`，首屏候选人有 canonical LinkedIn URL。
- 新增回归：
  - state 只有 search-seed、disk 有 roster
  - state 只有 roster、disk 有 search-seed
  - state/root aggregate 只有 `Agent` scoped seed、disk lane 子目录有 `Multimodal` scoped seed
  - asset population lightweight page 在 profile detail 未完成前仍保留 `linkedin_url`，供前端渲染“打开 LinkedIn”按钮

### Scoped-search / search-seed event-level response

- 继续收口 Wispr 之后暴露的更泛化边界：enrichment 入口能从 durable roster/search-seed artifacts hydrate，但如果某个 scoped-search provider worker 刚完成、尚未被本地 apply 成 snapshot artifact，后续 profile prefetch 仍看不到这批 usable LinkedIn URLs。
- 已把 search-seed worker 纳入 LinkedIn Stage 1 事件级 contract：
  - `SearchSeedAcquirer._execute_query_spec(...)` 创建的 search workers 现在带 `recovery_kind=search_seed_discovery`
  - `workflow_event_response` 的 `linkedin_stage_1` registry 覆盖 `search_planner` / `public_media_specialist` 的 `waiting_remote_search` workers
  - worker completion callback 现在支持 `search_seed_discovery`，会进入同一 inline incremental writer，而不是等整轮 aggregate 或 pre-retrieval refresh 才处理
- 新的本地顺序为：
  - 单个 search shard completed 后，先把 entries durable apply 到 `search_seed_discovery/entries.json` 与 `candidate_documents.json`
  - apply 后立即触发 `_queue_background_profile_prefetch_from_search_seed_snapshot(...)`
  - 如果同类 search shard 仍在 queued/running，full candidate artifact / PG materialize 返回 `deferred`，不阻塞 profile prefetch；同类 shard drain 后再由 shared writer budget 追平
- 这不是只服务 live roster 的修复：同一 contract 覆盖 scoped-search 多 shard query，例如 `OpenAI Agent + Multimodal` 这类多个 query worker 乱序完成的场景。之前的 disk hydration 负责 enrichment 入口合并已落盘 lane；本轮补的是“单 shard 完成本地 apply/prefetch”的上游事件入口。
- 新增回归：
  - `search_seed_discovery` remote-wait worker 可归入 `linkedin_stage_1` registry
  - search-seed 单 shard completed 时会先 durable apply 并触发 profile prefetch；当另一个 search shard 仍在运行时，full materialize 不会被调用，而是 deferred

### Anthropic full-reuse candidate board loading repair

- 排查 query/job `b992f9df009c`（`04/27 10:46`，`帮我找Anthropic做Pre-training的人`）自动跳到候选人看板后一直显示“候选人看板加载中”的原因：
  - job 已是 `completed`，但 job summary 和 PG `job_result_view` 都保留旧 contract：`candidate_source.source_kind=sqlite_store`、`view_kind=ranked_results`、`snapshot_id/source_path` 为空
  - PG `job_results` 对该 job 为 `0` 行，因此 results API 选择 ranked-results 后返回 `total_candidates=0`
  - Anthropic authoritative organization asset registry 实际可用：`snapshot_id=20260416T225318`、`candidate_count=3455`、`profile_detail_count=3389`、`profile_completion_backlog_count=57`
- 修复路径：
  - results resolver 现在会识别 retired/不完整 candidate source（例如旧 `sqlite_store`）且请求语义是 full asset/reuse 时，从 authoritative `organization_asset_registry` 恢复成 `company_snapshot` candidate source
  - 如果 persisted `job_result_view` 已是 `asset_population/company_snapshot`，它会覆盖旧 summary source；如果 result view 本身也是旧 `sqlite_store/ranked_results`，读取时会做一次 bounded PG repair，把该 job 的 result view 持久化为 `company_snapshot/asset_population`
  - 对缺少 paginated `normalized_artifacts/manifest.json/pages` 的旧 snapshot，summary path 不再为了 `profile_fetch_progress` 读取全量 materialized JSON 并对所有候选人 derive facets，而是用 `artifact_summary` 的 profile 计数字段构造轻量进度
  - 新增 monolithic `materialized_candidate_documents.json` window reader；当 manifest/pages 缺失时，候选人首屏可直接切片 monolithic artifact，不再构造全量 `Candidate` 对象
- 已对真实 job 执行修复并重启本地 backend：
  - PG `job_result_view(b992f9df009c)` 已修为 `source_kind=company_snapshot`、`view_kind=asset_population`、`snapshot_id=20260416T225318`
  - HTTP 复核：`/api/jobs/b992f9df009c/dashboard` 返回 `asset_population.available=true`、`candidate_count=3455`、`candidate_source_kind=company_snapshot`
  - HTTP 复核：`/api/jobs/b992f9df009c/candidates?offset=0&limit=50&lightweight=true` 返回 `result_mode=asset_population`、`returned_count=50`、`total_candidates=3455`
  - 停止了一个上周五遗留的 hosted smoke Python 进程；该进程不是当前 backend/frontend/worker，但持续占 CPU 并影响本地联调延迟
- 维护审计状态：
  - 已确认当前 20 个 authoritative organization snapshot 都有 paginated `manifest.json/pages`；PG `legacy job_result_view` 为 `0`
  - 新增 `repair-paginated-candidate-artifacts` 维护 CLI，可从已有 `materialized_candidate_documents.json` 原地派生 missing `manifest/pages/candidate shards`，不提交 provider、不改变 authoritative registry
  - 全历史 dry-run 发现 138 个 view 缺 paginated artifacts，主要是历史 Humans& / Thinking Machines Lab / OpenAI / Google 等非 authoritative row；按当前联调优先级先不修大公司历史快照，后续分批维护
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/results_store.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'legacy_sqlite_candidate_source or result_view_without_summary_candidate_source or dashboard_is_summary_only or auto_materialize_snapshot_candidate_documents'` -> `4 passed`

### OpenAI candidate board profile registry/materialization drift repair

- 排查 query `b12f28496204`（`2026-04-27T02:17:33Z`，OpenAI Reasoning）里 `候选人同步 784/784` 但 `LinkedIn 494/505` 的原因：
  - 原始口径差异是 candidate board rows 与 LinkedIn profile registry URL rows 不同，但 `AJ Sakher` 暴露了更深的 registry/materialization drift
  - `AJ Sakher` 的 raw Harvest profile 已存在于 `runtime/company_assets/openai/20260423T165904/harvest_profiles/36af4eabf9797876.json`，包含 canonical URL `https://www.linkedin.com/in/aj-sakher`、Seattle 地区、完整经历/教育
  - PG `linkedin_profile_registry` 缺少 raw ACw URL 与 canonical slug 的 fetched/alias row，后续 artifact rebuild 依赖 registry 解析 timeline，导致 page/manifest 回退到 missing-profile shard
- 修复路径：
  - `build_company_candidate_artifacts(...)` 在 materialize artifact view 前会对当前 snapshot 的 `harvest_profiles` 做 scoped `backfill_linkedin_profile_registry(...)`，并把结果写入 artifact summary / sync status
  - `SnapshotMaterializer.apply_harvest_profile_workers_to_snapshot(...)` 成功从 registry 或 snapshot cache 读到 completed profile payload 后，会补写 `mark_linkedin_profile_registry_fetched(...)` 与 alias metadata，避免 provider event ingest 后留下 raw/registry drift
  - 新增 regression 覆盖：candidate URL 是 Harvest search 返回的 `ACw...` URL，raw profile payload 给出 canonical slug；即使 registry 预先为空，artifact materialization 也会先 backfill registry 并输出完整 profile detail
- 已对真实 OpenAI snapshot 执行修复：
  - `PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli build-company-candidate-artifacts --company OpenAI --snapshot-id 20260423T165904`
  - scoped registry backfill 处理 `452` 个 raw profile 文件，`452 fetched`、`0 errors`
  - 重建后 canonical artifact：`candidate_count=784`、`profile_detail_count=784`、`profile_completion_backlog_count=0`、`missing_linkedin_count=0`
  - API 复核：`AJ Sakher` 返回 canonical LinkedIn URL、Seattle 地区、headline、8 条工作经历、1 条教育经历；candidate page `profile_fetch_progress=784/784`、`missing_registry_url_count=0`
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/snapshot_materializer.py tests/test_candidate_artifacts.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py -k 'backfills_snapshot_profile_registry_before_materializing'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_profile_registry_backfill.py tests/test_candidate_artifacts.py -k 'profile_registry or backfill or foreground_fast'` -> `8 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'harvest_profile_batch or background_harvest_prefetch or snapshot_materializer_candidate_delta'` -> `8 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'candidate_page_includes_profile_timeline_preview or profile_registry_raw_path or recover_profile_top_education or search_seed_preview_requires_profile_completion or sparse_harvest_profile_detail_requires_profile_completion'` -> `5 passed`
  - `tests/test_results_api.py -k 'profile_fetch_progress or materialized_candidate or needs_profile_completion'` 没有匹配测试，pytest 返回 `48 deselected`

### LinkedIn Stage 1 webhook edge cases and adaptive profile batching

- 继续检查 Lovable / Harvest profile pipeline 的响应不及时问题，当前结论分为两层：
  - Lovable 这批 tail profile actors 已全部被本地消费，`harvest_profiles` raw profile-like 文件已推进到 `1148`，18 个 `harvest_profile_batch_*.queue_summary.json` 全部 `completed`，job `80efbad6aaec` 没有残留 recoverable worker
  - PG/control-plane 当前 `candidate_count_for_company('Lovable')=1155`、`evidence_count_for_company('Lovable')=2328`，与 full `candidate_documents.json` / evidence count 对齐
  - `candidate_documents.linkedin_stage_1=173` 是 search-seed/stage snapshot 的候选人数口径，不是已取回 LinkedIn profile payload 的数量；当前 raw profile payload 已明显超过 200
  - 系统层面仍要求 hosted/local runtime 在提交新 Harvest actor 前配置 `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` 和 webhook shared secret 才能使用 provider push；未配置 webhook 的历史本地 CLI run 不会被 Apify retroactively 推送，新提交的本地 profile actor run 已有 local long-poll watcher 兜底
  - Apify 后台 `2026-04-27 03:58` 两个 `72` result run 和 `04:13` 四个 `36` result run 已匹配到 Lovable snapshot，是本地 runtime/recovery drain 继续提交的 Lovable tail；`04:41` / `04:50` 两个 `1` result run 是此前 Andrew Ng one-profile webhook smoke
- 补充验证和回归覆盖：
  - Apify `ACTOR.RUN.FAILED` 事件会和 `SUCCEEDED` 事件一样唤醒匹配的 LinkedIn Stage 1 remote-wait worker
  - provider webhook path 仍 quick-ack，job-scoped recovery 禁止内联跑 post-completion reconcile / housekeeping，避免 provider webhook 30s 窗口内阻塞
  - mixed-success / unresolved URL retry 仍由 profile registry 粒度控制，已 fetched URL 不进入后续 retry payload
- 调整 background profile prefetch 的 live batch sizing：
  - 原先 priority prefetch live cap 为 `75`，再经 balanced chunk 后容易形成 Lovable 上看到的固定 `73` 左右批次
  - 现在 profile-search/former-search live lane 复用 adaptive live fetch window，默认约 `45-55` URLs/batch，以更快拿到第一批结果并进入 local apply
  - 同时提交的新 worker 仍受 `harvest_profile_batch_submit_global_inflight` 控制，真实 provider actor in-flight 仍受 `harvest_profile_actor_global_inflight` / `SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT` 控制，默认保持在 4 左右，避免触发 Harvest/Apify too-many-requests
- 继续记录 materialization/PG 优化方向，但本轮未贸然改变写入 contract：
  - 当前 profile event path 已保证 next submit 不等待 full materialize
  - 本轮已加 changed-candidate lightweight control-plane upsert：当 profile batch delta 已写入 `candidate_documents` 但同类 worker 仍未 drain 时，先按 changed candidate IDs 更新 candidates/evidence，让看板更快看到已到货 profile detail
  - Harvest background snapshot materialization 默认使用 foreground/serving-fast artifact build profile；full hot-cache/compatibility refresh 保留给后续 maintenance/repair，而不是 provider event path
  - full snapshot materialize/PG 写入仍有优化空间：actor/apply/PG/artifact lag metrics 反推 batch window，并继续把 full rebuild 收敛为 repair/maintenance path
- 无 webhook 本地 run 的完成发现也做了 bounded fallback：
  - 如果未配置 `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL`，新提交的 Harvest profile actor 会启动 local long-poll watcher
  - watcher 只在观察到 provider terminal status 后注入同一 `remote_provider_event`，watch window 到期只停止本地等待，不会把 actor 标记为 failed
  - 如果已配置 Apify webhook，则不启用本地 watcher，避免重复唤醒
- Apify webhook 配置口径：
  - `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` 必须是 Apify 能访问到的公开 backend URL，例如 tunnel 或 hosted API
  - `providers.local.json` 里的 Harvest `api_token` 确实是调用 Apify API 的 token，可用于 submit actor run、给 run 附带 ad-hoc webhook，或查询 `GET /v2/acts/:actorId/webhooks`
  - `SOURCING_PROVIDER_WEBHOOK_TOKEN` 是本系统校验 inbound webhook 的共享密钥，不是 Apify API token；`APIFY_WEBHOOK_TOKEN` 只是兼容别名，避免把 Apify API token 暴露给 inbound callback
  - 由于短期试运行允许复用现有 Apify API token，本轮已把 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN` 默认置为启用：submit path 会把 Harvest actor `api_token` 写入 ad-hoc webhook header，backend webhook endpoint 也会接受同一 token；可用 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=0` 关闭，长期成熟后仍建议切回独立 shared secret
  - 只读 probe 已验证三个 Harvest actor 的 `GET /v2/acts/:actorId/webhooks` 可访问，当前 persistent actor webhook 列表均为 `0`；现有实现使用 run-scoped ad-hoc webhook，不依赖预创建 persistent webhook
  - 已把 `/runtime/secrets/temporary_apify_webhook_token` 中的临时 Apify token 写入 `runtime/secrets/providers.local.json` 的三个 Harvest actor 配置，并在写入前保留 `runtime/secrets/providers.local.backup_before_apify_webhook_20260426T202500Z.json`
  - 新增 `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`，记录 ECS 稳定域名、本地 tunnel、ad-hoc webhook 参数、token 口径、connectivity probe、one-profile round-trip smoke 和排障
  - 新增 `scripts/apify_webhook_roundtrip_smoke.py`，作为显式 opt-in live smoke；默认禁用 local watcher，要求公开 webhook URL 和 `--i-understand-this-submits-live-run`
  - 已在 ECS Nginx 增加 local-dev relay：`https://api.111874.xyz/local-dev/providers/apify/webhook -> 127.0.0.1:18765/api/providers/apify/webhook`，配合 `scripts/ecs_webhook_reverse_tunnel.py` 让本地 backend 使用稳定公开 callback URL 做 webhook smoke
  - ECS relay live smoke run `SeuvL6YsW48M2t1ML` / dataset `09ivTJCt5FhZHgZaP` 的 Apify ad-hoc dispatch `4hW8x91WACMHAROc2` 已在 Apify 侧显示 `SUCCEEDED`；该 one-profile actor 约 2 秒完成，本地 polling 先完成 worker，暴露出 webhook 到达时只扫描 recoverable worker 会丢审计事件的竞态
  - 已修复该竞态：`handle_remote_provider_event` 先唤醒 recoverable remote-wait worker；若同 `run_id` / `dataset_id` 的 known worker 已完成，则记录 `remote_provider_event: received_late`，不重启 recovery、不重放 dataset ingest
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/enrichment.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/storage.py src/sourcing_agent/runtime_tuning.py src/sourcing_agent/remote_provider_events.py tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_runtime_tuning.py tests/test_remote_provider_events.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_enrichment.py -k 'remote_provider_event or webhook or profile_search_prefetch or balanced_live_batches or bounded_parallel_live_batches or global_inflight_budget'` -> `10 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_runtime_tuning.py -k 'harvest_profile_batch or background_harvest_prefetch or company_employees or global_inflight_budget or code_22 or mixed_success or webhook or wait_for_finish or profile_search_prefetch'` -> `23 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_pipeline.py -k 'remote_provider_event or webhook or remote_event_followup or skip_post_completion_reconcile or skip_post_recovery_housekeeping'` -> `7 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_pipeline.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py -k 'remote_provider_event or webhook or wait_for_finish or mixed_success or code_22 or local_event_watcher or profile_search_prefetch or background_prefetch or snapshot_materializer_candidate_delta or foreground_fast_artifact_profile'` -> `16 passed`
  - `git diff --check -- src/sourcing_agent/enrichment.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/orchestrator.py src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/storage.py tests/test_enrichment.py tests/test_pipeline.py tests/test_remote_provider_events.py PROGRESS.md docs/NEXT_TODO.md docs/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/api.py src/sourcing_agent/harvest_connectors.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py tests/test_remote_provider_events.py -k 'ad_hoc_webhook or api_token or provider_webhook or webhook_endpoint'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/workflow_event_response.py src/sourcing_agent/remote_provider_events.py scripts/apify_webhook_roundtrip_smoke.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py tests/test_harvest_connectors.py tests/test_pipeline.py tests/test_results_api.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_workflow_event_response.py -k 'remote_provider_event or remote_event_lane or webhook'` -> `11 passed`
  - 真实 PG 路径复核：用 `run_id=SeuvL6YsW48M2t1ML` / `dataset_id=09ivTJCt5FhZHgZaP` 找到 completed worker `231`，重放 provider event 后返回 `reason=matching_remote_provider_workers_not_recoverable` 并写入 `remote_provider_event: received_late`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_harvest_connectors.py tests/test_pipeline.py tests/test_results_api.py tests/test_target_candidate_public_web.py tests/test_workflow_event_response.py -k 'webhook or remote_event or target_candidate_public_web or public_web_api or promotion'` -> `17 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'agent_worker or target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_frontend_browser_e2e.py -k 'target_candidate_public_web_selection_trigger_and_polling or target_candidate_public_web_promotion_and_export'` -> `2 passed`
  - `cd frontend-demo && npm run build` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `git diff --check -- <touched event/webhook/Public Web/docs files>` -> passed
- 本地前后端已重启用于后续人工联调：
  - backend: `http://localhost:8765` API OK；`GET /api/target-candidates/public-web-search?limit=5` 返回 `status=ok`
  - frontend: `http://127.0.0.1:4173` root OK
  - worker daemon: running，当前 recoverable worker count 为 `0`
  - 当前 backend/worker 已带 `SOURCING_APIFY_WEBHOOK_URL=https://api.111874.xyz/local-dev/providers/apify/webhook` 与 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1` 启动；`scripts/ecs_webhook_reverse_tunnel.py` 仍在运行
  - ECS local-dev relay connectivity probe 返回 HTTP `202` / `mode=async_recovery`；下一次本地 Harvest run 会尝试走 Apify provider webhook push，若 tunnel 断开则需要重新启动 tunnel 或回退到 local watcher/recovery
  - 重启后的本地 webhook endpoint 用 `sync=1` 重放 `SeuvL6YsW48M2t1ML` completion event，返回 `reason=matching_remote_provider_workers_not_recoverable` / `recovery_count=0`，证明 late-event path 已在当前服务进程生效

### LinkedIn Stage 1 remote completion wakeup

- 继续压缩 Lovable 暴露的 live tail latency，但保持上一段修复的核心边界：不把 `apply/materialize` 重新耦合回 next submit path。
- 新增 `workflow_event_response` 抽象，把远端等待 worker 的事件响应 lane 做成 registry，而不是在 orchestrator 中继续写 provider/company 特例：
  - 当前启用 lane 为 `linkedin_stage_1`
  - 覆盖 `acquisition_specialist` / `enrichment_specialist`
  - 覆盖 `harvest_company_employees` / `harvest_profile_batch`
  - 识别 `waiting_remote_harvest` / `waiting_remote_search`
- `run_worker_recovery_once` 现在在 shared recovery tick 后会做 bounded `remote_event_followup`：
  - 默认 `WORKFLOW_REMOTE_EVENT_FOLLOWUP_ENABLED=true`
  - 默认 `WORKFLOW_REMOTE_EVENT_FOLLOWUP_ROUNDS=1`
  - 发现 LinkedIn Stage 1 remote-wait worker 后，立即对对应 job 运行 job-scoped follow-up daemon
  - follow-up summary 合并回 `daemon`，并在 response 暴露 `remote_event_followup` 的 scope、target jobs/workers、claimed/executed counts
  - explicit job recovery 已有自己的 follow-up rounds，因此不会重复走 shared remote-event follow-up
- 同步修复一个 job-scoped recovery 语义回归：当 `workflow_resume_explicit_job=false` 且 `workflow_stale_scope_job_id` 指向当前 job 时，0 秒 stale gate 仍可恢复 scoped queued job，同时继续忽略 unrelated queued jobs。
- 新增架构文档 `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`，沉淀可复用 contract：
  - `remote_wait`
  - `completion_discovery`
  - `local_event_apply`
  - `downstream_materialize`
  - 其他 workflow 后续应通过 registry 和 regression 接入，不应复制 Lovable 专用补丁。
- 本轮复核 Lovable artifacts：
  - `candidate_documents.json=1155`
  - `candidate_documents.linkedin_stage_1.json=173`
  - `harvest_company_employees_visible.json=1008`
  - `harvest_company_employees_merged.json=1009`
  - `harvest_profiles/*.json=1220`，其中 raw profile-like payloads `1148`
  - full normalized candidate artifacts `1155`
  - strict roster candidate artifacts `818`
  - tail batch `harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json` 已为 `status=completed`、`requested_url_count=73`、`run_id=LLf6slcMDtP9WlKZf`、`dataset_id=Q95IGLwSSd9EEjoey`
- 其他 workflow 的事件级响应 backlog 已记录在 `docs/NEXT_TODO.md`：Target-candidate Public Web Search、Search seed discovery、segmented company employees roster、Public media/exploration workers、runtime lag metrics。

### Harvest profile completion event orchestration

- 继续收口 Lovable 暴露的 `linkedin-profile-scraper` 编排问题，明确这不是 roster 缺失，而是 provider completed 后本地消费、next batch submit、apply/materialize 三段耦合过深。
- 已把 Harvest profile completed worker 的本地处理收敛到统一 completion event contract：
  - worker completion callback、running workflow pre-retrieval refresh、running inline reconcile、completed workflow reconcile 都先触发 `provider completed -> local ingest visible -> next profile prefetch submit`，再执行 snapshot delta apply/materialize
  - running pre-retrieval refresh 不再保留 `apply current batch -> submit next batch -> sync` 的旧顺序分叉
  - 当 next profile worker 已 queued/deferred/active 时，当前 batch 只做 Harvest profile delta apply 与 worker-level `inline_incremental_ingest` marker，完整 candidate artifact rebuild 延后到同类 worker drain 后
  - pre-retrieval refresh 现在也会给已消费的 Harvest profile worker 写 `inline_incremental_ingest` marker，避免后续 tail worker callback 再次把同一 completed worker 纳入 micro-batch
- provider retry 行为补齐两个回归点：
  - Harvest dataset fetch 将 `Too many queued requests (code_22)` 识别为 retryable provider queue backpressure；HTTP 400 不直接短路 marker 判断
  - mixed-success profile batch 只对 unresolved URLs 做后续 retry/fallback，已成功解析的 URL 不进入后续 provider payload
- 复核 Lovable 真实 artifacts 仍与 handoff 一致：
  - job `80efbad6aaec` 仍为 `completed`，preview 为 `preview_ready`
  - `candidate_documents.json=1155`、`candidate_documents.linkedin_stage_1.json=173`
  - `harvest_company_employees_visible.json=1008`、`harvest_company_employees_merged.json=1009`
  - `harvest_profiles/*.json=1220`、raw profile-like payloads `1148`、full normalized candidate artifacts `1155`、strict roster candidate artifacts `818`
  - tail batch `harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json` 已记录 `status=completed`、`requested_url_count=73`、`run_id=LLf6slcMDtP9WlKZf`、`dataset_id=Q95IGLwSSd9EEjoey`
- 仍保留的真实边界：这次修的是“本地一旦观察到 completed worker/dataset 后的事件级响应和顺序解耦”；provider 侧完成事件的外部发现仍依赖现有 poll/recovery/worker callback，并未新增 Harvest webhook 或 provider 推送通道。

### Harvest runtime follow-up and documentation framing cleanup

- 补上这轮 company-roster / Harvest runtime 修复的 closeout 记录：
  - `orchestrator.py` 新增 completed-workflow 后台结果发现逻辑；即使没有 stale/recoverable worker，daemon 也会重新发现“workflow 已 completed，但仍有已完成 background worker 尚未被消费”的 case
  - `storage.py` 的 PG `replace_company_data(...)` 已切到 bulk delete + bulk upsert，候选人/evidence 同步不再走逐行写入；这解决了 Lovable 这类大 snapshot 在 reconcile/materialization 阶段看起来像“卡死”的主要控制面热点
  - Harvest 远端状态轮询与 dataset 下载新增 fast-yield runtime tuning：background `harvest_profile_batch` recovery 默认使用更保守的 HTTP timeout / fetch attempts，避免单个慢 provider 请求长时间占住一整个 recovery cycle
- 这轮 live 诊断确认了两个重要事实：
  - Lovable 的 `58 + 4 x 73` profile batch 不是整批重跑，而是互不重叠的 deferred tail drain
  - 当前更大的剩余问题不是重复提交，而是 remote completed run 仍主要靠 daemon/recovery 轮询节奏发现，尚不是事件级同步
- 文档整理前也把“为什么用户会感觉整条链路慢”拆得更明确：
  - `harvestapi/linkedin-profile-scraper` 在 provider 侧完成后，本地系统不一定会立刻收到并消费这个完成事件；常见情况仍是等下一轮 daemon/recovery 扫描，或等当前慢 HTTP poll/dataset download 返回
  - 结果就是 provider 已经完成，但本地 `profile_fetch_progress`、候选人详情物化、下一轮 profile batch submit 仍可能停在旧状态，形成“上游已到货、下游还没开工”的空转时间
  - 当时 orchestration 仍存在上游 submit 与下游 apply/materialize 耦合过深的问题：有些路径上，新的 profile-scraper batch submit 事实上要等前一批 completed batch 被 poll/download/apply/materialize 之后才继续推进，而不是在 provider completed 且 budget/lease 允许时就立即排下一批
  - medium former/search-seed 清单尤其受这个问题影响：理想状态应是“前一批一完成就继续排下一批 actor，同时把已完成结果异步交给下游 apply/materialize”；而不是把 submit、ingest、materialize 串成单条长链
- 最近一次 live 观察到的 Lovable profile registry 已推进到 `504 fetched / 73 queued`；说明 bulk replace + queued-run resume 修复已经让 tail 继续前进，但最后一段 completion detection 仍值得继续压缩
- Lovable 继续排查时，优先看这些现成落点：
  - job files：`runtime/jobs/80efbad6aaec.json`（当前 `status=completed`）与 `runtime/jobs/80efbad6aaec.preview.json`（当前 `status=preview_ready`）
  - snapshot root：`runtime/company_assets/lovable/20260426T193540/`
  - current/full roster provider outputs：`runtime/company_assets/lovable/20260426T193540/harvest_company_employees/`
    - `harvest_company_employees_visible.json` 当前 `1008` rows
    - `harvest_company_employees_merged.json` 当前 `1009` rows
  - former/search-seed summary：`runtime/company_assets/lovable/20260426T193540/search_seed_discovery/summary.json`（当前 `entry_count=173`）
  - main candidate snapshot：`runtime/company_assets/lovable/20260426T193540/candidate_documents.json`（当前 `1155` candidates）
  - stage-1-only snapshot：`runtime/company_assets/lovable/20260426T193540/candidate_documents.linkedin_stage_1.json`（当前 `173` candidates）
  - fetched profile/raw batch artifacts：`runtime/company_assets/lovable/20260426T193540/harvest_profiles/`（当前 `1220` 个 `*.json`，其中 raw profile-like payloads `1148`，包含 raw profile 与 batch queue/run/dataset artifacts）
  - normalized candidate artifacts：`runtime/company_assets/lovable/20260426T193540/normalized_artifacts/candidates/`（当前 full candidate JSON `1155` 个）；`strict_roster_only/candidates/` 是 filtered view（当前 `818` 个）
  - 这些数量说明当前 tail 已经从 provider/raw 层基本追平；后续重点是确认 board/API 默认读取 full candidate artifacts / PG candidate rows，而不是误把 strict filtered view 当作完整物化口径
- 当前还能直接接续的 tail batch 文件之一：
  - `runtime/company_assets/lovable/20260426T193540/harvest_profiles/harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json`
  - 最近一次检查该文件记录 `requested_url_count=73`、`status=completed`、`run_id=LLf6slcMDtP9WlKZf`、`dataset_id=Q95IGLwSSd9EEjoey`
- 调试期间临时启动过一次 live recovery 进程做验证；该进程在文档整理前已显式停止，本轮没有遗留仍在运行的恢复进程
- 本轮同时收口 Markdown 入口文档和活跃 tracker 的表述：
  - 新 session 默认应把仓库描述为招聘自动化、公开资料补全、候选人研究、provider 队列治理与后台恢复产品化
  - 减少与当前任务无关的高敏感表述，保留必须精确的 provider 名称、配置键、CLI flag 和 API contract
- 下一步仍值得继续的 runtime 工作：
  - 已完成本地 completion-event 编排收口：`submit next actor`、`ingest completed dataset`、`apply/materialize snapshot delta` 不再在 Harvest profile completed worker 消费路径上默认串行阻塞
  - 已补 `code_22` retryable backpressure 和 mixed-success batch 回归，证明只 retry failed/unresolved URL，不重跑整批
  - 后续如果继续压缩 tail latency，重点应转向 provider completion 的发现机制（poll cadence、callback handoff、可能的 webhook/long-poll），而不是再把 apply/materialize 放回上游 submit path
  - 在 bulk replace 稳住之后，再评估是否将 snapshot 同步继续收敛成真正的 delta materialization

## 2026-04-26 (Asia/Shanghai)

### Public Web target-candidate backend/frontend productization

- 完成目标候选人级 Public Web Search 的第一层后端产品化：
  - 新增 `target_candidate_public_web.py`，承接 target-candidate selected batch action、idempotency、run summary、DataForSEO-style checkpoint resume、person-level reuse asset upsert
  - 新增 PG-authoritative control-plane tables：`target_candidate_public_web_batches`、`target_candidate_public_web_runs`、`person_public_web_assets`
  - 新增 API：`POST /api/target-candidates/public-web-search`、`GET /api/target-candidates/public-web-search`
  - API trigger 只做幂等排队和 recoverable worker 创建，不在 HTTP request 内跑 live DataForSEO/fetch/LLM
  - 后台 worker 复用 `exploration_specialist` lane，metadata `recovery_kind=target_candidate_public_web_search`，初始 checkpoint 为 `waiting_remote_search`
  - per-candidate run 的 `search_checkpoint_json` 保存 query manifest、provider task id、poll count、query results、classified links 和 errors；未 ready 时后续 recovery takeover 会 poll/fetch 既有 task，不重复 submit
  - run 完成且存在 normalized LinkedIn URL key 时 upsert `person_public_web_assets`，对齐 LinkedIn profile 的 person-level reuse 模型
- 另一个 session 已继续接入第一层目标候选人页 UI，并在本 session 复核：
  - `TargetCandidatesPanel` 增加 checkbox selection、`Public Web Search` 批量按钮、状态刷新按钮、5s running-run polling、卡片级 run status/metrics/primary links 展示
  - 前端通过 `GET/POST /api/target-candidates/public-web-search` 消费后端 contract，不读 runtime 文件，不把 Public Web 结果写入 `primary_email`
  - `contracts/frontend_api_contract.ts` / `.schema.json` / `frontend_api_adapter.ts` 增加 Public Web Search typed contract
  - 该前端切片当时已从 compact run summary 继续接上 Public Web detail section；本轮后续已补齐 email/link promotion、Web Search promoted-only export 和对应 browser E2E
- 本轮继续完成 first-class Public Web signals / detail API 的后端切片：
  - 新增 PG-authoritative `person_public_web_signals`，并注册到 control-plane/live PG table registry；SQLite 仅作为既有 bootstrap/shadow，不作为 hosted truth source
  - run 完成后会按 `signals.json` 物化 email candidates 与 profile/public links，保存 source URL/domain/family、identity label、confidence、publishability、suppression reason、artifact refs、model provider/version
  - 新增 `GET /api/target-candidates/{record_id}/public-web-search`，按 target candidate record 返回 latest run、person asset summary、email candidates、profile links、grouped signals 和 evidence links
  - detail API 只返回 model-safe signal/evidence payload，不返回 `search_checkpoint`、raw HTML/PDF/search payload 或 raw document paths
  - 仍不把 Public Web email 写入 `target_candidates.primary_email`；promotion writer/API/UI 仍是后续独立工作
- 在进入 detail/export/promotion 前，补上 Public Web 质量评测层：
  - 新增 `src/sourcing_agent/public_web_quality.py`，可扫描实验产出的 `signals.json`，评估邮箱和 X/Substack/GitHub/Google Scholar 等 profile/media links 的 source URL、source family、identity label、publishability、promotion status 和 evidence presence
  - 新增 CLI：`evaluate-public-web-quality`，支持 `--experiment-dir`、`--output-dir`、`--summary-only`、`--fail-on-high-risk`，输出 JSON/CSV/Markdown 报告；其中 `--fail-on-high-risk` 用作高优先级质量门禁
  - Public Web 实验预算新增 `--max-ai-evidence-documents` / `PublicWebExperimentOptions.max_ai_evidence_documents`，默认 8；产品 API options 会按 1..20 归一化，便于正式接 UI 前跑更大候选人/query/fetch/LLM evidence 预算
  - `run_public_web_candidate_adjudication(...)` 的 AI result 现在记录发送给 LLM 的 email/link/fetched document/evidence slice 计数，方便复核预算是否真的生效
  - 用已有 replay 样本复核：4 candidates、13 signals、2 promotion-recommended emails，`cbfinn@cs.stanford.edu` 和 `dainves1@gmail.com` 均有 source URL，质量报告 0 issues
  - 用已有 live 10x10 social discovery 样本复核：10 candidates、378 profile link signals、0 trusted media links、648 medium issues；主要风险是 search-only X/Substack/GitHub/Scholar 结果多为 `unreviewed`、GitHub repo/deep link、X post/utility URL、Substack non-profile URL 或 same-name Scholar 噪声，证明正式 UI 不能把搜索结果直接当 confirmed profile
  - 用已有 live fetch+AI Scholar/academic summary 样本复核：3 candidates、94 signals、4 email candidates、21 trusted media links、180 medium issues；质量层发现旧 artifact 里仍有模型非规范 link type（如 `scholar_profile`/`github_profile`），评测器现在会归一化并标 `non_canonical_profile_link_type`
  - 跑了一轮真实更大 live quality pass：`live-public-web-quality-11x14-fetch12-ai16`，11 candidates、154 queries、880 entry links、131 fetched docs、12 email candidates、2 promotion-recommended emails；质量报告显示 94 trusted media links，但 12 fetches/candidate 的实际 fetch composition 被 discovered GitHub links 插队污染（118/131 fetched docs 是 GitHub，0 X/Substack），因此先修 fetch queue diversity，再决定是否升到 16 fetches
  - 修复 discovered GitHub front-insert：GitHub links 仍会作为 evidence candidates 保留，但不再前插抢占 Scholar/X/Substack/homepage/resume 的 fetch slots；discovered resume/CV 仍保持高优先级
  - 修复后跑 3-candidate live sanity：`live-public-web-quality-diversity-fix-3x14-fetch12-ai16`，33 fetched docs 分布为 homepage 5、resume 10、Scholar 4、GitHub 7、X 2、Substack 4、publication 3、academic profile 1；证明 12 fetches/candidate 在队列公平后可以覆盖 X/GitHub/Substack/Scholar 等主要渠道，下一轮无需先升到 16
  - 明确 LLM adjudication payload 不只依赖 fetched docs：现在会给候选人级 LLM 发送 source-balanced `entry_links` 与 `search_evidence`（DataForSEO URL/title/snippet/query/rank/provider context），避免 X/Substack 等难 fetch 平台只因没有 fetched document 就完全无法被模型判断
  - 重跑 3-candidate live LLM 对照：`live-public-web-quality-search-evidence-3x14-fetch12-ai16`。耗时约 20 分钟，其中 DataForSEO batch queue 约 5 分 15 秒，后续候选人按顺序 fetch/LLM 分析；第三个候选人因顺序 fetch 慢站点拖到约 20 分钟，总体证明 live 实验 CLI 慢主要来自 provider queue + sequential fetch/analysis，不是单次 LLM
  - `search_evidence` 后 X/Substack 判断分布改善：旧 run X=`confirmed 1 / unreviewed 25`、Substack=`unreviewed 44`；新 run X=`confirmed 1 / likely_same_person 8 / unreviewed 16`，Substack=`likely_same_person 6 / needs_review 1 / ambiguous_identity 8 / unreviewed 15`
  - 新质量报告显示 3 candidates、248 signals、8 email candidates、3 promotion-recommended emails、39 trusted media links、1 high-priority quality issue；该问题是 promotion-recommended email 缺 trusted identity，需要在 detail/promotion 前继续作为质量 gate
- 本轮继续补上 Public Web URL shape 与效率/前端 detail 收口：
  - 新增统一 `public_web_link_shape_warnings(...)` / `is_clean_profile_link(...)`，对 GitHub repo/deep link、X status/search/utility URL、Substack post/home feed/deep link、非 profile Scholar URL 产出显式 warning
  - LLM search evidence payload 现在携带 `link_shape_warnings` / `clean_profile_link`，prompt 明确这些 deep links 只能作为 evidence，不能当 clean profile
  - `summary.primary_links` 和 `person_public_web_signals.publishable` 现在同时要求 identity trusted 与 clean URL shape；detail API 会返回 top-level `link_shape_warnings` / `clean_profile_link`
  - 质量评测层复用同一 URL-shape helper；对既有 `live-public-web-quality-search-evidence-3x14-fetch12-ai16` 重新评估后，trusted media link 从旧口径 39 收紧为 9，X/Substack deep link 继续保留为 evidence/review signal
  - 实验/worker 分析支持 `max_concurrent_fetches_per_candidate`；实验 CLI 支持 `max_concurrent_candidate_analyses`，用于候选人级 LLM adjudication 并发。默认保持小并发（fetch=4、candidate analysis=2）以控制 provider/LLM 压力
  - 目标候选人页增加 Public Web detail section，按 record 调用 `GET /api/target-candidates/{record_id}/public-web-search`，展示 email candidates、profile/evidence links、identity label、publishability、suppression reason 和 URL shape warning chips；仍不提供 email promote 按钮
- 本轮继续完成 Public Web manual promotion 与专用导出包：
  - 新增 PG-authoritative `target_candidate_public_web_promotions`，保存 signal lineage、source URL/domain/family、identity/confidence、URL shape、operator、timestamp、previous/new value 和 promotion status
  - 新增 API：`GET/POST /api/target-candidates/{record_id}/public-web-promotions`；POST 默认只允许 publishable/clean signal promoted，email promotion 先写 promotion record，再更新 `target_candidates.primary_email`
  - Public Web detail API 现在会合并 latest promotion status，email/link 行可显示 `manually_promoted` / `manually_rejected`
  - 新增 `POST /api/target-candidates/public-web-export`，默认 `promoted_only`，导出 model-safe summary/signals/evidence links/promotions/manifest，并继续排除 raw HTML/PDF/search payload
  - 目标候选人页 detail section 增加 email/link promote/reject 控件；Public Web 专用导出按钮默认导出已选候选人，否则导出全量 target candidates 的人工确认公开信息
  - 新增 target-candidate Public Web browser E2E 脚本，覆盖 `/targets` selection -> trigger -> refresh/polling -> compact status card
  - 继续补一条 browser E2E，覆盖 completed Public Web detail 展开、人工确认邮箱和 clean profile link、promoted-only Public Web 导出下载；后端断言 promotion record 写入后 `primary_email` 才更新
  - 目标候选人页 toolbar/filter 做第一轮可用性优化：Public Web Search 与刷新状态合并到同一操作组，LinkedIn/Web Search 导出改为范围语义并用问号 tooltip 承载说明，新增关键词、跟进状态多选和 Public Web 状态多选筛选，隐藏容易误导的旧“查看历史记录”跳转按钮
- 本轮继续收口 Public Web 质量门禁、人工覆盖确认和导出模式产品控件：
  - `scripts/run_python_quality.sh all` 和 `pyproject.toml` 的 mypy target 现在显式纳入 `linkedin_url_normalization.py`、`public_web_quality.py`、`public_web_search.py`、`target_candidate_public_web.py` 及对应核心测试，避免 Public Web 新模块漂在默认门禁外
  - `run_pytest_matrix.py --mode changed` 对 Public Web 相关后端/测试/浏览器脚本路径会选中 `public-web-core`、`target-candidate-public-web-api`、`target-candidate-public-web-pg`，前端脚本改动也会拉起前端 contract/build suite
  - promotion API 支持对 non-publishable 或 dirty URL-shape signal 做人工覆盖确认，但必须传 `allow_unpublishable=true` 和 `override_reason`；无理由返回 `override_reason_required`，无效邮箱等 hard validation 仍不可覆盖
  - promotion detail、signal detail、promotion summary 和 `public_web_promotions.csv` 会保留 `override_reason`、原始 validation reason 和 `override_count`，覆盖路径仍先写 promotion record，再按 email promotion 更新 `primary_email`
  - 目标候选人页对需要覆盖的 email/link 显示 `覆盖确认邮箱/链接`，通过人工理由 prompt 写入 override metadata；默认 publishable/clean signal 仍走普通确认
  - Target-candidate Public Web Search 已接入 `workflow_event_response` 第一层 registry lane：shared recovery 发现 `waiting_remote_search` worker 后会做 job-scoped follow-up，不再完全依赖下一轮外层 daemon tick
  - 前端暂时隐藏尚未完全产品化的 `人工确认` / `含高置信` 导出模式控件，右侧导出容器只保留 `批量导出 LinkedIn Profile 信息` 和 `批量导出 Web Search 信息`
  - Web Search 导出按钮固定走 `promoted_and_publishable`：导出人工确认 signals 加 AI 判定 publishable 的未人工确认 signals；如果没有高置信个人主页/媒体链接，目标字段保持为空，仍排除 raw HTML/PDF/search payload
  - 浏览器 E2E 不再点击隐藏的 `含高置信` 控件；后端 API 测试已断言默认包不含未确认 GitHub link，而 `promoted_and_publishable` 包含 `ai_publishable_unpromoted` signal 并在 manifest 标记模式
- 本轮 closeout 文档已补齐新 session 恢复要点：
  - 本地 `8765` 上观察到的 `Public Web Search 接口暂不可用` 是旧后端进程导致的 404；新 session 应先重启当前分支 backend/worker 并验证 `GET /api/target-candidates/public-web-search?limit=5`
  - 当前本地 11 个 target candidates 尚未触发 Public Web Search，`未开始` 卡片状态是事实；Web Search 导出按钮里的 11 人只表示当前选择/筛选范围，不表示已有 11 人确认公开网络结果
  - 继续开发应先重启当前代码并小批量触发 2-3 个目标候选人的 Public Web Search，手动检查 detail、普通 promotion、override promotion、两种 Web Search export mode；代码层下一步再做 company-level API/CLI lane、scoped polling 和候选人卡片编辑/详情 UX
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/worker_daemon.py tests/test_target_candidate_public_web.py tests/test_control_plane_live_postgres.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web_api_queues_idempotent_runs'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_worker_recovery_daemon.py -k 'waiting_remote_search or recovery'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidates or public_web_api'`
  - `cd frontend-demo && npm run build`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback' tests/test_markdown_status.py`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m compileall -q src/sourcing_agent/storage.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py tests/test_target_candidate_public_web.py tests/test_results_api.py tests/test_control_plane_live_postgres.py`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/api.py src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py tests/test_target_candidate_public_web.py tests/test_results_api.py tests/test_control_plane_live_postgres.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'` -> `5 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed, 37 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `cd frontend-demo && npm run build`
  - inspected `runtime/public_web/experiments/replay-fetch-homepage-email-4-candidates/candidates/01_replay-chelsea-finn/signals.json` to confirm persisted signal fields match real email/link payloads
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_quality.py tests/test_public_web_search.py -k 'public_web_quality or email_extraction_suppresses_pdf_title_artifacts or publication_multi_email_keeps_candidate_local_part_and_suppresses_coauthors or ai_adjudication_marks_weak_identity_social_link_ambiguous or sanitizer or ai_evidence_document_budget'`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/replay-fetch-homepage-email-4-candidates --output-dir runtime/public_web/quality/replay-fetch-homepage-email-4-candidates --summary-only`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-dataforseo-entry-discovery-10x10-social-links --output-dir runtime/public_web/quality/live-dataforseo-entry-discovery-10x10-social-links --summary-only`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-dataforseo-fetch-scholar-academic-summary-3x7 --output-dir runtime/public_web/quality/live-dataforseo-fetch-scholar-academic-summary-3x7 --summary-only`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m compileall -q src/sourcing_agent/public_web_search.py src/sourcing_agent/public_web_quality.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/public_web_search.py src/sourcing_agent/public_web_quality.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py -k 'public_web or target_candidate_public_web or url_shape or bounded_concurrency'` -> `40 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-public-web-quality-search-evidence-3x14-fetch12-ai16 --output-dir runtime/public_web/quality/live-public-web-quality-search-evidence-3x14-fetch12-ai16-shape-warnings --summary-only`
  - `cd frontend-demo && npm run build`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'` -> `2 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed, 37 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `git diff --check`
  - `node --check frontend-demo/scripts/run_target_public_web_promotion_export_e2e.mjs`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/target_candidate_public_web.py tests/test_target_candidate_public_web.py tests/test_results_api.py tests/test_control_plane_live_postgres.py tests/test_frontend_browser_e2e.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api or promotion'` -> `7 passed, 45 deselected`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_frontend_browser_e2e.py -k 'target_candidate_public_web_selection_trigger_and_polling or target_candidate_public_web_promotion_and_export'` -> `2 passed, 7 deselected`
  - `cd frontend-demo && npm run build`
  - `bash ./scripts/run_python_quality.sh all`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_regression_matrix.py` -> `13 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --changed-path src/sourcing_agent/public_web_search.py --dry-run`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web or public_web_api or promotion'` -> `3 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_linkedin_url_normalization.py` -> `43 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed, 37 deselected`
- 剩余产品化边界已同步到 `docs/PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md`：
  - Company-level Public Web refresh 后续已落成 API/CLI-only v1；此处剩余为目标候选人页卡片编辑/备注/详情导航重做，以及更完整的人工手测/真实小批量 live run
- 新 session 交接文件：
  - `docs/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md`

### Harvest Mistral full-roster runtime root-cause hardening

- 复盘 `04/26 16:33 帮我找Mistral AI的全部成员` 暴露的根因：
  - 默认 small/unknown org full roster 预算是 `20 pages x 25 = 500`，原先 `company-employees` probe 只会把请求缩小到 provider 估算量，不会在 probe 发现 `1055 profiles total` 时从 500 扩到 1055
  - job 可以在 `profile_prefetch.status=queued`、`materialization_refresh_pending=true` 时进入候选人看板；这是 nonblocking board readiness 的设计，但 progress/summary 必须明确 profile hydration/materialization 仍在后台继续
  - background `harvest_profile_batch` 之前只有本地 submit worker 数，没有把 job-local active queued workers / registry queued URLs 当成强背压边界，容易在 recovery/reconcile 重复提交相近 batch，触发 Harvest `Too many queued requests (code_22)` 并放大成本
  - 新增 runtime tuning key 若不加入 `execution_preferences` 白名单，会被 `JobRequest` 归一化丢弃，造成“配置了限流但实际不生效”的同类治理缺口
- 已落地修复：
  - `HarvestCompanyEmployeesConnector` 的 probe sizing 现在会在估算总量高于默认请求时扩到 `min(estimated_total_count, provider_cap=2500)`，并同步放大 `takePages/maxItems/max_paid_items/timeout/charge cap`
  - `company-employees` summary / worker queue summary 现在记录 `requested_items_before_probe`、`effective_max_items`、`effective_take_pages`、`estimated_total_count`、`provider_cap_hit`、`requested_limit_would_truncate`、`partial_result` 和完整 `probe`
  - provider cap 命中时不再标成普通 completed：snapshot `stop_reason=provider_cap_reached`，summary `partial_result=true`
  - Harvest log parser 现在识别 `Max items limit reached: N`，用于审计“请求 cap 被打满”和 provider cap 的区别
  - `harvest_profile_batch_submit_global_inflight` 新增为默认 1 的全局提交背压；fast smoke 可升到 2，显式 execution preference / env 可覆盖
  - background profile prefetch 在 chunking 前过滤 registry 已经 `queued` 的 LinkedIn URL，只更新 source lineage，不再为这些 URL 再创建新 actor run
  - job-local active `harvest_profile_batch` worker 会消耗新 worker budget；超出预算的 URL 返回 `deferred_urls`，由后续 recovery/reconcile 继续排队
  - `harvest_profile_batch_submit_global_inflight` 及 Harvest/global/materialization in-flight key 已纳入 `execution_preferences` 白名单，避免 request normalization 静默丢配置
- 本轮继续把根因治理从进程内背压推进到 DB 级 actor/URL contract：
  - 新增 `runtime_provider_limiter_leases`，SQLite/PG 均支持 `acquire_runtime_provider_limiter_slot` / release；PG 使用 advisory transaction lock 保证同一 limiter key 下原子计数
  - 新增 `harvest_profile_actor_global_inflight`，默认 4，fast smoke 2，env `SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT` 可覆盖；profile-scraper actor lane 统一使用 limiter key `harvest_profile_scraper_actor`
  - background `harvest_profile_batch` 在提交前会先做 URL 级 registry lease claim；claim 失败只补 source lineage 和 `lease_contended_skip` event，不再重复 submit 同一 LinkedIn URL
  - background `harvest_profile_batch` 的 provider limiter lease 会写入 worker checkpoint；remote actor pending 时不释放，直到 recovery 完成/失败后释放，避免只限流本地 submit 而不控制真实 active actor
  - foreground `_fetch_harvest_profiles_for_urls`、company asset completion 的 live profile fetch、direct/segmented `company-employees` 均套入 DB 级 provider limiter；进程内 `runtime_inflight_slot` 仍保留为本进程二级保护
  - roster-heavy live profile fetch window 调保守：公司 roster 来源的大批量 URL 默认约 100-125/批，并发 1-2，避免 500 人清单被切成过多 60 人小 actor 同时排队
  - `parse_harvest_company_employee_rows` 会在 provider 只给 `publicIdentifier` 时提前构造 LinkedIn profile URL；候选人 metadata 也保留 `public_identifier/linkedin_url`，因此候选人看板可先显示“打开 LinkedIn”，不必等 profile detail materialization
  - job dashboard / candidate page 新增 `profile_fetch_progress`，按当前返回候选人的 LinkedIn URL 统计 `queued/fetched/failed_retryable/unrecoverable/missing/deferred`；前端候选人同步卡片展示 LinkedIn fetched/queued/retryable 概览
  - shared runtime tuning 继续扩展到 provider in-flight/backpressure 报告面：
    - `harvest_global_inflight_budget` 现在是 execution preference 白名单字段，可统一覆盖 Harvest 类 global in-flight budget，具体 lane 预算仍可用更细字段覆盖
    - 新增 `build_provider_backpressure_budget_report(...)`，统一输出 Harvest profile actor、profile batch submit、profile scrape、people search、company roster 的预算，以及 DB limiter active/wait/backlog/recommended action
    - scripted/simulate smoke per-case report 新增 `provider_backpressure`，aggregate summary 新增 provider limiter exhausted/backpressure rollup
  - workflow smoke benchmark 继续扩到同一报告口径：
    - per-case `workflow_benchmark` 现在同时包含 `search_returned_count`、`roster_returned_count`、`fetched_profile_count`、profile URL queued/total、board ready/nonempty、board total/first page returned 和 provider backpressure flags
    - aggregate `provider_case_report.workflow_benchmark.metrics` 会汇总 search/roster/profile/board-ready 数字，便于比较不同 strategy 的 workflow 行为，而不是只看 completed
- 复盘 `04/26 19:35 帮我找Lovable的全部成员` 暴露的新缺口并落地修复：
  - 实际 job `80efbad6aaec` 的 `linkedin-profile-search` 先返回 173 former seed，随后只提交了一个 58 URL profile-scraper batch；`company-employees` 之后返回 1008 visible entries，canonical `candidate_documents.json` 到 1155 candidates，但 profile registry 只有 `fetched=8 / queued=58`
  - root cause 1：`harvest_profile_batch` recovery 先看 registry，看到自己的 58 URL 已是 `queued` 后误判为“已有活跃队列”，没有用 worker checkpoint 的 `run_id=F8aV8...` / `dataset_id=atp1...` 继续 poll/download 已成功的 Apify dataset
  - root cause 2：Mistral 事故后把 background profile submit default 压到 1，导致 173 profile-search seed 这种中等批次也只排一个 actor；这属于把大 roster 成本背压错误套到 former/search-seed lane
  - root cause 3：provider 完成、dataset ingest、snapshot apply/materialize 三个阶段还没有完全拆开。即使 provider 侧 actor 已完成，本地也可能因为 recovery tick、慢 HTTP poll 或 apply/materialize 热点而延后下一轮 submit 与用户可见进度更新
  - 已修复：有同 worker checkpoint `run_id` 且 worker 未 completed 时，`_execute_harvest_profile_batch_worker` 会跳过 registry queued short-circuit，直接 resume remote run；完成后 release provider limiter、mark registry fetched、触发 existing inline/reconcile 路径
  - 已修复：completed `harvest_profile_batch` delta apply 后，会继续调用 baseline profile prefetch，把 deferred tail 继续排下去，而不是只 materialize 已完成的一个 batch
  - 已修复：profile-search/source-seed URL 现在通过 `seed_source_type/source_type/mode/account_id/source_path` 进入 source mix；live medium profile-search batches 默认按推荐窗口并行提交，173 URL 会拆成约 3 个 actor 同步排出；large company roster 仍保持保守窗口并继续受 DB limiter `harvest_profile_scraper_actor` 默认 4 保护
  - 仍未完全收口：对于已经 provider completed 的 profile-scraper run，本地还缺更强的 response-level / completion-level immediate handoff，导致“下一轮 submit”“本轮 dataset ingest”“下游 candidate detail materialization”之间仍可能出现不必要等待
  - `2026-04-26 20:40-20:56` 期间额外观察到的 `4 x 73` live Harvest profile-scraper actor 不是整批重跑：
    - 对 `runtime/company_assets/lovable/20260426T193540/harvest_profiles/harvest_profile_batch_*.queue_dataset_items.json` 做了 pairwise URL audit
    - `58 + 73 + 73 + 73 + 73` 五个 batch 共 `350` 个 unique LinkedIn URL，批次之间零重叠
    - 因此这些调用符合 deferred tail drain 预期，不是“同一个失败 batch 被完整重跑”
    - 当时 job summary 里的 `requested_url_count=1156 / deferred_url_count=798` 也与这个审计一致：`1156 - 8 cached - 350 fetched = 798 remaining deferred`
  - 前端 `progress` 映射修正：当 job status 已是 backend `completed` 但 `worker_summary.by_status` 仍有 `queued/running/waiting_remote_*` worker 时，UI 保持 `running` 语义并提示“结果已可浏览，后台仍在补全 LinkedIn profile 与候选人详情”，避免显示成完全 `results`
  - 测试缺口说明：原先 scripted/模拟覆盖了“queued URL 不应重复 submit”和 connector-level pending resume，但没有覆盖“worker 自己持有 checkpoint run_id + registry queued”这一组合状态；本轮新增真实 scripted provider pending->completed worker test 防止回退
- 这轮未跑新的 live Harvest 请求；Mistral 现有 artifact 结论仍是：canonical `candidate_documents.json` 已到 622 candidates，其中部分 profile detail 仍取决于 background prefetch/reconcile 尾巴是否完成。
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py src/sourcing_agent/enrichment.py src/sourcing_agent/acquisition.py src/sourcing_agent/runtime_tuning.py src/sourcing_agent/execution_preferences.py tests/test_harvest_connectors.py tests/test_enrichment.py tests/test_pipeline.py tests/test_runtime_tuning.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py tests/test_enrichment.py tests/test_runtime_tuning.py -k 'company_employees or company_checkpoint or parse_harvest_company_employee_run_log or queue_background_profile_prefetch or bounded_parallel_submit_workers or global_inflight_budget or fast_smoke or preserves_harvest_global'` -> `20 passed, 97 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'harvest_profile_batch or background_harvest_prefetch or full_roster_profile_prefetch or scoped_search_prefetch or harvest_company_roster or segmented_background_harvest'` -> `11 passed, 268 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/runtime_tuning.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/enrichment.py src/sourcing_agent/company_asset_completion.py src/sourcing_agent/acquisition.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/connectors.py src/sourcing_agent/profile_timeline.py src/sourcing_agent/orchestrator.py tests/test_runtime_tuning.py tests/test_storage_profile_registry.py tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_runtime_tuning.py tests/test_storage_profile_registry.py tests/test_harvest_connectors.py tests/test_enrichment.py tests/test_results_api.py -k 'runtime_provider_limiter or global_inflight_budget or fast_smoke or company_employee_rows or lease_blocks_duplicate_claims or roster_heavy or head_of_line or dashboard_is_summary_only'` -> `14 passed, 160 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_storage_profile_registry.py tests/test_runtime_tuning.py` -> `24 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'dashboard_is_summary_only or asset_population_compacts or profile_completion'` -> `5 passed, 43 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py -k 'harvest_profiles_for_urls or queue_background_profile_prefetch or roster_heavy or head_of_line'` -> `7 passed, 25 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py -k 'company_employee_rows or company_employees_probe or parse_harvest_company_employee_run_log'` -> `4 passed, 66 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/runtime_tuning.py src/sourcing_agent/execution_preferences.py src/sourcing_agent/workflow_smoke.py tests/test_runtime_tuning.py tests/test_workflow_smoke.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_runtime_tuning.py tests/test_workflow_smoke.py` -> `32 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/simulate_matrix_20260426_publicweb_harvest --seed-reference-runtime --fast-runtime --strict --timing-summary --report-json output/scripted_smoke_current/simulate_matrix_20260426_publicweb_harvest_report.json --summary-json output/scripted_smoke_current/simulate_matrix_20260426_publicweb_harvest_summary.json` -> `7 cases passed`; behavior guardrails all 0 for duplicate provider dispatch, unexpected Public Web Stage 2, prerequisite gaps, final-results board violations, and streaming materialization violations
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_long_tail_20260426_publicweb_harvest --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary --report-json output/scripted_smoke_current/google_long_tail_20260426_publicweb_harvest_report.json --summary-json output/scripted_smoke_current/google_long_tail_20260426_publicweb_harvest_summary.json` -> `1 case passed`; observed queued/waiting remote Harvest worker states and board-ready completion with behavior guardrails all 0
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/simulate_matrix_20260426_provider_benchmark --seed-reference-runtime --fast-runtime --strict --timing-summary --report-json output/scripted_smoke_current/simulate_matrix_20260426_provider_benchmark_report.json --summary-json output/scripted_smoke_current/simulate_matrix_20260426_provider_benchmark_summary.json` -> `7 cases passed`; new aggregate report includes `provider_backpressure` and `workflow_benchmark`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_long_tail_20260426_provider_benchmark --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary --report-json output/scripted_smoke_current/google_long_tail_20260426_provider_benchmark_report.json --summary-json output/scripted_smoke_current/google_long_tail_20260426_provider_benchmark_summary.json` -> `1 case passed`; `provider_backpressure.backpressure_case_count=1` from queued/waiting remote Harvest tail and board-ready completion remained healthy
  - `cd frontend-demo && npm run build`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/enrichment.py src/sourcing_agent/orchestrator.py src/sourcing_agent/runtime_tuning.py tests/test_enrichment.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py -k 'scripted_resume_polls_own_queued_registry_urls or resumes_remote_run_even_when_registry_urls_are_queued or profile_search_prefetch_defaults_to_parallel_actor_submission_for_medium_batches or queue_background_profile_prefetch_defers or bounded_parallel_submit_workers'` -> `5 passed, 30 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or reconcile_completed_workflow_after_background_harvest_prefetch_uses_shared_snapshot_apply_contract or scoped_search_prefetch_queues_all_known_profile_urls or full_roster_profile_prefetch_is_pending'` -> `4 passed, 275 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py -k 'profile_batch_execute_with_checkpoint_supports_scripted_pending_rounds'` -> `1 passed, 69 deselected`
  - `cd frontend-demo && npm run build`

## 2026-04-25 (Asia/Shanghai)

### Public Web target-candidate experiment: batch search and fetch validation

- 在新增 PG tables 前，先把候选人级 Public Web Search 方法打磨成 artifact-only 实验模块：
  - 新增 `public_web_search.py`，包含 source-family query planning、entry-link 分类/排序、DataForSEO batch/queue 执行、per-candidate status artifacts、内容 fetch、邮箱抽取、AI adjudication payload 与 signals summary
  - CLI 入口：`run-target-candidate-public-web-experiment`
  - 默认走 provider batch/queue；`--no-batch-search` 仅作为调试 fallback
  - `ai_extraction=auto` 现在不会在 entry-link-only 阶段调用 live LLM，避免搜索完成后被低价值 link-only adjudication 拖慢；有 fetched documents 或 email candidates 时再进入 AI review
  - unreviewed links 不再进入 `primary_links`，只有 `confirmed/likely_same_person` 才能成为 primary candidate links
  - classifier 已收紧：低价值目录/聚合页不再算个人主页或公司页，个人主页优先于 company text match 且需要更强姓名 URL/title 证据
  - query planner 已收紧首轮预算：10x4 配置现在跑 general identity、homepage with negative filters、company-constrained Scholar citations、company-constrained GitHub；name-only GitHub/Scholar 是后续 fallback，generic resume/CV 和 email/contact 已后置
  - `company_page` 现在只表示公司官方/公司域名页面，第三方文章/活动页仅提到公司名不会再归为 company page，且候选人级默认不 fetch company page
  - fetched homepage/GitHub/Scholar/publication pages 会产出二级 entry links；Scholar profile 现在抽取 homepage、verified email domain、affiliation、research interests
  - Google Scholar verified email domain 不再误提取成 `email@domain`；多邮箱论文会先压低非候选人 local-part 邮箱，避免把 coauthor email 直接 promotion
- 真实 DataForSEO batch entry discovery 验证：
  - run id: `live-dataforseo-batch-entry-discovery-10x4-ai-skip`
  - 10 candidates, 40 queries, 40 submitted tasks, 38 fetched task results, 0 task timeouts, 3 ready polls
  - wall time about 88s, earlier synchronous CLI pass was about 4min
  - corrected classifier recheck: 166 ranked links, including LinkedIn, Scholar, GitHub, X, Substack, personal homepage, resume/CV, academic profile, publication, company page
- Replay-style fetch/extraction 验证：
  - run id: `replay-fetch-homepage-email-4-candidates`
  - 4 candidates, 6 fetched documents, 2 extracted high-confidence email candidates
  - verified emails: `cbfinn@cs.stanford.edu` from Chelsea Finn Stanford homepage/CV, `dainves1@gmail.com` from Xiang Fu homepage `mailto:`
  - 邮箱仍只是 `promotion_recommended` candidates，不会写入 `primary_email`
- 已跑验证：
  - `PYTHONPATH=src .venv-tests/bin/python -m compileall -q src/sourcing_agent/public_web_search.py src/sourcing_agent/model_provider.py src/sourcing_agent/document_extraction.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_document_extraction.py tests/test_cli.py`
  - `.venv-tests/bin/ruff check src/sourcing_agent/public_web_search.py src/sourcing_agent/model_provider.py src/sourcing_agent/document_extraction.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_document_extraction.py tests/test_cli.py`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_document_extraction.py tests/test_cli.py` -> `49 passed`
- 详细产品化 TODO / empirical notes 已更新到 `docs/PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md`。

### SQLite product-surface retirement before Public Web

- 在 Public Web Search 产品化前完成 bounded storage cleanup：
  - 移除 `export-sqlite-snapshot` / `restore-sqlite-snapshot` CLI，以及 handoff bundle 的 SQLite 选项
  - `sqlite_snapshot` bundle kind 现在在 export/import/upload/download/restore 产品路径中显式拒绝，只能作为离线历史材料处理
  - `import-cloud-assets` 的 `control_plane_snapshot` 恢复现在要求 Postgres DSN，不能 fallback 到磁盘 SQLite
  - `show-control-plane-runtime` 输出改为 `control_plane_storage_banner`；non-PG runtime 是 error，不再是 legacy warning
  - profile-registry disk fallback、test-env seed disk fallback、hosted smoke seed disk fallback 已移除或改为 PG-only
  - 新 artifact 字段写 `control_plane_candidate_count/control_plane_evidence_count`，仅保留读取旧 `sqlite_*` 字段的兼容 fallback
- 新增/更新回归保护：
  - `tests/test_storage_surface_guardrails.py`
  - `tests/test_asset_sync.py`
  - `tests/test_cloud_asset_import.py`
  - `tests/test_object_storage.py`
  - `tests/test_cli.py`
- 已跑验证：
  - `./.venv-tests/bin/python -m compileall -q src scripts tests`
  - `./.venv-tests/bin/python -m pytest tests/test_asset_sync.py tests/test_cloud_asset_import.py tests/test_object_storage.py tests/test_cli.py tests/test_storage_surface_guardrails.py tests/test_candidate_artifacts.py tests/test_company_asset_completion.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_control_plane_postgres.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_control_plane_live_postgres.py tests/test_storage_profile_registry.py -q`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli show-control-plane-runtime`
  - `./.venv-tests/bin/python -m pytest -q`
  - 当前 full result：`1140 passed, 8 skipped, 25 subtests passed`

### Closeout validation, browser plan semantics, and documentation handoff

- 收掉最终 browser E2E 暴露的 plan 语义恢复问题：
  - `/api/plan/submit` 的异步 hydration 完成后，`frontend_history_links.metadata` 现在会持久化：
    - `request_preview`
    - `dispatch_preview`
    - `organization_execution_profile`
    - `asset_reuse_plan`
    - `lane_preview`
    - `effective_execution_semantics`
  - 前端 history recovery 会消费这些后端语义字段，不再把核心检索策略 / dispatch label 回退成 `待定` 或自行粗推断
  - 新组织 `full_company_roster` 的用户可见策略 label 统一为 `全量 live roster`，避免与已有本地资产的 `全量本地资产复用` 混淆
- 已完成 closeout 验证矩阵：
  - `./.venv-tests/bin/python -m pytest tests/test_execution_semantics.py tests/test_frontend_history_recovery.py -q`
  - `cd frontend-demo && npm run build`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q`
    - `5 passed, 2 skipped, 2 subtests passed`
  - `./.venv-tests/bin/python -m pytest tests/test_regression_matrix.py tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q`
  - `bash ./scripts/run_python_quality.sh typecheck`
  - `./.venv-tests/bin/python -m pytest tests/test_markdown_status.py -q`
  - isolated simulate matrix:
    - `scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/closeout_simulate_20260425 --seed-reference-runtime --fast-runtime --strict --timing-summary`
  - scripted Google long-tail:
    - `scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_long_tail_closeout_20260425 --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary`
  - 全仓：
    - `./.venv-tests/bin/python -m pytest -q`
    - 当前结果：`1135 passed, 8 skipped, 25 subtests passed`
- 文档 closeout 已开始：
  - 已确认 54 个一方 Markdown 均有 `> Status:` 头
  - 更新重点是 `AGENTS.md`、测试/运行/ECS 文档、active tracker、handoff，而不是机械改动每个历史 reference 文件
- GitHub handoff 已完成：
  - branch: `productization-2026-04-25-stable`
  - remote tracking: `origin/productization-2026-04-25-stable`
  - commits:
    - `ef1ea4c Stabilize productization workflow contracts`
    - `53d30f1 Add workspace development guardrails`
  - 已排除 runtime/cache/vendor/build output、generated `object_sync` 和 `frontend-demo/public/tml` 离线资产

### Productization pass: streaming/materialization reports, scripted scenarios, asset governance, Excel throughput

- 已完成 productization 第二批可执行契约：
  - Harvest / materialization：
    - `runtime_tuning.py` 新增 materialization coalescing window 与 streaming budget report
    - running job 的 inline incremental sync 现在会通过 shared `materialization_writer` in-flight slot
    - sync result 会记录 writer slot / streaming budget metadata
    - smoke provider case report 现在输出 `materialization_streaming`
    - aggregate smoke summary 会汇总 provider response count、pending delta count、provider response -> first materialization gap，并将 slow first materialization 作为 guardrail
  - scripted provider scenario：
    - 新增 scenario coverage validator
    - 新增 `configs/scripted/provider_behavior_matrix.json`
    - 覆盖 retryable 429、timeout、partial result、staged ready/fetch 四类行为
  - asset governance：
    - 新增 `asset_governance.py`
    - 建立 `(company_key, scope_kind, scope_key, asset_kind)` default pointer contract
    - canonical replacement plan 会生成新 default pointer、superseded predecessor 与 historical retention 摘要
    - 非 canonical lifecycle 不能被提升为默认指针
  - Excel intake：
    - intake summary 现在带 `throughput_plan`
    - 覆盖 company grouping、direct LinkedIn fetch batch、search-required count、row-level continuation、target candidates/export linkage gates
  - product journey regression matrix：
    - scripted provider / workflow smoke report / Excel intake / asset governance / product journey 已接入 high-signal test selection
- 已完成验证：
  - `py_compile` 覆盖本轮新增/修改的 backend modules
  - `tests/test_runtime_tuning.py`
  - `tests/test_scripted_provider_scenario.py`
  - `tests/test_asset_governance.py`
  - `tests/test_workflow_smoke.py -k 'provider_case_report or summarize_smoke_timings'`
  - `tests/test_excel_intake.py -k 'throughput_plan or local_exact_hit_and_local_manual_review'`
  - `tests/test_regression_matrix.py`
  - `tests/test_markdown_status.py`
  - repo-configured `typecheck`
  - hosted scripted Google long-tail timeout smoke sample

### Productization pass: persisted governance writers, Excel target/export actions, provider batch streaming

- 已完成 productization 第三批收尾：
  - asset governance 不再只是纯 helper：
    - 新增 `asset_default_pointers`
    - 新增 `asset_default_pointer_history`
    - storage writer 会持久化 canonical default pointer、superseded history 与 coverage proof
    - 新增 API：
      - `GET /api/assets/governance/default-pointers`
      - `POST /api/assets/governance/promote-default`
    - 新增 CLI：
      - `promote-asset-default-pointer`
  - Excel intake 的 target/export linkage 已从计划字段接成真实动作：
    - 新增 `POST /api/target-candidates/import-from-job`
    - 首页 Excel group 完成后可直接导入目标候选人
    - 首页 Excel group 完成后可直接导出 target candidates profile bundle
  - Harvest profile adapter 已支持 provider batch response callback：
    - `fetch_profiles_by_urls(..., on_batch_result=...)`
    - enrichment live fetch 会在单个 provider batch 返回后立即写 profile registry / fetched cache
  - materialization writer backpressure 继续收口：
    - `runtime_inflight_slot(...)` 支持同线程 reentrant，避免 outer sync + inner artifact write 在 budget=1 时死锁
    - `SnapshotMaterializer.synchronize_snapshot_candidate_documents(...)` 主入口也进入 shared `materialization_writer` budget
- 已完成验证：
  - `py_compile` 覆盖本轮 backend modules 与相关 tests
  - `tests/test_asset_governance.py`
  - `tests/test_runtime_tuning.py`
  - `tests/test_harvest_connectors.py`
  - `tests/test_results_api.py`
  - `tests/test_excel_intake.py`
  - `tests/test_regression_matrix.py`
  - 相关 `tests/test_pipeline.py` incremental sync / snapshot materializer 子集
  - frontend `npm run build`
  - repo-configured `typecheck`
  - 全仓 `pytest -q`：`1134 passed, 8 skipped, 25 subtests passed`

### Productization pass: runner shutdown, promoted aggregate proof, shared plan labels

- 已完成本轮 productization 第一批可执行契约：
  - runtime service 增加持久化 cooperative shutdown：
    - `services/<service_name>/stop_request.json`
    - `/api/runtime/services/shutdown`
    - `/api/jobs/{job_id}/cancel`
  - workflow cancel 现在会：
    - 将 workflow job 标记为 `cancelled`
    - retire 未完成 worker
    - release workflow lease
    - 请求 job-scoped recovery service 停止
  - multi-snapshot coverage 不再用 `source_snapshot_selection.mode` 当 proof：
    - `preferred_snapshot_subset` 只表示选择模式
    - 必须有显式 `coverage_proof` / promoted aggregate contract 才能解锁 multi-snapshot population-default reuse
  - 前后端检索策略 label 继续收口：
    - 后端 `compile_execution_semantics(...)` 输出 `execution_strategy_label`
    - 前端 plan 卡片优先展示后端语义 label，不再先自行推断
  - SQLite legacy surface 增加 CLI banner：
    - `show-control-plane-runtime` 会显式标记非 PG-only 为 legacy/emergency，并输出 migration exit
- 已完成验证：
  - targeted service daemon / pipeline cancel / planning / organization profile / execution semantics / CLI tests
  - frontend `npm run build`
- 本轮活跃 tracker：
  - `docs/SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md`

### Hosted smoke long-tail verification closure

- 收掉第二轮全仓 pytest 暴露的 hosted smoke 尾巴：
  - post-terminal nonblocking `harvest_profile_batch` 不再被 recovery follow-up 当成“可忽略尾巴”直接跳过；不阻塞主 job 完成，但仍会继续后台抓取并 reconcile profile detail
  - `workflow_smoke.py` 现在会在 job terminal 后继续 settle recoverable background workers，避免 smoke 把“主链完成”误判成“profile-tail 也已完成”
  - snapshot candidate documents 同步 / incremental apply 后统一失效对应 snapshot 的 asset-population 读缓存，避免候选人看板或 `/results?include_candidates=1` 读到旧 materialized payload
  - hosted smoke harness teardown 等待更多实际写 runtime 的后台线程，并对 cleanup 期间后台线程已删除子文件的 `ENOENT` 做安全处理，减少 rmtree race
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q`
  - `bash ./scripts/run_python_quality.sh typecheck`
  - `./.venv-tests/bin/python -m pytest -q`
  - 当前结果：`1105 passed, 8 skipped, 25 subtests passed`

## 2026-04-24

### Long-tail architecture hardening tracker

- 本轮剩余架构尾巴集中记录在：
  - `docs/SESSION_TRACKER_2026-04-24_LONG_TAIL.md`
- 每次 context compact / 会话恢复后，先读该 tracker、本文最新条目和 `docs/NEXT_TODO.md`，再执行命令，避免重复排查或把已修过的策略回退。
- 当前 long-tail checklist 已收口：
  - runner/recovery ownership 已由 hosted dispatch marker、recovery takeover guard、job lease release regression 固定
  - plan hydration 已升级为 normalized request-signature inflight dedupe，并记录 coalescing / queue wait metadata
  - runtime PG/schema bootstrap 已支持按 runtime namespace 解析 schema，isolated runtime 不再默认继承 production schema
  - Harvest provider calls 与 materialization writer 已并入 shared global in-flight budget / backpressure
  - asset governance/promotion 已加入 lifecycle gate，`draft/partial/superseded/archived/empty` 不可被自动 promotion 成 authoritative coverage baseline
  - cold materialization 已去掉一次冗余 per-candidate normalize，并为 profile/source JSON 建立 mtime/size 缓存与 selector index
- SQLite compatibility surface 已继续收紧：production runtime 默认要求 PG，只有显式 `SOURCING_ALLOW_PRODUCTION_SQLITE_CONTROL_PLANE=1` 才允许 emergency SQLite control plane
- `2026-04-26` 又补了一层收口，减少排障误导：
  - `show-control-plane-runtime` / runtime summary 现在明确区分 `default_db_path/settings_db_path` 只是 shadow seed path，不等于 live authoritative DB
  - store 新增 generic compatibility-shadow accessors，外围 import/watchdog/CLI 不再继续用 `getattr(... sqlite_shadow_connect_target ...)` 这一类兼容分支去猜测真实 store target
  - 文档统一改成“disk-backed live SQLite retired; ephemeral compatibility shadow remains”，避免把“SQLite 已退役”误读成 repo 内完全没有 shadow/compat 代码
- 当前验证状态与命令记录见 `docs/SESSION_TRACKER_2026-04-24_LONG_TAIL.md`。

### Excel intake relaunch gate 收尾

- 首页 `Excel Intake` 入口已重新打开：
  - 默认展示；需要灰度关闭时设置 `VITE_ENABLE_EXCEL_INTAKE_WORKFLOW=false`
  - `.env.development` / `.env.production` 现显式启用该入口
- 本地浏览器上传链路已收口：
  - Vite `dev` 与 `preview` 都支持 same-origin `/api/*` proxy
  - 前端 API client 对本地 same-origin HTML fallback 增加 `127.0.0.1:8765` / `localhost:8765` loopback retry
  - Excel workflow 继续使用真实 `FormData` 上传，后端转为 `file_content_base64` 后在云端解析，不再依赖用户本地路径
- 多公司 Excel 自动拆 job 已保留并增加真实浏览器回归：
  - `frontend-demo/scripts/run_excel_intake_e2e.mjs`
  - `tests/test_frontend_browser_e2e.py::FrontendBrowserFastE2ETest::test_browser_excel_intake_upload_splits_companies_via_same_origin`
  - 覆盖选中文件、提交 `/api/intake/excel/workflow`、按 company 拆成多个 group、首页展示拆分结果
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_excel_intake.py tests/test_frontend_history_recovery.py tests/test_api_json_serialization.py -q -k 'excel or Excel'`
  - `./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py`
  - `npm run build`
  - `./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q -k 'excel_intake'`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q -k 'excel_intake_upload'`

### Runtime environment namespace 隔离收口

- 已新增统一 runtime environment contract：
  - `src/sourcing_agent/runtime_environment.py`
  - `SOURCING_RUNTIME_ENVIRONMENT=production|local_dev|test|simulate|scripted|replay|ci`
  - provider mode 仍由 `SOURCING_EXTERNAL_PROVIDER_MODE` 表达，但不再单独承担环境隔离语义
- Harvest shared provider cache 已改成 live-only 且按 runtime namespace 分目录：
  - `runtime/provider_cache/<runtime_environment>/live/<logical_provider_name>/...`
  - `simulate/scripted/replay` 不再读写 shared Harvest provider cache
  - 旧版 `runtime/provider_cache/<logical_name>/...` 不再作为可复用 cache；遇到 `_offline` legacy body 会删除
- test/scripted/replay runtime 已阻断 repo-level PG 自动继承：
  - `scripts/dev_backend.sh` 在隔离 runtime 下先设置 `SOURCING_RUNTIME_ENVIRONMENT`
  - 默认写入 `<runtime_dir>/.isolated-local-postgres.env`
  - 除非显式提供 runtime 专属 `SOURCING_LOCAL_POSTGRES_ENV_FILE` 或设置 `SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV=1`
- hosted production entrypoint 已收紧：
  - `scripts/run_hosted_trial_backend.sh` 默认 `SOURCING_RUNTIME_ENVIRONMENT=production`
  - hosted 默认 provider mode 从 `replay` 改为 `live`
  - production 下 `replay/simulate/scripted` 需要显式 temporary override，否则启动失败
- `/api/runtime/health` 现在会返回当前 `runtime_environment / provider_mode / provider_cache_namespace / runtime_dir`，用于部署或手动测试前确认实际运行 namespace
- 文档已落到：
  - `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`
  - `docs/TEST_ENVIRONMENT.md`
  - `docs/ECS_PRELAUNCH_CHECKLIST.md`
  - `docs/ALIYUN_ECS_TRIAL_ROLLOUT.md`
  - `docs/PG_ONLY_CUTOVER_TRACKER.md`
  - `docs/NEXT_TODO.md`

### Perplexity full-roster 语义、hosted queued 卡住与 former broad-search 收口

- Meta `TBD` / ByteDance `Seedance` plan 复核：
  - 两者实际 `strategy_type` 都是 `scoped_search_roster`，都会走带关键词的 scoped current/former profile search
  - 前端 label 差异来自 baseline 状态：ByteDance 没 baseline；Meta 曾被 4 月 23 日 Excel intake 的 2 人快照误标成 authoritative baseline
  - 已新增大组织 coverage baseline gate：Excel/import/supplemental 小样本资产仍可作为候选人资产检索，但没有 shard / standard bundle / coverage proof 时，不再解锁 `prefer_delta_from_baseline` 或 `baseline_reuse_available`
  - 已在本地 PG 刷新 Meta execution profile：`baseline_candidate_count=2`、`coverage_baseline_reuse_ready=false`、`current_lane_default=keyword_search`
- Perplexity `Agent` 方向 plan 显示 `全量公司资产` 是当前预期：
  - Perplexity 本地没有 authoritative baseline，organization execution profile 走 `fallback_unknown_company`
  - fallback 默认 acquisition mode 是 `full_company_roster`
  - `Agent` 在这个模式下应作为后续本地 retrieval/rerank 关键词，而不是限制 current company roster API
- 已确认并修复 hosted workflow `queued` 卡住：
  - 根因是 `start_workflow(...)` 先启动 recovery daemons，再启动 hosted dispatch，recovery 可能抢先持有 job lease
  - queued hosted recovery 也曾在持有 job lease 时再启动 hosted thread，导致 thread 立即看到 `already_running` 后退出
  - 现在 hosted dispatch 先于 recovery daemon 启动；recovery 接管 queued hosted job 时不再持有外层 job lock
- 已收口 former lane 的 full-roster 语义：
  - 对 full-company roster / 小公司全量成员策略，former `linkedin-profile-search` 默认只传 `pastCompanies`
  - 不再把 `Agent` 这类方向词作为 `searchQuery` 限制 former 搜索
  - 只有 scoped / large-org keyword-only 策略才保留 keyword-limited former search
- 旧 Perplexity job `b22332dfcc6f` 是修复前启动的，但修复后已从 queued 恢复并完成：
  - worker 日志记录本次 Perplexity asset population 约 `525` candidates
  - `/api/jobs/{job_id}/results` 默认仍只返回轻量/检索结果视图，不等价于资产总人口数
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_seed_discovery.py -q -k 'former_paid_fallback_keyword_only_skips_blank_query or former_paid_fallback_full_roster_uses_broad_past_company_when_not_keyword_only or paid_fallback_all_queries_union_runs_all_harvest_queries'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'resume_queued_workflow_prefers_hosted_dispatch_when_requested or resume_queued_hosted_workflow_dispatches_without_holding_job_lock or start_workflow_dispatches_hosted_runner_before_recovery_daemons or start_hosted_workflow_thread_skips_duplicate_dispatch_with_fresh_marker or progress_auto_recovery_skips_fresh_hosted_dispatch_for_queued_job'`
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/seed_discovery.py tests/test_pipeline.py tests/test_seed_discovery.py`
  - 本地 backend / frontend 已重启并健康：`http://127.0.0.1:8765`、`http://localhost:4173`

### Monica probe/full duplicate 与 Perplexity plan 卡顿根因修复

- `harvestapi/linkedin-company-employees` 的 adaptive probe 现在会在“probe 已证明完整”时直接复用 probe dataset：
  - 条件是 probe completed、未触发 provider cap、`estimated_total_count > 0`、且 `returned_item_count >= estimated_total_count`
  - 同步 `fetch_company_roster(...)` 与 queued `execute_with_checkpoint(...)` 两条入口都已覆盖
  - 因此 Monica AI 这类 probe 只返回 8 个成员的 case，不再为了同一个 payload 再发第二次 full run
- Perplexity plan 9-13 分钟的根因不是 planner 本身：
  - 独立 `explain-workflow` 对 `Perplexity Agent` 实测约 `12.87s`
  - 本地 serve 进程里同时跑了 startup `organization_asset_warmup`
  - 该 warmup 默认无上限扫描/同步所有 runtime organization assets，长期占用一个 Python 线程和 GIL，拖慢 async plan hydration
- startup organization asset warmup 已改成显式 opt-in：
  - 默认 `STARTUP_ORGANIZATION_ASSET_WARMUP_ENABLED=false`
  - 需要冷启动预热时再手动开启，不能再作为每次本地/线上 serve 的默认后台任务
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_harvest_connectors.py -q -k 'complete_probe_dataset or probe_company_roster_query_records_probe_summary or multi_page_fetch_budget'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'startup_organization_asset_warmup_is_opt_in_by_default'`
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/harvest_connectors.py src/sourcing_agent/orchestrator.py tests/test_harvest_connectors.py tests/test_pipeline.py`

### Scripted workflow guardrails 当前轮收尾

- `workflow_smoke.py` 的 progress observability 已把计数语义拆清：
  - `result_count / observed_company_candidate_count` 是单调观测计数，回退仍视为 regression
  - `manual_review_count` 是待处理 backlog，profile 补全后下降是正常行为，现单独输出 `backlog_reductions`
  - synthetic terminal fallback 在缺少终态计数字段时仍会 carry forward，避免 terminal summary 把有效计数误清零
- 当前 scripted smoke 已能结构化验证这几类用户手动测试暴露过的问题：
  - duplicate provider dispatch
  - default-off `Public Web Stage 2` 是否被误打开
  - prerequisite-ready 到下游启动之间是否存在异常空转
  - `Final Results` 与候选人看板持久化是否一致
  - progress counter regression 与 manual-review backlog reduction 分口径统计
- 新一轮验证已完成：
  - `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py -q`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_google_long_tail_current_recheck4 --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary --report-json output/scripted_smoke_current/google_long_tail_recheck4_report.json --summary-json output/scripted_smoke_current/google_long_tail_recheck4_summary.json`
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q -k 'hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover or hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider or hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch or hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch or hosted_scripted_long_tail_accepts_request_scoped_fast_smoke_profile_without_server_env'`
  - `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py tests/test_harvest_connectors.py tests/test_seed_discovery.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or reconcile_completed_workflow_after_background_company_roster or reconcile_completed_workflow_after_background_harvest_prefetch or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline'`
- Google long-tail scripted strict recheck 当前结果：
  - total 约 `5502.19ms`
  - `progress_regression_case_count=0`
  - duplicate dispatch / disabled stage / prerequisite gap / final-results board violation 均为 `0`

### Search-seed profile prefetch 已前推到单 query 返回粒度

- `src/sourcing_agent/worker_daemon.py` 这轮把 worker result 消费从“按提交顺序阻塞 `future.result()`”改成了“按实际完成顺序消费”：
  - `AutonomousWorkerDaemon.run(...)` 现在会按 completed order 处理 worker result
  - 同时支持 `result_callback`
  - 这让上层不必再等最慢的那条 query，才能看到更快 query 的增量结果
- `src/sourcing_agent/seed_discovery.py` 现在新增了 `on_incremental_query_result` contract：
  - `discover(...)` 会在单个 query spec 返回 usable entries 后立即回调
  - `_provider_people_search_fallback(...)` 也同步接入，不再只在 paid fallback 全部完成后才统一吐结果
  - 当前 `web_search` / `harvest_profile_search` 两条 search-seed 入口都已经具备 query-level incremental emit
- `src/sourcing_agent/acquisition.py` 也不再把 search-seed prefetch 分成“incremental 临时分支 + final snapshot 另一套逻辑”：
  - 新增 shared helper，把 search-seed entries 统一转成 prefetch candidates
  - acquisition 侧会按 job-local seen URL 去重
  - overlapping query results 不会在同一 job 里重复派发相同的 profile prefetch
  - final snapshot prefetch 会和前面 incremental dispatch 汇总成同一份 summary，而不是把“前面已派发过”覆盖成一个误导性的 skipped 结果
- 这轮解决的是：
  - search-seed 不再必须等整轮 discover 结束，才第一次启动 profile prefetch
  - overlap query 不再在同一 job 内二次派发同一个 LinkedIn profile URL
  - worker daemon completion order 不再被最慢 worker 拖成伪串行
- 这轮还没有结束的点：
  - 还没把 `linkedin-company-employees` / `linkedin-profile-search` 的 provider response 也统一收成同一套 request-level incremental ingest
  - 也还没把 scraper batch 返回后的 delta materialization / debounce writer 收到最终形态
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_worker_daemon.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_seed_discovery.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store'`
  - 当前均已转绿

### Company-roster background output 已并入 shared snapshot apply / reconcile contract

- `src/sourcing_agent/snapshot_materializer.py` 这轮新增了 `apply_company_roster_workers_to_snapshot(...)`：
  - `harvest_company_employees` completed worker 现在不再只是“等下次整轮恢复时再重抓”
  - 会直接把 worker/shard 的 queue dataset materialize 成 worker snapshot
  - 再把它增量 merge 回 root snapshot 的：
    - `harvest_company_employees_merged.json`
    - `harvest_company_employees_visible.json`
    - `harvest_company_employees_headless.json`
    - `harvest_company_employees_summary.json`
  - 同时也会把 root `candidate_documents.json` 一并增量更新
- `src/sourcing_agent/orchestrator.py` 现已把这条 contract 接到两条主路径：
  - `pre_retrieval_refresh`
  - `completed-job background reconcile`
  - running/completed 两条路径不再对 `company_roster` 各长一套独立逻辑
- `src/sourcing_agent/workflow_refresh.py` 这轮也补了 shared classification / metrics：
  - `worker_has_completed_background_company_roster_output(...)`
  - `resolve_reconcile_snapshot_dir(...)` / `resolve_reconcile_snapshot_id(...)` 现在优先认 `root_snapshot_dir`
  - segmented shard worker 不会再错误把 shard dir 当成最终 root snapshot 去 reconcile
  - refresh metrics 现新增：
    - `inline_company_roster_worker_count`
    - `background_company_roster_reconcile_count`
- partial segmented roster 这轮也收了 completion contract：
  - `_restore_segmented_roster_snapshot_from_snapshot_dir(...)` 不再要求 all-shards-ready 才第一次恢复
  - 已完成 shard 现在可以先恢复成 partial root snapshot，供当前 workflow 继续主链
  - 但 `src/sourcing_agent/acquisition.py::_load_cached_roster_snapshot(...)` 会显式跳过 `completion_status != completed` 的 partial roster
  - 这样 partial current roster 可以服务当前 job，但不会误被后续新 job 当成“可直接复用的完整 cached roster”
- 这轮解决的是：
  - `linkedin-company-employees` completed background worker 不再游离在 search-seed 之外
  - running/completed 两种 reconcile 都共享同一套 roster apply contract
  - segmented roster 可以 partial restore，但不会错误解锁 cached reuse
- 这轮还没结束的点：
  - `linkedin-company-employees` live provider response 仍未做到“单个 provider request 返回就立即 inline ingest”，当前收口点还是 completed worker/shard output
  - scraper batch 返回后的 delta materialization / debounce writer 仍未收到终态
- 已完成验证：
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_refresh.py src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/orchestrator.py src/sourcing_agent/acquisition.py`
  - `./.venv-tests/bin/python -m pytest tests/test_worker_daemon.py tests/test_seed_discovery.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or reconcile_completed_workflow_after_background_company_roster or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline or restore_roster_snapshot_rehydrates_segmented_harvest_queue_shards or restore_roster_snapshot_rehydrates_partial_segmented_harvest_queue_shards_without_cached_reuse'`
  - 当前均已转绿

### Harvest/company-roster worker completion 现已走统一的 inline incremental ingest 主路径

- `src/sourcing_agent/snapshot_materializer.py` 这轮补上了 `apply_harvest_profile_workers_to_snapshot(...)`：
  - completed `harvest_profile_batch` worker 不再只靠后续 `CompanyAssetCompletionManager` 全量补课
  - 现在会直接把已持久化的 raw LinkedIn profile payload delta-merge 回 root `candidate_documents.json`
  - 复用现有 profile match / membership review / non-member apply helper，不再额外长一套 harvest-only merge 逻辑
- `src/sourcing_agent/orchestrator.py` 现已把 running/completed 两条路都收口到同一 helper：
  - running job:
    - worker recovery completion callback 会立即 apply company-roster / harvest-prefetch 输出
    - company-roster apply 后会立刻继续 queue baseline profile prefetch
    - full sync/materialize 现已收成 same-kind micro-batch + per-job single-writer：
      - 同 job、同 snapshot、同 kind 的多个 completed worker 会先合并成一个 apply batch
      - 同 kind worker 仍在 in-flight -> 只做 delta apply，先不重跑 full sync
      - 同 kind worker drain 完 -> 只触发一次 `_synchronize_snapshot_candidate_documents(...)`
  - completed job:
    - `_reconcile_completed_workflow_after_harvest_prefetch(...)` 现改为先走 shared harvest delta apply
    - 不再回退到 `CompanyAssetCompletionManager.complete_snapshot_profiles(...)` 作为另一套 reconcile 主路径
- `src/sourcing_agent/acquisition.py` 这轮把 in-process segmented roster 也接到了同一 callback：
  - 当 `ThreadPoolExecutor` shard 中已有 completed local shard、但整轮 segmented roster 仍需 background resume 时
  - 会立即把这批 completed shard 通过同一 inline reconcile callback merge 回 root snapshot
  - 不再等到 recovery daemon 下一拍才第一次看见这些已完成 shard
- 为了避免“callback 已吃掉 worker，但 refresh/reconcile 又二次吃同一 worker”：
  - completed background worker 现在会写 `inline_incremental_ingest` marker
  - `worker_has_completed_background_company_roster_output(...)` / `worker_has_completed_background_harvest_prefetch(...)` 会跳过已 inline-consumed worker
- 这轮实际收掉的是：
  - `linkedin-company-employees` background worker completion -> root snapshot apply -> downstream prefetch/sync
  - `harvest_profile_batch` completion -> delta merge -> same-kind micro-batch sync/materialize
  - in-process segmented `harvest_company_employees` completion -> same inline callback / apply contract
  - completed reconcile 不再保留 harvest-prefetch 的另一套 legacy completion-manager 双轨
- 还没继续做的更深一层优化：
  - 现在剩下的不是“同步 shard 有没有接入 callback”，而是更细的 runtime tuning：
    - 还没继续做 time-window coalescing
    - 还没把 per-job single-writer 再提升成更完整的 global in-flight budget / backpressure contract
- 已完成验证：
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/orchestrator.py src/sourcing_agent/worker_daemon.py src/sourcing_agent/workflow_refresh.py`
  - `./.venv-tests/bin/python -m pytest tests/test_worker_daemon.py tests/test_worker_recovery_daemon.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or reconcile_completed_workflow_after_background_harvest_prefetch_uses_shared_snapshot_apply_contract or execute_segmented_harvest_company_roster_workers_emits_inline_callback_for_completed_local_shards or micro_batches_completed_harvest_prefetch_workers_and_syncs_once'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or reconcile_completed_workflow_after_background_company_roster or reconcile_completed_workflow_after_background_harvest_prefetch or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline'`
  - 当前均已转绿

### Full-roster blocked resume 已允许基于 partial baseline 提前继续

- `src/sourcing_agent/orchestrator.py` 这轮收掉了一个仍然存在的阶段级 barrier：
  - 之前 `acquire_full_roster` 只要还有 `acquisition_specialist / harvest_company_employees` worker 在跑，就会继续保持 `blocked`
  - 即使 former/search-seed baseline 已经足够进入 `enrich_linkedin_profiles`，workflow 也会被错误卡住
- 这轮又把同一条 contract 前推到了 `src/sourcing_agent/acquisition.py`：
  - `acquire_full_roster` 在 current-roster Harvest 已 queued、但 former/search-seed baseline 已到位时，不再先返回 `blocked`
  - 现在会直接以 partial baseline 继续主链，并提前派发 background profile prefetch
  - 这样不必再等 recovery tick 才从 blocked 状态被重新拉起
- 同时也把原先不对称的反向分支收掉了：
  - 如果 current roster 已经 ready，但 former/search-seed 还在后台 worker
  - `acquire_full_roster` 现在也会继续主链，而不是继续 `blocked`
  - current/former 两侧改为共用同一个 full-roster baseline continuation helper，不再各长一套局部分支
- 现在的 contract 改为：
  - 只要 `acquire_full_roster` 已经具备可用 baseline
  - 无论剩余后台 worker 是 `harvest_company_employees` 还是 former/search-seed
  - workflow 就允许继续 resume 到 enrichment / normalize / retrieval
  - current-roster Harvest worker 留在后台继续补 current lane，而不是阻塞主链
- 这意味着：
  - `LinkedIn Stage 1 completed` 不再必须等待所有 current roster worker terminal
  - full-roster job 可以更早进入 preview / board-ready 路径
  - readiness 与 checkpoint reuse 的 contract 现在更一致，不再出现“checkpoint 已够复用，但 readiness 还在机械等待”的分叉
  - former/search-seed baseline 上的 LinkedIn profile prefetch 也能更早开始，而不是只能等 `enrich_linkedin_profiles`
- search-seed acquisition / recovery 这轮也继续前推到更细粒度：
  - `acquire_search_seed_pool(...)` 一旦拿到带 `profile_url` 的新 entries，就会立刻派发 background profile prefetch
  - `background_search_seed_reconcile`
  - `pre_retrieval_refresh`
  - 这两条补偿路径现在也会在 merge `search_seed_snapshot` 后立刻触发同一套 prefetch，而不是只等后续 enrichment stage
- 同时把旧测试契约对齐到当前 default-off stage 设计：
  - `tests/test_pipeline.py` 不再假设 `enrich_public_web_signals` 在默认 plan 中一定存在
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'resume_blocked_workflow_uses_search_seed_baseline_with_noncritical_pending_workers or acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists or enrich_profiles_writes_baseline_when_full_roster_profile_prefetch_is_pending or restore_completed_workflow_stage_summary_contract_keeps_latest_completed_at or scoped_search_prefetch_queues_all_known_profile_urls or execute_retrieval_skips_ranked_scoring_for_asset_population_default_view'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'resume_blocked_workflow_after_workers_complete or resume_blocked_workflow_ignores_pending_exploration_workers or job_progress_classifies_blocked_acquisition_workers or get_job_progress_triggers_auto_recovery_when_blocked_on_acquisition_workers or acquire_full_roster_blocks_when_background_harvest_is_pending'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_blocks_when_background_harvest_is_pending or acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists or resume_blocked_workflow_uses_search_seed_baseline_with_noncritical_pending_workers or enrich_profiles_writes_baseline_when_full_roster_profile_prefetch_is_pending'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_queues_profile_prefetch_immediately_for_recovered_entries or acquire_search_seed_pool_allows_baseline_when_background_queue_is_pending or acquire_search_seed_pool_still_blocks_when_background_queue_has_no_entries or reconcile_completed_workflow_after_background_search_seed or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_full_roster or acquire_search_seed_pool or reconcile_completed_workflow_after_background_search_seed or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or resume_blocked_workflow_uses_search_seed_baseline_with_noncritical_pending_workers or acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists'`
  - 当前均已转绿

### Workflow behavior guardrails 已接入 scripted smoke report

- scripted provider 路径现会把 dispatch invocation 写入隔离 runtime：
  - `src/sourcing_agent/scripted_provider_scenario.py`
  - `src/sourcing_agent/search_provider.py`
  - `src/sourcing_agent/harvest_connectors.py`
- `src/sourcing_agent/workflow_smoke.py` 现在会在 per-case `provider_case_report` 和顶层 export 中同时输出：
  - `behavior_guardrails.duplicate_provider_dispatch`
  - `behavior_guardrails.disabled_stage_violations`
  - `behavior_guardrails.prerequisite_gaps`
  - `behavior_guardrails.final_results_board_consistency`
- 这意味着 scripted smoke 不再只看 wall-clock，而能直接回答：
  - 同一 payload 有没有被重复 dispatch
  - `Public Web Stage 2` 这种 default-off stage 有没有被误打开
  - `LinkedIn Stage 1 -> Stage 1 Preview -> stage_2_final` 之间有没有不合理空转
  - `Final Results` 之后 board 是否真正 non-empty
- 汇总层 `summarize_smoke_timings(...)` 也新增了 aggregate summary：
  - duplicate dispatch case/signature/redundant dispatch counts
  - unexpected public web stage case count
  - prerequisite gap p95/min/max
  - final-results-vs-board violation case count
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest -q tests/test_workflow_smoke.py`
  - `./.venv-tests/bin/python -m pytest -q tests/test_hosted_workflow_smoke.py -k 'large_org_full_roster_overflow_completes_without_live_provider'`
  - `./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir <tmp> --provider-mode scripted --scripted-scenario configs/scripted/large_org_full_roster_overflow.json --fast-runtime --case xai_full_roster --strict --timing-summary`
  - 实测这条 `xai_full_roster` scripted smoke 的 `behavior_guardrails` 为全绿：
    - duplicate dispatch `0`
    - unexpected public web stage `0`
    - prerequisite gaps `0ms`
    - final-results / board inconsistency `0`

### Public Web Stage 2 已改为 default-off，不再阻塞候选人看板

- workflow submission 默认 `analysis_stage_mode=single_stage`：
  - `POST /api/workflows` 不再默认把 job 送进 `two_stage`
  - 默认链路现在是 `LinkedIn Stage 1 -> Stage 1 Preview -> normalize/materialize -> final results`
- plan 默认不再加入 `enrich_public_web_signals`：
  - 只有显式 `analysis_stage_mode=two_stage` 的请求才会把 `Public Web Stage 2` 放进 acquisition plan
  - 因此默认 workflow 不会再被一个当前几乎无增量价值的 stage2 acquisition 卡住
- `Stage 1 preview` 的 summary / event 文案也已对齐：
  - default path 改为 `Continuing snapshot materialization`
  - 只有显式启用 two-stage 的 case 才会显示 `Continuing Public Web Stage 2 acquisition`
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest -q` 跑了这条变更对应的高信号子集
  - 覆盖了：
    - plan default-off / explicit two-stage opt-in
    - single-stage workflow 不再落 `public_web_stage_2`
    - two-stage 显式启用时旧链路仍可继续工作
    - `/api/workflows` 默认 submission contract 改为 `single_stage`

### `/api/plan` 已拆成 fast submit + async plan hydration

- 这轮先把“进入检索方案页”从同步 `explain_workflow()` 长尾里剥离出来：
  - 新增 `POST /api/plan/submit`
  - 前端不再阻塞等待完整 plan/explain 才切路由
  - 现在会先创建 `history_id`、立即进入 `phase=plan` 的 loading 态，再通过 frontend-history recovery 异步补全 plan
- 后端新增了按 `history_id` 去重的 plan hydration in-flight state：
  - `src/sourcing_agent/orchestrator.py`
    - `submit_plan_workflow(...)`
    - `_queue_plan_hydration(...)`
    - `_run_plan_hydration(...)`
  - `frontend_history_link.metadata.plan_generation` 现在显式记录：
    - `queued`
    - `running`
    - `completed`
    - `failed`
    - 以及 request id / timing / error message
- `/api/frontend-history/{history_id}` recovery 也补上了显式 `error_message`，前端可以直接区分：
  - “还在生成 plan”
  - “plan 已生成可展示”
  - “plan generation failed”
- 前端 `SearchPage` 已接到新 contract：
  - submit / revision 现在走 fast submit
  - 方案页允许 `plan=null` 的 loading state
  - history hydration 对 `phase=plan && plan missing` 不再死循环 recover，而是走定时 hydration polling
  - 历史侧栏在这类记录上显示“检索方案生成中”，不再直接显示“未生成方案”
- API lane 也一起收了：
  - `/api/plan/submit` 被标为 `light` lane
  - 避免它继续和重 POST 共用 shared lane，导致轻量提交也被排队拖慢
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_frontend_history_recovery.py tests/test_api_json_serialization.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'http_api_light_routes_are_classified_for_reserved_lane'`
  - `cd frontend-demo && npm run build`
  - 当前均已转绿

### Remaining note after fast submit split

- 这轮收掉的是“HTTP submit/route transition 长尾”，不是 `build_plan` 本身的纯计算成本：
  - `llm_normalize_request`
  - `_build_augmented_sourcing_plan`
  - `_build_execution_bundle`
  - 这些仍在后台 hydration 线程里执行
- 因此用户进入方案页已可接近秒级，但完整 plan detail 的最终 ready 时间，仍受 planner/LLM 本身耗时影响。

### 2026-04-23 PM tracker closure pass

- `docs/SESSION_TRACKER_2026-04-23_PM.md` 现已正式关闭：
  - immediate stabilization tails are no longer tracked as an open session backlog
  - long-horizon follow-ups were promoted into `docs/NEXT_TODO.md`
- authoritative snapshot board source has been re-aligned:
  - `src/sourcing_agent/orchestrator.py` now ignores non-patch job asset-population overlays when the candidate source is an authoritative snapshot baseline
  - this fixes the class where `/api/jobs/{job_id}/dashboard` and `/api/jobs/{job_id}/candidates?offset=0` exposed different first-page prefixes for the same authoritative asset
- frontend board hydration/recovery contract is now more stable:
  - `frontend-demo/src/lib/api.ts`
    - candidate-page merge is now offset-aware
    - incomplete dashboard summaries proactively backfill page 0 before continuing hydration
- smoke/stage digest contract was tightened:
  - `src/sourcing_agent/workflow_smoke.py` now synthesizes missing canonical provider stages such as `public_web_stage_2` from later completed stages
  - hosted smoke assertions no longer flap on reuse cases where the provider stage was effectively skipped but finalization already completed
- regression coverage added:
  - `tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_authoritative_snapshot_candidate_page_ignores_non_patch_overlay_prefix_drift`
  - `tests/test_workflow_smoke.py::WorkflowSmokeTest::test_stage_summary_digest_synthesizes_missing_public_web_stage_from_final_stage`
- verified in this pass:
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q -k 'authoritative_snapshot_candidate_page_ignores_non_patch_overlay_prefix_drift or large_org_full_roster_existing_baseline_defaults_to_asset_population'`
  - `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py tests/test_hosted_workflow_smoke.py tests/test_scripted_test_runtime.py -q`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q`
  - current result: green (`4 passed, 2 skipped` for browser E2E; relevant smoke/runtime suites green)

### Background prefetch submit 与 backend stage timing 已做 fresh smoke 复核

- `execution_preferences.py` 这轮把 runtime tuning 的数值型 override 也保留下来，避免：
  - request normalize 后把 `harvest_prefetch_submit_workers`
  - `parallel_search_workers`
  - `parallel_exploration_workers`
  - `candidate_artifact_max_workers`
  - 这类 runtime knobs 静默丢失
- `enrichment.py` 的 background Harvest prefetch submit 现已与 foreground hydration 基本对齐：
  - chunk submit 不再全串行
  - 会基于 URL/source-shard 上下文解默认 window
  - 再通过 shared runtime tuning resolver 做 cap/override
- `orchestrator.py` / `workflow_smoke.py` 这轮也补了 backend stage timestamp repair：
  - `stage_1_preview` nested payload 会带 `started_at` / `completed_at`
  - `linkedin_stage_1` 缺失 `started_at` 时会从既有 summary 或 job `created_at` 回填
  - smoke 汇总现在优先消费 backend authoritative timestamps，而不是先掉回 client timeline
- 已用隔离 runtime 重跑 fresh smoke，并落盘到 `output/scripted_smoke_v4/`：
  - `openai_reuse`
    - `reuse_snapshot`
    - total 约 `1505.78ms`
    - `job_to_board_nonempty` 约 `1020.23ms`
  - `google_scoped_cold`
    - `delta_from_snapshot`
    - total 约 `5144.26ms`
    - `job_to_board_nonempty` 约 `1076.37ms`
  - `xai_live_roster`
    - `new_job`
    - total 约 `18825.6ms`
    - `job_to_board_nonempty` 约 `15120.17ms`
    - progress 峰值：
      - `queued_worker_count=3`
      - `waiting_remote_harvest_count=3`
      - `pending_worker_count=3`
- fresh smoke 结论：
  - `provider_case_report.stage_wall_clock_ms` 不再回到空字典
  - `provider_case_report.workflow_wall_clock_ms` 在 reuse / delta / live-roster 三类代表性 case 都已产出
  - backend stage timing 修复已经体现在标准 smoke report，而不只是单测
- 尾巴已继续收掉：
  - per-case 顶层 `stage_summary_digest / stage_wall_clock_ms / workflow_wall_clock_ms` 现已与 nested `provider_case_report` 对齐导出
  - synthetic terminal sample 现会保留 monotonic counters，不再把 completed case 的 `manual_review_count` / `result_count` 打回 `0`
- 已补并通过回归：
  - `tests/test_workflow_smoke.py::WorkflowSmokeTest::test_synthetic_terminal_sample_preserves_monotonic_counts`
  - `tests/test_workflow_smoke.py::WorkflowSmokeTest::test_build_case_level_smoke_exports_surfaces_stage_digest_and_timings`
  - fresh recheck: `output/scripted_smoke_v4_recheck/openai_reuse/report.json`

### Shared runtime tuning 继续收口到 scheduler lane budget，并补上 workflow wall-clock smoke report

- `runtime_tuning.py` 这轮继续向上收口：
  - 不再只覆盖 provider query / roster shard / candidate artifact 并发
  - 同一套 profile/override resolver 现在也覆盖：
    - `parallel_search_workers`
    - `parallel_exploration_workers`
    - `search_worker_unit_budget`
    - `public_media_worker_unit_budget`
    - `exploration_worker_unit_budget`
- `worker_scheduler.py` 不再直接机械读 `cost_policy.parallel_*`：
  - 会先从 plan / task intent view 的 `execution_preferences.runtime_tuning_profile` 抽 shared runtime context
  - 再统一解析 lane limits / lane budget caps
  - 这样 `PersistentWorkerRecoveryDaemon`、scheduler summary、exploration worker daemon 不再各自长一套并发预算分叉
- `enrichment.py` / `exploratory_enrichment.py` 也继续并到同一 contract：
  - public-web exploration worker 数不再由 `acquisition.py` 直接读 `cost_policy`
  - `MultiSourceEnricher` 现按 request runtime tuning + cost policy 统一解出 exploration parallelism
  - `ExploratoryWebEnricher` 的 daemon plan 也会带上真实 plan/request runtime context，而不是临时拼一个只含 `parallel_exploration_workers` 的简化 plan
- `workflow_smoke.py` 的 provider-grade report 继续升级：
  - 新增 `workflow_wall_clock_ms`
  - 优先使用 backend stage summary 的时间戳
  - 若 backend stage summary 缺失/不稳定，则回退到 client-observed timeline + smoke timings 估算：
    - `job_to_stage_1_preview`
    - `job_to_final_results`
    - `stage_1_preview_to_final_results`
    - `final_results_to_board_ready`
    - `job_to_board_ready`
    - `job_to_board_nonempty`
  - aggregate summary 现也带：
    - `workflow_wall_clock_ms`
    - `strategy_rollups`
      - `effective_acquisition_mode`
      - `dispatch_strategy`
- `scripts/run_simulate_smoke_matrix.py` 现支持直接落盘：
  - `--report-json`
  - `--summary-json`
  - 这样不再依赖 shell redirect 去抓 stdout/stderr，scripted smoke report 更稳定
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_runtime_tuning.py tests/test_worker_scheduler.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_enrichment.py tests/test_seed_discovery.py tests/test_company_asset_completion.py tests/test_worker_recovery_daemon.py -q`
- 新 scripted smoke 基线已落到 `output/scripted_smoke_v3/`：
  - `openai_reuse`
    - `full_local_asset_reuse`
    - total 约 `1351.05ms`
    - `job_to_final_results` 约 `80.59ms`
    - `job_to_board_nonempty` 约 `129.88ms`
  - `google_scoped_cold`
    - `baseline_reuse_with_delta`
    - total 约 `18569.96ms`
    - `job_to_stage_1_preview` 约 `2266.48ms`
    - `job_to_final_results` 约 `16080.28ms`
    - `job_to_board_nonempty` 约 `16257.71ms`
    - progress 峰值：
      - `queued_worker_count=1`
      - `waiting_remote_harvest_count=1`
      - `pending_worker_count=1`
      - `active_worker_count=1`
  - `xai_live_roster`
    - `scoped_live_search`
    - total 约 `27678.67ms`
    - `job_to_stage_1_preview` 约 `8234.12ms`
    - `job_to_final_results` 约 `27340.48ms`
    - `job_to_board_nonempty` 约 `27472.91ms`
    - progress 峰值：
      - `queued_worker_count=3`
      - `waiting_remote_harvest_count=3`
      - `pending_worker_count=3`

### Shared runtime tuning 收口到 provider throughput / artifact workers，并把 smoke report 升级到 progress observability

- `runtime_tuning.py` 不再只管 cooldown/poll：
  - 现在同一套 profile/override resolver 也覆盖：
    - `provider_people_search_parallel_queries`
    - `harvest_company_roster_parallel_shards`
    - `candidate_artifact_parallel_min_candidates`
    - `candidate_artifact_max_workers`
- 已接入的主路径：
  - `seed_discovery.py`
    - Harvest `profile_search` 并行 query worker 数不再各自读 cost_policy，改为先走 shared runtime tuning resolver
  - `acquisition.py`
    - segmented `company-employees` shard fetch / shard worker dispatch 并发上限改为 shared resolver
  - `candidate_artifacts.py`
    - `foreground_fast` 的 candidate artifact 并发预算不再写死旧常数
    - 现在按 build profile + CPU + runtime tuning overrides 统一计算
    - `build_execution` 也新增：
      - `parallel_min_candidates`
      - `parallel_worker_cap`
      - `runtime_tuning_profile`
- `scripts/run_simulate_smoke_matrix.py` / `scripts/run_live_large_org_regression.py` 已清掉遗留的
  `allow_high_cost_sources` 调用参数，脚本重新与当前 active contract 对齐。
- `workflow_smoke.py` 的 provider-grade case report 继续升级：
  - 新增 `progress_observability`
  - 单 case 现在会结构化输出：
    - progress sample 数
    - `queued_worker_count / waiting_remote_harvest_count / pending_worker_count` 峰值
    - `result_count / observed_company_candidate_count` 是否回退
    - `manual_review_count` 是否作为 backlog 正常下降，而不是被误判为候选人数回退
    - 是否出现 “results 已 ready，但 progress terminal 事件滞后” 的 synthetic terminal sample
  - aggregate summary 也会带：
    - `progress_regression_case_count`
    - `progress_sample_count`
    - `progress_maxima`
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_runtime_tuning.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_candidate_artifacts.py -q -k 'records_phase_timings or foreground_fast_skips_compatibility_exports_and_hot_cache or aliases_strict_view_when_it_matches_canonical or legacy_like_benchmark_flags_disable_fast_paths or reuses_candidate_shards_and_rebuilds_only_dirty_candidates or uses_scope_replace_when_all_states_are_dirty'`
  - `./.venv-tests/bin/python -m pytest tests/test_company_asset_completion.py tests/test_enrichment.py tests/test_seed_discovery.py -q`
- 新 benchmark / smoke 观测：
  - `scripts/run_candidate_artifact_benchmark.py --candidate-count 320 --dirty-candidates 24 --repeat 3 --build-profile foreground_fast`
    - `cold_full` optimized median `3703.2ms` vs legacy-like `6745.95ms`，约 `1.82x`
    - `incremental_patch` optimized median `2007.68ms` vs legacy-like `2347.21ms`，约 `1.17x`
  - isolated/scripted smoke:
    - `openai_reasoning` full local reuse: total 约 `2115.8ms`，board probe wait 约 `17.48ms`
    - `google_multimodal_pretrain` baseline reuse + delta scripted case: total 约 `33807.25ms`
      - report 正确标出 `terminal_progress_lag_detected=true`
      - 无 progress counter regression
    - `xai_full_roster` scripted large-roster/profile-tail case: total 约 `31091.1ms`
      - `queued_worker_count / waiting_remote_harvest_count / pending_worker_count` 峰值均为 `3`
      - 无 progress counter regression，且最终 progress 已观测到 terminal

## 2026-04-23

### Evening stabilization tracker consolidated

- 已新增 [docs/SESSION_TRACKER_2026-04-23_PM.md](docs/SESSION_TRACKER_2026-04-23_PM.md)
  - 把 `2026-04-23 20:00` 之后讨论的收尾项集中成单一 Markdown tracker
  - 明确区分：
    - fixed
    - partially fixed
    - still open
  - 目的是避免 context compact 后再次遗漏：
    - `allow_high_cost_sources` 彻底退场
    - worker/progress/results 可见状态一致性
    - API payload slimming 剩余尾巴
    - runtime namespace isolation
    - Harvest batching / benchmark / Excel intake relaunch gate

### 4/21-4/23 变更复盘已落文档，core lane 语义与 live profile hydration 再收一层

- 已新增正式复盘文档：
  - `docs/CHANGE_REVIEW_2026-04-21_2026-04-23.md`
  - 重新归类了 4/21-4/23 这轮改动里：
    - 哪些属于正确的 contract 收口
    - 哪些只是方向正确但仍有 compatibility tail
    - 哪些本质上仍是补丁式/双轨式修复，需要继续回退成单一 contract
- 已继续清理 planner/review 的 core acquisition lane 可见语义：
  - `full_company_roster` 的 `roster_sources` 现以 `harvest_company_employees` 为主
  - `scoped_search_roster` / `former_employee_search` 现以 `harvest_profile_search` 为主
  - 不再在这类 core lane 的用户可见 metadata 里继续表达成 `web_search_seed_queries` 优先
  - `planning._build_assumptions(...)` 也已去掉旧的 “low-cost web search before paid LinkedIn people search” 叙事
- 已继续收 `enrichment.py` 的 live profile hydration 瓶颈：
  - `_fetch_harvest_profiles_for_urls(...)` 不再按 batch 全串行 waterfall 执行
  - 当前改成 bounded-window 并行：
    - `live`: 最多 2 个 batch 并发
    - `non-live`: 最多 4 个 batch 并发
  - 仍保留按 chunk 记 queued/fetched/failed registry 状态的 contract，不做无界 fan-out
- 已把同一套 Harvest live batching contract 向后台 `company_asset_completion` 收口：
  - live profile completion 不再固定 `150 x 1 worker` 的大批串行模式
  - 改为复用 `enrichment` 的 URL-count + source-mix adaptive window
  - roster-heavy completion 现在也能抬到 `3` 个并发 batch，而不是继续串行补全
- 已补/更新回归：
  - `tests/test_enrichment.py`
    - balanced live batch 断言更新为顺序无关
    - 新增 bounded parallel live batch 断言，验证 `205 -> 103/102` 且 `max_inflight=2`
  - `tests/test_company_asset_completion.py`
    - live completion 改为断言 adaptive parallel batching
    - 新增 roster-heavy live completion `240 -> 60x4` 且 `max_inflight>=3`
  - `tests/test_planning_modules.py`
    - 补 core roster source 语义断言
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_enrichment.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_planning_modules.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_review_plan_instructions.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_former_search_seed_keeps_primary_provider_search_for_scoped_search or build_sourcing_plan_splits_linkedin_and_public_web_acquisition_stages or plan_stage_colloquial_full_company_request_can_skip_review'`
- 当前剩余尾巴：
  - background prefetch worker chunk 提交仍主要是顺序 dispatch，尚未做同等级别的 bounded parallel submit
  - generic high-cost toggle 本身已从 active contract / smoke helper / 当前回归 fixture 中彻底移除；剩余的是更上层的 provider policy 收口，不是字段兼容尾巴

### Generic cost toggle 删除完成，补上 investor category replace 去重护栏

- `allow_high_cost_sources` 已从 active contract / smoke helper / 当前测试 fixture 中彻底移除：
  - 不再保留 `high_cost_requires_approval` 衍生 policy tail
  - 当前代码/测试搜索仅剩 tracker 与历史进度文档中的历史描述
- 顺手补上了 `storage.py` 的更泛化去重护栏：
  - `replace_company_category_data(...)` 现在会先删除与 incoming candidate ids 冲突的旧行
  - candidate/evidence bulk replace path 也会先做一次 payload 级 dedupe
  - 这样像 `investor_firm_roster` 这种“同 candidate_id 以另一 category 已存在”的情况，不会再把 workflow 打成 `UNIQUE constraint failed: candidates.candidate_id`
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_provider_execution_policy.py tests/test_planning_modules.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'investor_firm_workflow_uses_tiered_firm_roster or build_sourcing_plan_full_company_defaults_do_not_require_high_cost_approval or acquire_former_search_seed_uses_primary_provider_search_mode or acquire_former_search_seed_keeps_primary_provider_search_when_high_cost_not_allowed or acquire_former_search_seed_keeps_primary_provider_search_for_scoped_search or plan_stage_colloquial_full_company_request_can_skip_review or queue_workflow_backfills_missing_target_company_from_approved_review_scope or queue_workflow_rebuilds_google_keyword_shard_plan_from_approved_review_request or compile_plan_review_instruction_uses_model_then_schema_validation or workflow_progress_schema_summarizes_stage_and_worker_state or run_workflow_blocking_triggers_job_recovery_when_acquisition_is_blocked'`
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q -k 'test_hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs or test_hosted_simulate_xai_boundary_and_anthropic_asset_population or test_hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover or test_hosted_simulate_large_org_full_roster_existing_baseline_defaults_to_asset_population'`
  - `cd frontend-demo && npm run build`
  - `./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py tests/test_hosted_workflow_smoke.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/storage.py`
- 顺手补了一轮 hosted harness teardown 修复：
  - `tests/test_hosted_workflow_smoke.py` 现在会等待：
    - `background-outreach-layering-*`
    - `background-snapshot-materialization-*`
  - 代表性的 hosted smoke 组合回归已能在同一 pytest 进程内顺序通过，不再在 teardown 阶段被后台 reconcile 线程打出 segfault
  - 整套 hosted/browser full matrix 仍未重跑，但这条高频 crash 已不再是当前 blocker

### Replay 假候选人已收口，ECS 已切回 live provider

- 已修正 `harvest_connectors.py` 中 `replay` 模式的离线行为：
  - `replay` 现在是严格的 cache-only
  - cache miss 不再合成 `Offline Member` / 假 LinkedIn profile
  - synthetic sample 仅保留给 `simulate`
- 已补测试并通过：
  - `tests/test_harvest_connectors.py` 中 replay cache-miss / company-roster probe 相关断言已更新为“返回空结果而不是合成样本”
- 已在 ECS 上完成 Surge AI 污染清理：
  - 删除了 `Surge AI / 20260423T195046` 的 PG registry / shard / generation / membership / materialization state / history-job / LinkedIn registry 记录
  - 删除了 `/srv/sourcing-ai-agent/runtime/company_assets/surgeai`
  - 删除了 `/srv/sourcing-ai-agent/runtime/hot_cache_company_assets/surgeai`
  - 删除了对应 `runtime/jobs/401ca57f9fc9*.json`
  - 清理后复核计数均为 `0`，不会被后续 baseline / snapshot 复用
- 进一步确认并补上了第二层根因修复：
  - 问题不只是 `replay` 会产出假数据，而是这些 `_offline` body 之前还能落进生产共享的 `runtime/provider_cache/*`
  - 结果是后续 `live` job 也会命中这份污染缓存，表现成“明明切回 live 了，还是读到 Surge Ai Offline Member”
  - 现已收口为：
    - `live` / `replay` 都不会复用带 `_offline` 标记的 shared provider cache
    - `replay` / `simulate` / `scripted` 产出的 harvest body 不再写回 shared provider cache
    - 发现历史 `_offline` shared cache 时会直接丢弃并删除，而不是继续命中
  - 这次清理也补齐到了 provider cache 层，不再只删 PG / runtime assets / jobs 文件
- 经验沉淀：
  - `replay` 只是 provider mode，不等于独立测试环境
  - 真正的问题是生产 live runtime 与 replay/simulate/scripted 的 cache namespace 之前隔离不严
  - 后续终态仍建议继续推进“live runtime / test runtime / scripted runtime”物理隔离，避免再次发生交叉污染
- 已将 ECS `sourcing-ai-agent.service` 的 `SOURCING_EXTERNAL_PROVIDER_MODE` 从 `replay` 改为 `live`
  - `systemctl` 已重载并重启
  - `/proc/<pid>/environ` 已确认当前进程实际运行在 `SOURCING_EXTERNAL_PROVIDER_MODE=live`
  - `http://127.0.0.1:8765/health` 返回正常

### 检索方案页已前置 Target company LinkedIn

- `frontend-demo/src/components/PlanCard.tsx` 已将 `Target company LinkedIn` 从 `Advanced Mode` 挪到检索方案主卡片：
  - 默认展示后端当前识别到的 LinkedIn company URL
  - 以带圈问号 tooltip 展示识别来源 / 置信度 / 是否可复用本地资产
  - 仍保留手动修正输入框，只有用户填入新 URL 时才覆盖默认 slug 解析
- `frontend-demo/src/styles.css` 已补齐对应样式，并已通过 `frontend-demo` 生产构建验证
- 已完成 Cloudflare Pages production 发布：
  - Pages project: `sourcing-ai-agents-demo`
  - production deployment: `66610098-6882-4b91-9848-061aefd05229`
  - custom domain `https://demo.111874.xyz` 已确认切到新 bundle：
    - `/assets/index-CWVpTh4a.js`
    - `/assets/index-BVF0ErbD.css`
  - `https://demo.111874.xyz/health` 返回正常

### ECS 已完成整仓部署、PG control-plane bootstrap 与 Cloudflare production 发布

- 已将本地最新整仓代码增量同步到 ECS：
  - 远端代码目录确认是 `/opt/sourcing-ai-agent`
  - runtime 仍挂在 `/srv/sourcing-ai-agent/runtime`
  - 这次不是只补单文件，而是整仓 `rsync`，同时保留远端 `runtime/` 与 `venv/`
- 已确认并修复 ECS 上的真实 hosted leak：
  - 原有 `sourcing-ai-agent.service` systemd unit 仍直接执行
    `python -m sourcing_agent.cli serve`
  - drop-in `runtime.conf` 仍注入旧配置：
    - `SOURCING_EXTERNAL_PROVIDER_MODE=live`
    - `SOURCING_API_MAX_PARALLEL_REQUESTS=2`
  - 更关键的是它没有注入 `SOURCING_CONTROL_PLANE_POSTGRES_*`
  - 结果是 ECS 会自动重启回旧环境，并继续把 live control-plane 写到磁盘 SQLite shadow
- 已在 ECS 上补齐 Postgres 基础设施并完成控制面迁移：
  - 安装系统 `postgresql` / `postgresql-contrib`
  - 创建本地 `sourcing` 用户与 `sourcing_agent` 数据库
  - 将远端 `.local-postgres.env` 改成 ECS 实际可用 DSN：
    - `postgresql://sourcing:***@127.0.0.1:5432/sourcing_agent`
  - 将 live `runtime/control_plane.shadow.db` 全量 sync 到 PG
  - 已通过 `validate_postgres=true` 校验：
    - `jobs=4`
    - `job_events=138`
    - `organization_asset_registry=152`
    - `asset_materialization_generations=224`
    - `asset_membership_index=15863`
    - `candidate_materialization_state=1535`
    - `linkedin_profile_registry=452`
- 已把 ECS 的 systemd backend 改成 PG-only 生产配置：
  - 主 unit 现在显式注入：
    - `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
    - `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
    - `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
    - `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`
    - `SOURCING_EXTERNAL_PROVIDER_MODE=replay`
    - `SOURCING_API_MAX_PARALLEL_REQUESTS=8`
    - `SOURCING_API_LIGHT_REQUEST_RESERVED=2`
  - 已移除旧的 `runtime.conf`
  - `systemctl status sourcing-ai-agent.service` 已确认新环境生效
  - `/proc/<pid>/environ` 已确认当前 ECS live 进程实际携带上述 PG-only 变量
- 已完成 ECS 上的 OpenAI shard-family metadata 收口：
  - `OpenAI / 20260423T165904`
  - `--rebuild-from-assets` 恢复并校验 6 条 shard rows
  - family 结果已确认：
    - `Language Model -> Text`
    - `Vision -> Vision`
    - `Multimodal -> Multimodal`
- 已完成 Cloudflare Pages production 重新发布：
  - 先 build 最新 `frontend-demo/dist`
  - 再以 `--branch main` 发布到 `sourcing-ai-agents-demo`
  - 当前 `demo.111874.xyz` 已更新到最新 bundle：
    - `index-CnK2iRg-.js`
- 已完成公网验证：
  - `https://api.111874.xyz/health`
  - `https://demo.111874.xyz/health`
  - `https://demo.111874.xyz/api/runtime/health`
  - 当前均返回 `200`

### Hosted same-origin API proxy 已切到 Cloudflare Pages Functions

- 已完成 `demo.111874.xyz` 的同域 `/api/*` 收口，但实际落地路径不是独立 Worker route，而是 Pages Functions：
  - `frontend-demo/functions/api/[[path]].js`
  - `frontend-demo/functions/health.js`
  - `frontend-demo/cloudflare_api_proxy.mjs`
- 根因是当前本地 `cloudflare_pages.api_token` 足够做 Pages deploy，但不足以单独做 Workers script deploy / route bind：
  - `wrangler pages deploy ...` 可用
  - `wrangler deploy ... --route ...` 返回 `Authentication error [code: 10000]`
- 因此这轮改成把同域 proxy 收进 Pages 项目本身：
  - `demo.111874.xyz/api/* -> https://api.111874.xyz/api/*`
  - `demo.111874.xyz/health -> https://api.111874.xyz/health`
  - 发布 frontend 时同域 proxy 也一起更新，不再拆成“先 Pages、再 Worker route”两步
- 已完成验证：
  - `https://demo.111874.xyz/api/runtime/health` 返回后端 JSON，不再回落到前端 `index.html`
  - `https://demo.111874.xyz/health` 返回后端 JSON
  - `https://demo.111874.xyz` 当前已重新回到 `same-origin` bundle：`index-CnK2iRg-.js`
- 文档已同步更新：
  - `docs/CLOUDFLARE_SAME_ORIGIN_PROXY.md`

### 临时隐藏首页 Excel intake 入口，等待浏览器上传稳定性收口

- 历史记录：这一段描述的是当时的临时降级状态，已被 `2026-04-24` 的 “Excel intake relaunch gate 收尾” 取代。
- 当时首页 `Excel Intake` 入口改为 feature-flag gated：
  - `VITE_ENABLE_EXCEL_INTAKE_WORKFLOW=true` 才会显示
  - 默认本地/线上构建都不再展示该入口
- 原因不是后端 Excel 解析 contract 已失效，而是本地真实浏览器环境里仍出现间歇性：
  - `Local backend is unreachable via same-origin`
  - `Local backend is unreachable via http://localhost:8765`
  - `Local backend is unreachable via http://127.0.0.1:8765`
- Playwright 下 direct upload / workflow split 已可通过，但用户浏览器仍未稳定，因此当前策略是先隐藏，避免继续把未收敛链路暴露到首页。
- 后续重新开放前，需要补浏览器级稳定性回归并完成 direct-upload 网络路径的最终收口。

### 修复 acquisition_shard_registry 真实 PG 存量库的 legacy bool/int 迁移炸点

- 已修复 `OpenAI / Reasoning` 在 `/api/plan` 与 `/api/workflows/explain` 上触发的 `500`：
  - 根因不是 query 语义，而是本地 PG 里 `acquisition_shard_registry` 仍停留在 legacy 单表形态
  - 旧表中的 `provider_cap_hit` 是 `bigint`
  - writer schema ensure 在做 former/current split cutover 时，直接把旧列灌进新 `BOOLEAN` 列，导致类型不匹配
- 这次把修复收口到了 PG 契约边界，而不是只在某个调用点打补丁：
  - legacy table -> split tables 的迁移 SQL 现在会显式把 `provider_cap_hit` 归一为 boolean
  - `upsert_acquisition_shard_registry_rows(...)` 也会把旧 `0/1` payload 统一归一为 boolean
  - 因此同时覆盖：
    - 存量库 schema ensure
    - 后续 live writer / bulk upsert
- 已在本地真实 PG 执行 schema ensure，并确认当前状态：
  - `acquisition_shard_registry` 现为 compatibility view
  - `acquisition_shard_registry_current` / `acquisition_shard_registry_former` 均为物理表
  - 两张 split 表里的 `provider_cap_hit` 均为 `boolean`
- 已补回归：
  - `tests/test_control_plane_postgres.py`
  - 覆盖 legacy table split migration 的 `provider_cap_hit` 归一化
  - 覆盖 acquisition shard upsert 时 `0/1 -> bool` 的 PG 写入归一化
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_control_plane_postgres.py -q`
  - 本地 `POST /api/plan`
  - 本地 `POST /api/workflows/explain`
  - 当前对 `我想要OpenAI做Reasoning方向的人` 均已返回 `200`

### 收口 hosted live snapshot 在 `Public Web Stage 2 -> Final Results` 间被前台 materialization 卡住的问题

- 已锁定这次 `OpenAI / Multimodal` 在 ECS 上“Stage 2 已完成、但 Final Results 与候选人看板迟迟不出”的根因：
  - 不是 PG/SQLite 存储后端本身把 job 卡死
  - 而是 `direct asset-population finalization` 明明已可用时，`_refresh_running_workflow_before_retrieval(...)` 仍把已完成的 `harvest_prefetch` worker 视为“必须先 inline sync/materialize snapshot”的信号
  - 于是前台继续跑 `candidate_documents / normalized_artifacts / retrieval-ready`，把结果页阻塞在 post-stage2 收尾
- 这轮已把 contract 改成更清晰的两段式：
  - authoritative candidate source ready -> foreground finalize
  - snapshot sync/materialize/reconcile -> background deferred materialization
- 已完成的后端收口：
  - `orchestrator.py` 新增统一的 deferred `background_snapshot_materialization` helper
  - direct finalization 当前 snapshot 与 baseline patch 两条分支，都会显式写出 background materialization contract
  - `pre_retrieval_refresh` 新增“harvest-prefetch 可 defer 到后台”的语义，不再把这类 worker 完成机械等同于前台必须同步
  - background reconcile 事件文案也从“baseline-backed finalization”泛化成“deferred finalization”，避免误导
- 这次也补上了之前缺失的 regression fixture：
  - `direct finalization + completed harvest workers + no search-seed delta + snapshot artifacts not ready`
  - 这是此前本地回归没覆盖到、但 ECS hosted live case 正好触发的组合
- 已完成验证：
  - `python3 -m py_compile src/sourcing_agent/orchestrator.py tests/test_pipeline.py`
  - `./.venv/bin/python -m pytest tests/test_pipeline.py -q -k 'direct_finalization or harvest_prefetch_for_direct_finalization or harvest_defer_into_background_materialization or preview_can_use_stage_candidate_documents or equivalent_baseline_reuse or reused_snapshot_short_circuits_after_public_web_stage'`
  - 当前为 `9 passed`

### baseline candidate 选择收口，避免 stale authoritative row 误把已覆盖 query 判成 delta

- 已修复 `OpenAI / Coding` 这类“公司已有本地 query-family 资产，但 authoritative row 较旧”的计划误判：
  - 之前 `compile_asset_reuse_plan(...)` 只盯当前 authoritative registry row
  - 一旦 authoritative row 没及时晋升到较新的 snapshot，就会把已覆盖的 `current/former` query family 重新判成 `delta_from_snapshot`
  - 这次已改成：
    - 先算 authoritative baseline 的 reuse plan
    - 再对同公司其他 registry snapshot 做同一套 coverage 评估
    - 若存在更优 baseline candidate（更少 missing / 无 delta / coverage 更完整），自动切到那个 snapshot
- 本地真实验证结果：
  - `帮我找OpenAI做Coding方向的人`
  - 现已从：
    - `baseline_reuse_with_delta`
    - `planner_mode=delta_from_snapshot`
  - 修正为：
    - `effective_acquisition_mode=full_local_asset_reuse`
    - `planner_mode=reuse_snapshot_only`
    - `baseline_snapshot_id=20260422T171923`
- 已补回归：
  - `tests/test_workflow_explain.py::WorkflowExplainTest::test_explain_workflow_prefers_better_non_authoritative_snapshot_when_authoritative_baseline_is_stale`
- 这轮也额外回归了高信号 query：
  - `OpenAI / Reasoning`
  - `Anthropic / Pre-training`
  - 当前 reuse / delta 语义未被带坏
- 继续把 planner 与 `organization_execution_profile` 的 source 对齐也收掉了：
  - 根因不是 PG 缺数据，也不是 execution profile 缓存提前返回
  - 真正问题是 `organization_execution_profile._select_best_organization_asset_registry_row(...)`
    把 `evaluate_organization_asset_registry_promotion(...)` 当成了多轮链式比较器使用
  - `explicit_baseline_inclusion` 在链式比较里会把已经选中的更优 snapshot 又“晋升”回较旧 aggregate row，导致：
    - planner 选中较新的 baseline candidate
    - execution profile / authoritative row 仍停在旧 snapshot
- 这次已改成：
  - 提取共享 helper，统一做 organization-asset baseline candidate selection
  - 先按 coverage/quality 排序候选 row
  - 再始终以“当前 authoritative row”为固定基准，挑选第一个真正可晋升的更优候选
  - 不再在多个非 authoritative rows 之间反复链式 promotion
- 这层 shared helper 现在已同时接到：
  - `backfill_organization_asset_registry_for_company(...)`
  - `organization_execution_profile._select_best_organization_asset_registry_row(...)`
  - `compile_asset_reuse_plan(...)` 的 store-backed baseline candidate inventory
  - 避免出现“planner 已选对 baseline candidate，但 authoritative promotion / execution profile 仍各走各的”继续漂移
- 本地真实 PG/runtime 已验证：
  - `OpenAI` 当前 authoritative row 已从 `35230 / 20260422T151623` 切到
    `35232 / 20260422T171923`
  - `organization_execution_profiles.source_registry_id / source_snapshot_id` 也已同步切到同一 row
- 继续收掉了 `explicit_baseline_inclusion` 过宽导致的 live 回摆：
  - 之前它只要看到 “candidate selected_snapshot_ids 包含当前 authoritative snapshot” 就会直接允许 promotion
  - 这会让 `OpenAI` 在 authoritative 已经是 `20260422T171923` 时，又被更薄的历史 aggregate
    `20260416T095325` 反压回去
  - 现已改成：
    - `explicit_baseline_inclusion` 只有在 candidate sort key 更优时才可晋升
    - 同时必须满足：
      - 要么是显式 aggregate 且对当前 authoritative 形成非回退 subsumption
      - 要么是当前 authoritative 本身质量很差时的 `quality_recovery`
- live 验证结果：
  - `帮我找OpenAI做Coding方向的人`
    - `effective_acquisition_mode=full_local_asset_reuse`
    - `planner_mode=reuse_snapshot_only`
    - `baseline_snapshot_id=20260422T171923`
    - `organization_execution_profile.source_snapshot_id=20260422T171923`
  - `帮我找OpenAI做Pre-train方向的人`
    - 仍保持 `planner_mode=delta_from_snapshot`
    - 未被这轮 shared helper / promotion guard 收口带坏
- 已补回归：
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_promotion_candidate_selection_does_not_chain_regress_on_explicit_baseline_inclusion`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_explicit_baseline_inclusion_does_not_promote_thinner_aggregate_over_richer_authoritative`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_candidate_inventory_prioritizes_promoted_row_before_authoritative`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_select_best_registry_row_does_not_regress_after_promoting_better_candidate`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_ensure_execution_profile_promotes_better_registry_row_and_aligns_source_snapshot`

### Browser E2E 候选人看板分页稳定性与真实大 snapshot cold build 基准

- `frontend-demo/scripts/run_workflow_e2e.mjs` 现支持两类入口：
  - 正常 `Search -> plan -> review -> results`
  - 直接打开 `?history=...&job=...` 的 restored-results 入口
- `tests/test_frontend_browser_e2e.py` 新增并固定了两条候选人看板分页稳定性 browser E2E：
  - large-org existing baseline workflow case
  - restored-history results recovery case
- 当前 browser 断言已经明确覆盖：
  - 切到第 2 页后不回跳到第 1 页
  - 当前页首批候选人 preview 顺序保持稳定
  - 如果没有观察到 `loadedCount` 继续增长，则必须已经处于 `loaded == total` 的全量态，不能是半加载错态
- 为了稳定制造 hydration 窗口，后端新增了一个测试专用延迟钩子：
  - `SOURCING_TEST_CANDIDATE_PAGE_DELAY_MS`
  - 仅用于 browser E2E / test harness，不改变默认生产路径
- 已完成验证：
  - `frontend-demo` 生产构建
  - `tests/test_frontend_browser_e2e.py`
    - `test_browser_workflow_e2e_covers_large_org_existing_baseline_asset_population`
    - `test_browser_results_recovery_hydration_preserves_second_page`
- 追加跑了一条真实大 snapshot 冷启动 benchmark：
  - 来源：隔离 runtime 下的 Google `20260423T040115`
  - 规模：`5897` candidates
  - `build_company_candidate_artifacts(..., build_profile=\"foreground_fast\")`
  - cold full wall time：`863216ms`，约 `14.4min`
  - 关键 timings：
    - `prepare_candidates`: `376223ms`
    - `payload_build_total`: `376552ms`
    - `view_write_total`: `381302ms`
    - `state_upsert`: `107ms`
    - `generation_register`: `651ms`
    - `finalize_total`: `1222ms`
  - 结论：
    - 当前真实大 snapshot cold build 的主瓶颈已明确不是 PG `state_upsert` / `finalize`
    - 主要耗时仍集中在 per-candidate prepare/payload/materialization 主路径

### explain/smoke 脚本隔离 runtime 收口

- `scripts/run_explain_dry_run_matrix.py` 与 `scripts/run_simulate_smoke_matrix.py` 现在都支持：
  - `--runtime-dir`
  - `--runtime-env-file`
  - `--seed-reference-runtime`
  - `--provider-mode`
  - `--scripted-scenario`
  - `--fast-runtime`
- 新增 `src/sourcing_agent/scripted_test_runtime.py`：
  - 负责启动脚本自带的 in-process backend
  - 显式把 `SOURCING_RUNTIME_DIR` 指向专用 `runtime/test_env/...`
  - 最初默认写空的 local-postgres env sentinel 来阻断仓库根 `.local-postgres.env`；later 2026-05-07 PG-only workflow confidence closeout replaced that for smoke/manual confidence with runtime-scoped `.scripted-local-postgres.env` and fail-closed PG readiness.
- 这意味着 explain/smoke 脚本现在可以稳定跑在“专用测试 runtime”上，而不是继续隐式吃当前本地运行态数据
- 近真实 scripted 模拟的推荐路径也已补档：
  - 先用 `scripts/seed_test_env_assets.py` 把当前 authoritative snapshot 种到 `runtime/test_env/...`
  - 再用 `--runtime-dir` + `--provider-mode scripted` 跑 explain/smoke
- 已补回归：
  - `tests/test_scripted_test_runtime.py`
  - 覆盖“阻断父目录 PG env 泄漏”和“seeded reference runtime 可直接提供 explain API”

### authoritative baseline completeness contract 收口与 large-specific reuse gate 去分叉

- `asset_reuse_planning.py` 已把“large org 单独 reuse helper + small/medium 通用 helper”的结构收成单一 authoritative-baseline completeness contract：
  - population-default reuse 与 embedded query reuse 现在共用同一套主判断骨架
  - 不再通过“large org 直接走另一套分支”来决定能否 `reuse_snapshot`
- `medium -> hybrid` 的语义已明确保留在 acquisition shape，而不是 reuse eligibility：
  - directional query -> `scoped_search_roster`
  - broad/full-company query -> `full_company_roster`
- 修掉了一个 hosted smoke 回归：
  - 之前把“multi-snapshot directional query 不能直接 population-default reuse”错误扩散到了 embedded query reuse
  - 已收回到 population-default 那一层，恢复 `Google multimodal + pre-train` 这类 case 的：
    - current lane `reuse_baseline`
    - former lane `delta_acquisition`
- 当前已明确的业务语义：
  - `Anthropic` 这类已经具备完整 authoritative baseline 的公司，可以直接 `reuse_snapshot`
  - `Google / OpenAI` 当前更接近 family-scoped authoritative baseline：
    - 已覆盖 family 可 reuse
    - 未覆盖 family 继续 `delta_from_snapshot`
    - 不应因为积累了多个方向性 snapshot 就自动视为“公司级 full local reuse”
- `selected_snapshot_ids > 1` 的粗代理已经进一步缩窄：
  - directional query 不再因为“snapshot 数量 > 1”被机械阻断
  - 现在看的是 `source_snapshot_selection.mode` / aggregate coverage proof
  - `all_history_snapshots` 这类历史并集仍不解锁 population-default reuse
  - `preferred_snapshot_subset` 这类显式聚合子集，在 lane coverage 完整时可解锁 population-default reuse
- 已完成验证：
  - `tests/test_planning_modules.py`
  - `tests/test_workflow_explain.py`
  - `tests/test_pipeline.py`
  - `tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_hosted_explain_dry_run_matrix_covers_reference_regressions`
  - `tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs`
  - 全仓 `pytest -q`
  - repo-configured `mypy`
  - 当前均已转绿

### PG former/current 物理分表 cutover、job/results 默认瘦身与全仓回归转绿

- 已完成 PG `acquisition_shard_registry` 的 former/current 物理分表 cutover：
  - live writer、SQLite->PG sync、direct-stream sync、PG->PG migration、PG snapshot export 现统一收口到：
    - `acquisition_shard_registry_current`
    - `acquisition_shard_registry_former`
  - 逻辑名 `acquisition_shard_registry` 现在作为 compatibility view 保留给上层读取契约
  - 不再把“单表 + lane contract”当成 PG 终态
- `/api/jobs/{job_id}` 现改为 summary-first 默认返回：
  - 默认只返回 job summary / status / 轻量 request preview
  - full `events` / `intent_rewrite` 需显式 `?include_details=1`
  - 这与 `/api/jobs/{job_id}/results` 之前的 summary-first 改动配套，进一步收掉前端 polling 的重 payload
- 补充并固定了几类新增测试契约：
  - PG former/current split upsert contract
  - `/api/jobs/{job_id}` summary-only default + opt-in detail
  - hosted smoke 中真正需要 full candidates 的调用显式 `?include_candidates=1`
- 顺手修掉了几处跨环境路径漂移：
  - `harvest_connectors.py` 不再把 macOS `/var` 强制提前解析成 `/private/var`
  - `service_daemon.py` 生成 systemd 单元时不再把工作目录强行 `.resolve()`
- 已完成验证：
  - 全仓 `pytest -q`
  - repo-configured `mypy`
  - 相关 control-plane / results API / hosted smoke 子集
  - 当前均已转绿

## 2026-04-21

### 手工测试缺口复盘、follow-up reuse 回归补齐与 history 恢复链路入默认矩阵

- 这轮把“为什么 simulate / explain / 已有 E2E 没挡住后续手测问题”正式沉淀进：
  - `docs/TESTING_PLAYBOOK.md`
- 核心结论已明确：
  - 之前更多在测“单次 workflow 能否完成”
  - 手工暴露的问题则集中在“workflow 写回后，下一次 explain / 下一次结果恢复 / 历史打开是否仍正确”
  - 因而必须把 `persisted state` 当成默认断言对象，而不是只看当次 API 返回
- 新增两条默认 automated regression：
  - `test_hosted_simulate_reuse_queries_preserve_follow_up_planning_contract`
    - 先跑完整 reuse workflow，再做 follow-up explain
    - 覆盖 `Reflection AI Post-train` 与 `OpenAI Reasoning`
  - `test_hosted_simulate_completed_history_round_trip_exposes_results_recovery`
    - 断言 `history_id -> results -> /api/frontend-history/{history_id}` 恢复链路稳定
- `src/sourcing_agent/regression_matrix.py` 也同步收口：
  - `hosted-workflow-smoke-focus` 默认把上述两条用户链路一起纳入
  - `orchestrator / results / history` 相关改动现在也会命中这组 smoke，而不再只跑 explain/API contract
- 当前测试体系已明确把以下模式作为后续默认原则：
  - reuse / delta planning 改动必须至少有一条 `run once -> persisted -> explain again`
  - history/results 相关改动必须至少有一条 `history_id -> completed -> recovery`
  - 本地资产复用 query 需要独立 timing budget，不再混在 large-org live 路径里看

### Mac migration prep、full snapshot lane 与 ECS access playbook

- 已把“从当前 Linux/WSL 虚拟机迁到另一台 Mac”收成正式文档与脚本：
  - `docs/MAC_DEV_ENV_MIGRATION.md`
  - `scripts/prepare_mac_migration_bundle.sh`
- 迁移语义现在明确拆成两条：
  - portable migration bundle
    - 目标是让 Mac 侧快速恢复为可运行开发环境
  - full local snapshot
    - 目标是尽可能完整保留旧工作树、runtime、`runtime/secrets/`、完整 `~/.codex` 与取证上下文
- `prepare_mac_migration_bundle.sh` 现在支持：
  - `--stage-repo`
  - `--stage-repo-full`
  - `--stage-codex`
  - `--stage-codex-full`
  - `--stage-full-snapshot`
  - `--include-secrets`
  - `--include-codex-auth`
  - `--codex-session-id <id>`
- 本地 PG 发现机制已补上 env-file 入口：
  - `.local-postgres.env`
  - `.local-postgres/connection.env`
  - 不再只依赖 Linux/WSL 里的 `.local-postgres/{extract,data}`
- 当前本地已确认：
  - `~/.codex/sessions/` 中存在 `019d6630-2137-7f70-b742-43f979b8207b`
  - `~/.codex/state_5.sqlite` 的 `threads` 表里也存在该 session id
  - 因此如果完整迁移 `~/.codex`，继续 `codex resume 019d6630-2137-7f70-b742-43f979b8207b` 的成功率会明显高于只拷 `history.jsonl`
- 这轮还新增了一份可重复使用的 ECS 连接文档：
  - `docs/ECS_ACCESS_PLAYBOOK.md`
  - 统一沉淀 SSH config、端口转发、`rsync/scp`、远端 health probe、最小重启路径
- 当前推荐的传输优先级也已写明：
  - 1. 机器直连 `rsync/scp`
  - 2. 外接 SSD / 局域网共享盘
  - 3. OSS / R2 / S3 等对象存储中转
  - 一般不需要为了本地开发环境迁移专门走 OSS；只有在两台机器无法直接连通，或你更希望云端中转时才采用
- 已把“全量迁移版”的标准 resync 清单补进：
  - `docs/MAC_DEV_ENV_MIGRATION.md`
  - 覆盖 source-side freeze、full snapshot 生成、`rsync` 直传、Mac 侧 restore、以及 `codex resume` 校验


## 2026-04-13

### OpenAI hosted rerun 跑通、search-seed lane merge 修复与云端 canonical bundle 补齐

- 已修复一个直接影响 hosted live run 的 acquisition 状态问题：
  - `scoped_search_roster` 的 current lane 与 former lane 之前共用单个 `search_seed_snapshot`
  - former lane 会覆盖 current lane，导致 `95 + 44` 最终只剩 `80`
  - 现已改成：
    - current/former search-seed snapshot 合并
    - 合并结果写回 `search_seed_discovery/entries.json + summary.json`
    - hosted recovery / restore 看到的也是同一份 merged snapshot
- 已补定向回归测试：
  - former search-seed merge
  - 方向型 scoped search 在模型弱化时仍保留 `functionIds=[24,8]`
  - 不回退到 `job_titles`
- 已完成一条真实 hosted OpenAI workflow 复跑：
  - `job_id=49044c9afdd2`
  - `snapshot_id=20260413T140350`
  - 不再手动 `execute-workflow`
  - 自动完成：
    - `linkedin_stage_1`
    - `stage_1_preview`
    - `public_web_stage_2`
    - `stage_2_final`
- 这次 OpenAI hosted rerun 的关键结果：
  - current lane `94`
  - former lane `44`
  - merged stage-1 candidate base `130`
  - stage-1 preview `36 matches`
  - final `40 matches`
- 已导出并开始同步新的 canonical cloud bundle：
  - `company_snapshot_openai_20260413t140350_20260413T061157Z`
  - `sqlite_snapshot_sourcing_agent_db_20260413T061157Z`
- 文档口径已继续统一：
  - 服务器恢复默认只认 `import-cloud-assets`
  - hosted 默认执行路径只认 `serve + run-worker-daemon-service`
  - `download-asset-bundle + restore-*`、`execute-workflow` 退回为排障动作

## 2026-04-12

### Canonical cloud bundle catalog 与 intent_axes 执行层下沉

- 已把服务器恢复默认基线收敛为：
  - 1 个全局 `sqlite_snapshot`
  - 每个 canonical company 1 个 `company_snapshot`
- 已新增 canonical bundle 清单文档：
  - `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md`
  - 记录实际 bundle id、恢复顺序、去重策略与 Google clean snapshot 替换说明
- 已把 `intent_axes` 从展示层继续下沉到执行入口：
  - `JobRequest.from_payload` 现在可直接 materialize `intent_axes`
  - plan review instruction inference 会消费 `intent_axes`-only request payload
  - review apply / target company backfill 也会先 materialize `intent_axes`
- 已进一步把 planning/acquisition 内部消费切到统一 `intent_view`：
  - retrieval strategy / structured filters / filter layers / criteria summary / open questions 不再各自散读老字段
  - acquisition 对 `force_fresh_run / reuse_existing_roster / run_former_search_seed` 的判断也开始统一走 `intent_view`
  - 修复了一个 request merge 回归：
    - 模型直接返回的 `scope_disambiguation` 现在会被真正合入 request payload
    - 不会再被后续规则推断静默覆盖
- 已补回归测试，确保“模型只返回 intent_axes”时，planning / review / acquisition 仍能生成正确执行字段，而不只是 preview 好看。
- 已修一处 preview / request 漂移：
  - 过去 `request_preview.keywords` 可能缺少 acquisition strategy 真正执行的扩展关键词
  - 现在 `plan_workflow / start_workflow` 返回的 `request` 与 `request_preview` 都会对齐 execution-aligned request
  - 例如 Google `multimodal + Pre-train` 这类 query，会明确把 `Pre-train` 反映到返回 payload，而不再只存在于内部 `filter_hints.keywords`
- 本地与 OSS 远端 canonical bundle index 当前都已收敛为：
  - 7 个 `company_snapshot`
  - 1 个 `sqlite_snapshot`
  - legacy `company_handoff` 与旧 Google/SQLite bundle 已从索引删除

### Effective request payload 贯通、规则检索语义澄清与 Excel intake 补档

- 已把 `intent_axes -> intent_view -> effective request payload` 继续下沉到执行末端：
  - `build_effective_request_payload(...)` 已进入 acquisition worker payload、search seed discovery、retrieval ranking
  - `seed_discovery.py` 不再各自散读老扁平字段，而是消费统一 execution payload
  - `scoring.py` 与 `semantic_retrieval.py` 也开始直接按 `intent_view` 解释 `target_company / organization_keywords / employment_statuses / must_have_*`
- 已明确当前 retrieval 的真实形态，避免文档误导：
  - 默认 final ranking 不是 LLM-driven rerank
  - `scoring.py` 仍是规则/lexical/confidence 主链
  - `semantic_retrieval.py` 默认走本地 sparse semantic
  - external semantic provider 只在 `allow_high_cost_sources` 打开时参与
- 已补回归测试，覆盖新的执行语义：
  - `tests/test_planning_modules.py`
  - `tests/test_pipeline.py`
  - `tests/test_scoring.py`
  - `tests/test_semantic_retrieval.py`
- 已开始把 Excel intake 纳入当前文档口径：
  - 当前支持 `intake-excel / continue-excel-intake`
  - 上传表格后可先做 schema 识别、本地去重、manual review continuation，再决定是否触发新 LinkedIn fetch

## 2026-04-11

### Hosted 默认路径文档化、前端禁区明确化与 GitHub 上传边界收束

- 已把云端默认执行路径明确为 `serve + run-worker-daemon-service`：
  - README 新增 hosted 默认运行章节
  - operations playbook 新增云端最小启动组合与健康检查
  - 强调“手工 execute-workflow 续跑”只用于排障，不作为常规运行方式
- 已补前端边界，避免直接读 runtime 文件：
  - `docs/FRONTEND_API_CONTRACT.md` 明确前端只消费 API contract
  - 明确禁止把 `runtime/company_assets/*`、`runtime/jobs/*` 作为前端主数据源
  - 阶段反馈统一以 `workflow_stage_summaries` 为准
- 已新增集中指南：
  - `docs/HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md`
  - 汇总云端启动、前端禁区、GitHub 上传范围、上线前检查
- 已在文档索引与 README 文档地图挂载新入口：
  - `docs/INDEX.md`
  - `README.md`
- 已补 GitHub 推送边界说明（降部署成本）：
  - 建议提交 `src/tests/docs/contracts/configs-example/README/PROGRESS`
  - 不提交 `runtime/**`、`secrets/**`、raw assets 与本机缓存
  - 明确临时 live smoke 配置不应作为默认提交资产

### 两阶段工作流阶段总结透传、真实 smoke test 收敛与前端契约补齐

- 已把阶段性 workflow summary 固化为稳定返回结构：
  - `workflow_stage_summaries`
  - 同时出现在：
    - `GET /api/jobs/{job_id}/progress`
    - `GET /api/jobs/{job_id}/results`
  - 当前固定阶段顺序：
    - `linkedin_stage_1`
    - `stage_1_preview`
    - `public_web_stage_2`
    - `stage_2_final`
- snapshot 侧现也会同步落盘阶段总结文件：
  - `runtime/company_assets/{company}/{snapshot_id}/workflow_stage_summaries/*.json`
  - 这样前端不必直接读 snapshot 文件，但运维/离线调试仍可审计
- 已修复一个关键回归点：
  - 后台 harvest/search reconcile 之前会覆盖或丢失阶段 marker
  - 现在 `linkedin_stage_1 / stage1_preview / public_web_stage_2 / analysis_stage_mode` 会在 reconcile 后保留
- 已完成真实 live smoke test 验证：
  - Humans& 任务已在“不手动 execute-workflow”的情况下端到端完成
  - 最新稳定样例：
    - `job_id=b752d176a669`
    - `snapshot_id=20260411T145117`
- 前端 contract/example 已同步更新：
  - `contracts/frontend_api_contract.ts`
  - `contracts/frontend_api_contract.schema.json`
  - `contracts/frontend_api_adapter.ts`
  - `contracts/frontend_react_hooks.example.tsx`
  - 现在前端可直接拿 typed `workflow_stage_summaries`，并使用 stage helper 渲染阶段卡片
- 定向回归测试已通过：
  - `single_stage_workflow_still_publishes_stage_progress_markers`
  - `background_harvest_prefetch_reconcile_preserves_stage_progress_markers`
  - `two_stage_workflow_publishes_stage1_preview_and_continues_public_web_stage2_by_default`
  - `http_api_smoke`

## 2026-04-10

### GitHub Dev 差异审阅、操作教程补齐与推送准备

- 已完成本地工作区相对 `origin/dev` 的结构化审阅，并沉淀为可复盘文档：
  - `docs/GITHUB_DEV_DIFF_REVIEW_2026-04-10.md`
  - 覆盖 workflow orchestration、acquisition/harvest、profile registry、query rewrite、frontend contract 五大变更面
- 已补“当前怎么调用 + 怎么追踪进度 + 怎么恢复执行”的实操教程：
  - `docs/WORKFLOW_OPERATIONS_PLAYBOOK.md`
  - 统一说明 CLI 与 API 的标准调用路径：
    - `plan -> review-plan -> start-workflow -> show-progress/show-workers -> show-job/show-trace`
  - 补充 query dispatch 去重策略与高成本 query 的 plan-only 审查建议
- 已将上述新文档挂到 canonical 入口：
  - `docs/INDEX.md`
  - `README.md`（子项目文档地图）
- 已对 Google keyword-first 路径做参数层修正（plan-only 复核通过）：
  - 关键词提取扩大到 `Veo / Nano Banana / vision-language / video generation / multimodality`
  - `keyword_priority_only` 下的 `search_seed_queries` 改为方向词优先，不再回落到泛化 `... Research Researcher` 模板
  - large-org keyword 模式下避免把 `job_titles` 强塞进 former profile-search 过滤，降低误收敛风险
- 当前推送前状态：
  - 文档入口、进度文档、变更评审文档已补齐
  - 代码侧已有较大规模改动，建议以 PR 方式先推送到 `dev`，通过 CI 与人工 review 后再合并

## 2026-04-08

### Canonical Asset Views、Facet Hard Filters 与文档体系收束

- 已把 Thinking Machines Lab 的分析入口收束为单一 canonical snapshot：
  - `runtime/company_assets/thinkingmachineslab/20260407T181912`
  - 并显式拆成：
    - `canonical_merged`
    - `strict_roster_only`
- retrieval 现已支持显式资产视图与更稳的功能过滤：
  - `asset_view`
  - `must_have_facet / must_have_facets`
  - `must_have_primary_role_bucket / must_have_primary_role_buckets`
- primary-role-bucket 过滤现已贯穿：
  - request normalization
  - planning summary
  - retrieval hard filter
  - baseline request-family matching
- 已收紧 retrieval scoring：
  - `notes` 不再作为 primary role bucket 查询的主要 lexical / semantic 命中来源
  - 这样 `ops / recruiting / infra_systems / multimodal` 这类切片不会再被 acquisition boilerplate 大面积污染
- Thinking Machines Lab strict-view live validation 已更新：
  - `total_matches=9`
  - `manual_review_queue_count=0`
  - top matched fields 只剩：
    - `work_history`
    - `derived_facets`
    - `focus_areas`
- 已补新的当前态文档：
  - `docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md`
  - `docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md`
  - `docs/DATA_ASSET_GOVERNANCE.md`
  - `docs/SERVICE_EVOLUTION_STRATEGY.md`
- 已开始清理 GitHub-facing 文档入口：
  - 新增 `docs/INDEX.md` 作为 canonical docs map
  - 根目录 README / ONBOARDING / 子项目 README 已开始从 dated handoff 迁回 current-state docs
  - canonical docs 中的绝对本机路径链接和 `cd '/home/...'` 命令已改成 GitHub-friendly 相对路径或通用路径

## 2026-04-07

### Harvest Auth 恢复、Browser Search Provider 与 PDF 强解析

- 已恢复本机真实 provider 配置：
  - `runtime/secrets/providers.local.json` 现已重新写回：
    - Claude relay
    - Qwen / DashScope
    - Harvest / Apify
    - R2 object storage
  - 同时补了本机 `search_provider` 配置：
    - `serper_google -> google_browser -> duckduckgo_html`
    - `google_browser` 默认使用：
      - `runtime/vendor/playwright/node_modules`
      - `runtime/vendor/playwright-browsers`
      - `runtime/vendor/npm-cache`
  - 所有真实配置继续只保留在本机 secret/runtime 层，不进 Git
- 已重新验证 Harvest auth：
  - 新 Apify token 已真实可用
  - `linkedin-company-employees` live smoke test 返回 `STATUS 201`
  - `linkedin-profile-scraper` live smoke test 已成功返回 `Saurabh Garg` 的 profile detail
  - 因此早先的 `401 user-or-token-not-found` 已确认是旧 token 状态，不再是当前本机配置
- 已纠正 `linkedin-profile-scraper` 的关键 schema 结论：
  - 之前错误地把字段写成了 `profileUrls`
  - 当前经 live 验证确认可用字段是：
    - `urls`
    - `publicIdentifiers`
    - `queries`
    - `profileIds`
  - 当前代码已改回 `urls`
  - 另外已经确认：
    - vanity URL 可用
    - opaque LinkedIn URL 可用
    - `profileIds` 可用
- 已重新验证 Thinking Machines Lab 的 profile completion 主链：
  - `complete-company-assets --profile-detail-limit 6`
  - `fetched_profile_count=6`
  - 说明 Harvest profile-scraper 已重新进入真实补全链路
  - 当前 `completed_candidates=0` 的原因已收敛为 candidate/profile matching 仍偏保守，而不是 auth/schema 问题
- 已新增 browser-based Google search provider：
  - 新增 `scripts/google_search_browser.cjs`
  - 新增 `google_browser` provider，已接入 `src/sourcing_agent/search_provider.py`
  - `SearchProviderSettings` 已补：
    - `google_browser_node_modules_dir`
    - `google_browser_browsers_path`
    - `google_browser_script_path`
    - `google_browser_headless`
    - `google_browser_locale`
  - 当前 provider 采用真实 Chromium/Playwright 路线，而不是 requests 模拟 Google Search
  - 当前 WSL 环境下仍有一个明确阻塞：
    - browser launch 缺少系统共享库 `libnspr4.so`
    - 因此 browser lane 已实现，但本机 live Google query 暂未真正跑通
- 已补 PDF 多级文本提取链：
  - `pypdf`
  - `pdfminer.six`
  - `pdftotext`
  - `OCR (pdftoppm + tesseract)` 预留
  - 当前实现会自动选择文本质量最好的提取结果
  - `runtime/vendor/python` 已安装本机可复用的 `pdfminer.six`
  - 代码现会自动从 `runtime/vendor/python` 发现本地增强依赖，不进 Git
- 已对 Horace He 的 PDF 做真实验证：
  - 当前最佳方法为 `pdfminer`
  - 提取文本约 `3538` 个字符
  - 已成功拿到 Cornell 教育信息、Facebook/Google 工作经历和技能字段
  - 这说明 PDF 强解析链已从“只保留链接”升级到“可产出结构化草稿”
- 已补相关测试覆盖：
  - `tests/test_harvest_connectors.py`
  - `tests/test_search_provider.py`
  - `tests/test_document_extraction.py`

### HarvestAPI 调用方法沉淀与 Corner Case Exploration 升级

- 已补 Harvest 调用手册：
  - 新增 `docs/HARVESTAPI_PLAYBOOK.md`
  - 系统记录了三个 actor 的 console/readme 链接：
    - `linkedin-profile-scraper`
    - `linkedin-profile-search`
    - `linkedin-company-employees`
  - 系统记录了当前项目里真正可复用的 payload 规范、former/current 推荐调用顺序、Thinking Machines Lab live 结论和已知坑
- 已修正 `HarvestProfileConnector` 的关键 payload 错误：
  - 先前错误地改成了 `profileUrls`
  - 当前已根据 live 验证改回 `urls`
  - 并明确记录 `urls/publicIdentifiers/queries/profileIds` 都是当前 actor 可用入口
  - 对应测试已补到 `tests/test_harvest_connectors.py`
- 已重新确认 Harvest secret 的本地标准位置：
  - 默认开发环境放在 `runtime/secrets/providers.local.json`
  - 不再建议依赖旧项目 `api_accounts.json` 隐式发现
- 已确认一个关键调试结论：
  - `2026-04-07` 早先使用旧 token 做最小 API smoke test 时，Apify 返回了 `401 user-or-token-not-found`
  - 当前已切到新的可用 token，并完成 live 验证
  - 相关历史结论保留在 `docs/HARVESTAPI_PLAYBOOK.md`，用于说明“0 results”不一定是 payload 问题，也可能是 auth 问题
- 已升级 `analyze_page_asset` 输出 schema：
  - 新增：
    - `education_signals`
    - `work_history_signals`
    - `affiliation_signals`
    - `document_type`
  - deterministic fallback 现已能从 `text_blocks` 中做基础规则抽取
  - Claude / Qwen / 其他 OpenAI-compatible provider 现在也能按同一 schema 返回结构化结果
- 已补统一文档抽取模块：
  - 新增 `src/sourcing_agent/document_extraction.py`
  - 统一处理：
    - HTML homepage / CV
    - Google Docs CV
    - PDF resume
  - 新增通用能力：
    - `analyze_remote_document`
    - `build_candidate_patch_from_analysis`
    - `build_candidate_patch_from_signal_bundle`
    - `extract_pdf_text`
- 已补底层二进制资产能力：
  - `AssetLogger` 新增 `write_bytes`
  - `web_fetch.py` 新增 `fetch_binary_url`
  - 这使 PDF 原始资产、提取文本和 analysis input/output 都能按统一资产纪律落盘
- 已升级 `manual_review_resolution`：
  - 人工 review 提供的 homepage / Google Docs / PDF source link 现在不仅会保存为 evidence
  - 还会自动进入文档分析
  - 并将 education/work history/affiliation 草稿回写到 candidate
  - 对应测试已补到 `tests/test_manual_review_resolution.py`
- 已升级 `exploratory_enrichment`：
  - 搜索结果页命中的 homepage 现在会继续跟进其 `resume_urls`
  - 若命中 Google Docs CV 或 PDF resume，会进一步抽取结构化信号
  - exploration merge 阶段已能把这些信号补进 candidate，而不再只写 notes / media_url
- 已做真实小样本验证：
  - Jeremy Bernstein 的 Google Docs CV 现在可自动抽出 education/work history/Thinking Machines Lab affiliation 草稿
  - Horace He 的 PDF 现在已接入 `resume_url -> raw pdf -> extracted_text -> analysis` 链路
  - 当前 `pdfminer` 已可对 Horace 这份 PDF 抽出有效正文，`pypdf` 不是这份 PDF 的最佳方法
  - OCR 仍作为后续增强项保留，用于真正的图片型 PDF
- 已更新 onboarding/readme：
  - `README.md`
  - monorepo 根 `README.md`
  - 新增把 `docs/HARVESTAPI_PLAYBOOK.md` 纳入接手必读路径

## 2026-04-06

### Thinking Machines Lab 资产补全与可复用候选文档提炼

- 已修正 Harvest profile parser 的两个关键问题：
  - `parse_harvest_profile_payload` 现在会从 `firstName + lastName` 回填 `full_name`
  - `currentPosition.companyName` 现在会参与 `current_company` 提取
  - 这避免了已有 LinkedIn URL 的 profile detail 因姓名或 current company 解析不全而无法进入后续匹配
- 已修正 known-URL profile 匹配策略：
  - 若 `candidate.linkedin_url` 与 `profile.profile_url` 一致，且姓名一致，则优先视为同一个人
  - 不再把“必须先匹配 company 字段”作为已知 URL detail enrichment 的唯一条件
- 已补公司级历史资产物化层：
  - 新增 `candidate_artifacts.py` 中的 `materialize_company_candidate_view`
  - 会聚合同一公司所有历史 snapshot 的 `candidate_documents.json` 与当前 SQLite 主库
  - 不再因为后续 former workflow 覆盖主库，就丢失 earlier current roster
- 已补公司级后处理资产补全入口：
  - 新增 `complete-company-assets`
  - 补全流程现在是：
    - materialize company history
    - sync candidates/evidence back into SQLite
    - known-URL profile completion
    - unresolved exploration
    - follow-up profile completion
    - rebuild normalized/reusable artifacts
- 已补 normalized/reusable candidate artifact 提炼：
  - `build-company-candidate-artifacts` 现在会输出：
    - `materialized_candidate_documents.json`
    - `normalized_candidates.json`
    - `reusable_candidate_documents.json`
    - `manual_review_backlog.json`
    - `profile_completion_backlog.json`
  - 新增 `profile_completion_backlog`，用于显式管理“已有 LinkedIn URL 但尚未拿到 full detail”的候选人
- Thinking Machines Lab 当前物化结果已更新：
  - snapshot: `20260406T172703`
  - `candidate_count=55`
  - `evidence_count=75`
  - `status_counts={current: 29, former: 25, lead: 1}`
  - `manual_review_backlog_count=2`
  - `profile_completion_backlog_count=25`
- 当前 backlog 的含义已明确：
  - `manual_review_backlog`
    - `Horace He`: unresolved lead with homepage/CV evidence
    - `Tianle Li`: current candidate but missing individual LinkedIn URL
  - `profile_completion_backlog`
    - 主要是 Thinking Machines Lab former candidates
    - 当前设备上 `Harvest profile-scraper` 未启用，因此被保留为待补全资产，而不是被静默忽略
- 已确认当前设备的 provider 状态：
  - `harvest.profile_scraper.enabled = false`
  - `harvest.profile_search.enabled = false`
  - `harvest.company_employees.enabled = false`
  - 因此当前阶段依赖：
    - 已有 live snapshot / provider cache / manual review asset
    - 而不是新发起 live Harvest detail requests
- 已补测试覆盖：
  - `tests/test_harvest_connectors.py`
  - `tests/test_candidate_artifacts.py`
  - `tests/test_company_asset_completion.py`
  - 针对性测试通过

### GitHub 同步与跨设备接手

- 已将 `Sourcing AI Agent Dev/` 整理为 monorepo 根目录，准备同步到 private GitHub repo：
  - 代码、文档、示例配置、去敏后的历史方法论资产进入 Git
  - `runtime/`、`providers.local.json`、历史 `api_accounts.json`、zip/tar 原包不进入 Git
- 已补根目录 onboarding/sync 文档，降低后续在公司电脑或其他 AI 环境下继续开发的接手成本：
  - 根目录 `README.md`
  - 根目录 `ONBOARDING.md`
  - 根目录 `GITHUB_SYNC_PREP.md`
  - `docs/DEVELOPMENT_GUIDE.md`
- 已明确 GitHub repo 不是 runtime asset storage：
  - live payload、company snapshots、profile raw assets、manual review raw assets 需要单独安全存储
  - 换设备后应恢复 secrets 和必要 runtime 子集，而不是依赖 Git 自动同步
- 已补跨设备可恢复的安全资产同步设计：
  - 新增 `docs/CROSS_DEVICE_SYNC.md`
  - 明确 GitHub / secret manager / cloud object storage / local runtime 四层分工
  - 明确哪些资产应该 durable sync，哪些只应本地保留
  - 明确后续实现切入点：
    - `asset_bundle_manifest`
    - `export-asset-bundle / restore-asset-bundle`
    - exported SQLite snapshot
    - cloud asset registry
- 已将跨设备恢复方案落成可执行命令：
  - 新增 `asset_sync.py`
  - CLI 新增：
    - `export-company-snapshot-bundle`
    - `export-company-handoff-bundle`
    - `export-sqlite-snapshot`
    - `restore-asset-bundle`
  - 当前 bundle 形态为：
    - `bundle_manifest.json`
    - `export_summary.json`
    - `payload/<runtime_relative_path>`
  - 可直接作为后续 object storage durable sync 的上传单位
- 已完成 Thinking Machines Lab handoff bundle 的真实导出与恢复验证：
  - handoff bundle:
    - `runtime/asset_exports/company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z/`
    - `569` files
    - `29346219` bytes
  - sqlite snapshot:
    - `runtime/asset_exports/sqlite_snapshot_sourcing_agent_db_20260406T125538Z/`
  - restore smoke test:
    - `/tmp/sourcing-agent-restore-smoke`
- 已完成 object storage sync 第一版实现：
  - 新增 `object_storage.py`
  - 当前 provider:
    - `filesystem`
    - `s3_compatible`
  - 当前 CLI 新增：
    - `upload-asset-bundle`
    - `download-asset-bundle`
    - `restore-sqlite-snapshot`
  - 当前本地 smoke test 已跑通：
    - handoff bundle upload
    - handoff bundle download
    - sqlite snapshot upload
    - sqlite restore helper
- 已完成真实 Cloudflare R2 配置接入与 live 验证：
  - 当前使用 Cloudflare R2 的 S3-compatible endpoint，而不是 dashboard 链接
  - `sqlite_snapshot` 已成功真实上传到 R2
  - 同一 `sqlite_snapshot` 已成功从 R2 下载回来
  - 说明当前：
    - endpoint
    - region=`auto`
    - access key / secret key
    - SigV4 签名逻辑
    都可用
  - Thinking Machines Lab `company_handoff` 大 bundle 已完成真实 durable sync 闭环：
    - real upload:
      - `bundle_id=company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z`
      - `571` uploaded objects
      - `29570059` bytes
      - `max_workers=8`
    - real download:
      - `571` downloaded objects
      - `29570059` bytes
      - `max_workers=8`
    - local restore from downloaded bundle:
      - restored to `/tmp/r2-tml-handoff-restore`
      - `569` runtime files restored
- 已完成 object storage sync 第二版优化：
  - `upload-asset-bundle / download-asset-bundle` 已支持并发 `max_workers`
  - 已补 per-object retry/backoff
  - `S3CompatibleObjectStorageClient` 已改成 thread-local session
  - 已补 local/remote sync metadata：
    - local: `runtime/object_sync/bundle_index.json`
    - local: `runtime/object_sync/runs/*.json`
    - remote: `indexes/bundle_index.json`
    - remote: `indexes/sync_runs/*.json`
- 已补恢复教程：
  - 新增 `docs/RECOVERY_TUTORIAL.md`
  - 明确同机换账号、新机器恢复、bundle 上传下载、SQLite 恢复的具体命令
- 已明确 Thinking Machines Lab retrospective 当前处于“已完整复盘、待继续补全资产”状态：
  - 当前没有已完成但未落盘的关键测试结论
  - 后续仅在新增 TML live execution 或新增资产时继续更新
- 已补结构化交接文档：
  - 新增 `docs/HANDOFF_2026-04-06.md`
  - 用结构化 handoff 取代保留冗长聊天记录，降低新 AI session / 新设备接手成本
- 已明确后续服务器化/云端资产化方向：
  - 服务运行位置迁移到长期运行的服务器
  - provider secrets 由 secret manager 注入
  - 高价值 runtime 资产进入 object storage durable storage
  - 相似用户意图优先复用既有资产，再做 plan / filtering / rerank / presentation
- 已明确当前 GitHub repo 已足够作为代码仓库：
  - 不再建议创建第二个“包含 secrets 和 runtime 的完整 GitHub repo”
  - secrets 与高价值数据资产继续采用 secret manager + object storage 分层存储
- 已新增显式待办清单：
  - `docs/NEXT_TODO.md`
  - 用于切换账号/设备后继续执行：
    - Thinking Machines Lab 后续资产补全
    - normalized asset / reusable candidate artifact 提炼
    - object storage 的 resume/progress 继续优化
    - 服务器化/云端资产化方向继续推进

### 已记录待办

- `worker daemon` 的真实 systemd 安装与启用延后到正式服务器环境：
  - 当前开发环境是 WSL，本地可生成 unit 并验证 service 壳层逻辑
  - 但不把 `/etc/systemd/system` 安装视为当前开发阶段目标
  - 等正式上线到长期运行的 Linux 服务器后，再执行安装、`enable --now` 和运维接入
- 新增 source family 的产品化流程需要保持“先交互、后规划、再开发/测试”的阶段门：
  - 先确认用户的目标、偏好、覆盖范围、执行深度、成本容忍度
  - 再进入 source onboarding review 和具体链路实现
  - 新链路必须继续满足数据资产意识、可审计性、可扩展性要求

### 已记录的产品决策

- 明确将当前项目继续泛化为通用 sourcing workflow，而不是 Anthropic / xAI 特例扫描器
- 已实现 `AcquisitionStrategyCompiler`
  - plan 阶段可输出 `full_company_roster / scoped_search_roster / former_employee_search / investor_firm_roster`
  - 明确低成本优先的 slug resolution 顺序：relation check / web search -> LinkedIn people search API -> profile detail API
  - 对大公司查询会给出 scope confirmation points，例如 `Google Gemini` 默认建议缩到 `Google DeepMind / Gemini`
  - 已编码 HarvestAPI 成本规则：
    - 已知 URL 时默认 route 到 `linkedin-profile-scraper`
    - `linkedin-company-employees` 只适用于批量场景
    - `linkedin-profile-search` 仅作为低成本 web search 不足时的 fallback
- 已实现 `PublicationCoveragePlanner`
  - plan 阶段可输出 publication source families、seed queries、LLM extraction role、fallback steps
  - 当前将 official research / engineering / blog / docs / publication platforms 作为标准 coverage family
- 记录后续 Thinking Machines Lab 端到端验证的高质量 connector 策略：
  - HarvestAPI LinkedIn profile search，用于按意图定向检索
  - HarvestAPI LinkedIn company employees，用于获取高质量公司 roster
  - HarvestAPI LinkedIn profile scraper，用于按 LinkedIn URL 获取 full profile detail
- 明确 HarvestAPI 使用策略：
  - 默认不需要 email
  - profile scraper 默认 `Full` 模式，避免因 detail 不完整而重复调用
  - 可利用 `moreProfiles` 扩展相同公司经历或相近背景的人选
  - 因成本较高，仅在最终验证或人工确认后调用
- 明确后续产品化重点：
  - 早期阶段要根据用户意图制定 acquisition strategy，而不只是固定 company roster
  - 后期 retrieval 要升级为多层过滤与多置信度结果输出
  - 高价值 LinkedIn Profile 与最终 result artifact 未来进入云端存储，本地保留 workflow 执行态与调试缓存
- 明确 publication enrichment 的升级方向：
  - 当前 `arXiv affiliation -> author / acknowledgement -> co-author` 只能作为 baseline
  - 后续需要让 LLM 先做 source coverage planning，确保 coverage 不只包含 arXiv，也包括 official research / engineering / blog / docs 等 source families
  - 对弱结构化的 author / acknowledgement / contributor 文本，允许 LLM 参与抽取与归一化
  - 仍保留 deterministic fallback，避免模型不可用时中断工作流
- 明确 retrieval 产品策略：
  - 默认输出 `hybrid` 结果
  - 同时支持 `structured_only` 与 `semantic_heavy`
  - 结果按 `high confidence / medium confidence / lead only` 分层呈现，供用户按 precision / recall 需求选择
- 已完成执行层改造第一步：
  - acquisition runtime 不再只会执行 company roster，而是会按 strategy 分叉
  - `scoped_search_roster / former_employee_search` 已接入 low-cost search-seed acquisition
  - enrichment 的 slug/profile 解析已改为 `web-first, paid-fallback`
- 已补 Harvest profile-scraper adapter 基础层：
  - 仅在已知 LinkedIn URL 且配置了 Apify token 时启用
  - 默认 `Full`
  - 默认不抓 email
  - 作为 high-cost provider 中成本最低的一档 detail connector
- 已补 publication lead 的 second-pass enrichment：
  - publication / acknowledgement 发现的 lead 不再只停留在 `lead` 文档层
  - 在剩余 enrichment budget 内，会继续尝试解析 LinkedIn profile
  - 若 profile 验证通过，可将 `lead` 升级为 `employee` 或 `former_employee`
- 已补 `docs/DATA_ARCHITECTURE.md`
  - 明确当前 canonical schema、snapshot 资产类型、审计链路
  - 明确仍缺 `confidence persistence / criteria self-evolution / cloud asset registry`
- 已补 `further exploration` 模块：
  - 对 unresolved lead 做低成本网页探索，而不是默认直接花费 LinkedIn search 成本
  - 可从网页、X、GitHub、个人主页、CV 中回收 profile 线索
  - 若探索后发现 LinkedIn URL，可重新进入 detail enrichment
- 已补 criteria evolution persistence 第一版：
  - 新增 `criteria_feedback` 和 `criteria_patterns`
  - 新增 API/CLI 入口写入人工 review feedback
  - `accepted_alias` 这类 feedback 已可影响后续 scoring
- 已补 criteria audit persistence：
  - 新增 `criteria_versions`
  - 新增 `criteria_compiler_runs`
  - plan / workflow / retrieval 都会留下 criteria 编译版本与 provider 记录
- 已补 confidence persistence：
  - `job_results` 新增 `confidence_label / confidence_score / confidence_reason`
  - retrieval artifact 和持久化结果都会带上 `high / medium / lead_only`
- 已补 model-agnostic page analysis：
  - `further exploration` 不再把页面摘要能力写死为 Qwen
  - 当前通过通用 `analyze_page_asset` 接口接入模型，保留 deterministic fallback
- 已显式固化 raw-first / compact-context 规则：
  - 外部 API 返回、网页原文、analysis input/output 默认先落盘
  - 模型默认只读取 compact excerpt，而不是直接吃整页 HTML 或大体积 raw payload
  - `.pdf` 等 binary-like URL 默认不直接送入页面分析上下文
- 已补 centralized asset logger：
  - 新增 `asset_logger.py`
  - snapshot 下统一维护 `asset_registry.json`
  - company roster、search seed、LinkedIn profile、Harvest profile、publication raw page、exploration page、analysis input/output 统一登记
- 已补跨进程 worker recovery daemon：
  - `agent_worker_runs` 新增 `lease_owner / lease_expires_at / attempt_count / last_error`
  - 新增 recoverable worker 扫描、claim、renew、release
  - 新增 `PersistentWorkerRecoveryDaemon`
  - `stale running` worker 现在会被当作可恢复 worker 重新进入 scheduler
  - 已补 CLI/API 控制面：
    - CLI: `show-recoverable-workers / run-worker-daemon-once / run-worker-daemon`
    - API: `GET /api/workers/recoverable / POST /api/workers/daemon/run-once`
  - 已补跨连接测试，验证 DB lease 可协调独立进程式恢复器
- 已补系统级 daemon service 壳层：
  - 新增 `service_daemon.py`
  - 支持单实例 lock、心跳状态、优雅退出
  - 支持 `run-worker-daemon-service`
  - 支持 `show-daemon-status`
  - 支持 `write-worker-daemon-systemd-unit`
  - 新增 API：`GET /api/workers/daemon/status`、`POST /api/workers/daemon/systemd-unit`
  - 新增测试 `tests/test_service_daemon.py`
- 已补 criteria auto-evolution loop 第一版：
  - feedback 写入后自动触发一次 criteria recompile
  - 新 criteria version 会保留 `parent_version_id / trigger_feedback_id / evolution_stage`
  - 新增 `/api/criteria/recompile` 与 `recompile-criteria`
- 已补 feedback -> rerun -> result diff：
  - `rerun_retrieval=true` 时会在 recompile 后自动执行 retrieval rerun
  - 新增 `criteria_result_diffs`
  - baseline job 与 rerun job 的 `added / removed / moved` 会被持久化并落成 diff artifact
- 已将 result diff 升级为双层 diff：
  - `rule_changes` 会比较 baseline / rerun 的 criteria version、pattern snapshot、request、plan
  - `result_changes` 会比较 rerun 前后的候选人进入、退出、排序与置信度变化
  - 新增 `impact_explanations`，将规则变化与结果变化合并成可读的审计解释
  - criteria version 现会保存包含 disabled pattern 在内的全量 pattern snapshot，避免事后解释被当前 active state 污染
- 已补候选人级影响归因：
  - diff 现包含 `candidate_impacts`
  - 会针对 `added / removed / moved` 候选人，归因到具体 pattern change、request change，及其对应的 matched field
  - 对 alias 这类规则变更，现可直接解释“哪条规则让哪个候选人进入结果”
- 已补 rerun gating + cost policy：
  - `rerun_retrieval` 现支持 `auto / cheap / full`
  - `auto` 会按 feedback 类型、baseline 规模和预估影响决定是否 rerun
  - `cheap` rerun 会收紧 `top_k / semantic_rerank_limit`，并强制使用 deterministic summary
  - 低信号 feedback 会被 gate 掉，避免无意义 rerun 和重复模型摘要成本
- 已补 baseline job / request-family 精确匹配：
  - rerun 不再默认选择“同公司最近完成 job”
  - 会先按 request-family signature 和 family score 匹配最接近的 baseline job
  - 仅在没有足够接近的 family match 时，才退回到 latest-company fallback
  - rerun 返回和 diff artifact 现会显式包含 `baseline_selection`
- 已补 confidence evolution：
  - `must_have_signal / false_negative_pattern` 会派生出 `must_signal + confidence_boost`
  - `exclude_signal / false_positive_pattern` 会派生出 `exclude_signal + confidence_penalty`
  - scoring 现会让这些 pattern 直接影响 `confidence_score / confidence_label / confidence_reason`
  - rerun diff 已能表现“结果未换人，但 confidence label 变化”的场景
- 已补 company-level confidence band evolution：
  - 会按同公司历史 feedback 统计 precision / recall pressure
  - 自动微调 `high / medium` band 边界，而不是只做 pattern 级加减分
  - 每次 retrieval / rerun 会把当时生效的阈值保存到 `confidence_policy_runs`
  - result artifact 现会附带 `confidence_policy`
- 已补 request-family confidence policy + time decay：
  - feedback 现会自动带上 `request_signature / request_family_signature`
  - confidence policy 默认优先按 request-family 过滤历史 feedback，而不是混用同公司所有 query
  - 老 feedback 会按时间半衰，避免旧项目经验无限累积污染当前 band
- 已补 feedback-derived auto pattern suggestion：
  - 当 feedback 带 `job_id + candidate_id` 时，会结合 `matched_fields` 与 candidate context 自动生成 pattern suggestions
  - suggestion 单独持久化为 `criteria_pattern_suggestions`，默认 `suggested`，不会直接写入 active patterns
  - `show-criteria` / `/api/criteria/patterns` / `record-feedback` 返回里现在都能看到 suggestions
- 已补 suggestion review loop：
  - 支持 `review-suggestion` CLI 和 `/api/criteria/suggestions/review`
  - suggestion 可被标记为 `applied / rejected`
  - `applied` suggestion 会写入 active patterns，并可继续触发 criteria recompile 与 retrieval rerun
- 已补 manual policy freeze / override：
  - 新增 `confidence_policy_controls`
  - 支持 `request_exact / request_family / company` 三种 scope
  - 支持 `freeze_current / override / clear`
  - retrieval 会按 `request_exact -> request_family -> company` 优先级选择 active control
  - active control 会写入 retrieval artifact，方便后续审计“这次 band 是自动 policy 还是人工锁定”
- 已补 canonical request matching for manual controls：
  - confidence policy control 创建时会先 canonicalize request payload
  - 避免 raw request 与 runtime request signature 不一致，导致 freeze/override 无法命中
- 已补 Plan Review Gate：
  - plan 阶段会生成 `plan_review_gate`
  - 对 scoped roster、investor firm roster、高成本 source 等计划，workflow 会先返回 `needs_plan_review`
  - 已新增 `plan_review_sessions`
  - review 后可把 `extra_source_families / confirmed_company_scope / allow_high_cost_sources` 写回 approved plan
- 已补 Manual Review Queue：
  - retrieval 现在会自动产出 `manual_review_items`
  - 当前会优先收集 `lead_only`、缺少 LinkedIn profile、需要人工确认 membership 的候选人
  - 已新增 manual review 的 API / CLI review 入口
- 已补 Investor Firm Roster Workflow：
  - `investor_firm_roster` 不再只是 planning strategy
  - acquisition runtime 现可基于既有 investor 资产生成 tiered firm plan
  - 会先产出 firm tiering，再归一化 full investor roster，后续再做 role/involvement filter
- 已补模块级文档与操作示例：
  - 新增 `docs/MODULES.md`
  - 新增 `configs/confidence_policy_freeze.example.json`
  - 新增 `configs/confidence_policy_override.example.json`
  - 新增 `configs/suggestion_review_apply.example.json`
  - 新增 `configs/plan_review_approve.example.json`
  - 新增 `configs/manual_review_resolve.example.json`
- 已补 `LLM-driven Search Planner`：
  - plan 现会显式生成 `search_strategy`
  - query 会被编译成具名 bundle：`relationship_web / publication_surface / public_interviews / targeted_people_search`
  - “公开访谈 / Podcast / YouTube” 这类新 sourcing 方法，已经可以先沉淀为 source family，并进入 plan review 与执行链
- 已将 search planner 接入 search-seed acquisition：
  - `seed_discovery` 不再只消费裸 `search_seed_queries`
  - 会执行 query bundles，并对 `public_interviews / publication_and_blog` 生成 `public_media_lead`
  - 这些 lead 会进入后续 exploration / manual review，而不是直接丢失
- 已落地 semantic/vector retrieval 第一版：
  - 新增 `semantic_retrieval.py`
  - retrieval 现已真正执行 `structured hard filters + lexical/alias + sparse-vector semantic rerank + confidence banding`
  - 对 `post-train` 这类 lexical 不稳定的 query 变体，已能通过 semantic hit 召回候选人
- 当前 fully agentic sourcing copilot 的现状边界：
  - 已具备 model-assisted plan / search / page analysis / weakly structured exploration
  - 但 runtime 仍不是“会自动 handoff 给多个自主子 Agent”的执行架构
  - 后续若要进一步 agent 化，应增加 specialist lanes / handoff runtime / long-running agent state
- 已补 `agent runtime` 第一版：
  - 新增 `agent_runtime.py`
  - workflow / retrieval 已会生成 `agent_runtime_session`
  - acquisition / retrieval 过程已记录 `agent_trace_spans`
  - 当前 lane 为 specialist-lane runtime，而非自治 swarm；但 handoff、trace、runtime state 已具备
- 已补 `semantic provider` 第一版：
  - 新增 `semantic_provider.py`
  - 默认 `LocalSemanticProvider` fallback
  - 已可切换 DashScope/Qwen embedding + rerank
  - 当前配置面向 `text-embedding-v4 + gte-rerank-v2`，并预留 `qwen3-vl-rerank`
- 已将公开视频结果纳入低成本数据资产：
  - `public_interviews / publication_and_blog` query bundle 会保存 `public_media_results / public_media_analysis`
  - 当前先基于标题/摘要做初步关系判断，不主动抓 transcript
  - 若后续需要深挖访谈内容，可直接复用这些资产
- 已补 autonomous worker runtime 第一版：
  - `search_planner / public_media_specialist / exploration_specialist` 已支持并行 worker
  - worker 现会持久化 `budget / checkpoint / output / interrupt_requested`
  - 同一个 `job_id + lane_id + worker_key` 已支持 checkpointed resume
  - acquisition state 已把 `job_id / plan_payload / runtime_mode` 下传到 search/exploration worker
- 已补 worker 控制面：
  - CLI 新增 `show-workers / interrupt-worker`
  - API 新增 `GET /api/jobs/{job_id}/workers` 与 `POST /api/workers/interrupt`
  - `GET /api/jobs/{job_id}/results` 与 `GET /api/jobs/{job_id}/trace` 现会附带 `agent_workers`
- 已补 worker 生命周期测试：
  - 新增 `tests/test_agent_runtime.py`
  - 已验证 `checkpoint -> interrupt -> resume -> complete`
  - 全量测试现为 `57` 个通过
- 已补 lane-aware scheduler 第一版：
  - 新增 `worker_scheduler.py`
  - scheduler 会按 lane priority、resume mode 和并行上限选择下一批 runnable worker
  - 当前优先恢复 `reuse_checkpoint / resume_from_checkpoint`，再执行 fresh worker
  - 已新增 `GET /api/jobs/{job_id}/scheduler` 与 CLI `show-scheduler`
- 已补 checkpoint 恢复语义：
  - public media analysis 现在会把 `completed_urls + analysis_map` 写入 checkpoint
  - exploration worker 现在会把 `completed_queries + gathered_signals + result_summaries` 写入 checkpoint
  - 这样 interrupted/completed worker 再次执行时，不会丢失已经沉淀的中间状态
- 已补 autonomous worker daemon loop：
  - 新增 `worker_daemon.py`
  - search/public media/exploration worker 现在统一通过 daemon loop 执行
  - daemon 会执行 `resume/retry loop + lane budget arbitration`
  - failed worker 现在会按 retry limit 自动重试
  - completed worker 现在会优先复用已持久化 output，而不是重复请求外部 source
- 已补 lane budget caps：
  - cost policy 现新增 `search/public_media/exploration` 的 worker unit budget
  - scheduler summary 现会输出 `lane_budget_caps`
  - daemon 会记录 `lane_budget_used`
- 已补 daemon 测试：
  - 新增 `tests/test_worker_daemon.py`
  - 已验证 `failed -> retry` 与 `budget exhausted -> backlog`
- 全量测试现为 `62` 个通过

## 2026-04-05

### 已完成

- 解压 `Sourcing AI Agent Dev` 下 4 个压缩包，并确认核心材料分布
- 阅读 `Anthropic华人专项` 的 `README.md`、`PROGRESS.md`、`api_accounts.json`、`company_ids.json`
- 阅读 `anthropic-employee-scan`、`investor-chinese-scan`、`biz-visit-onepager` 的 `SKILL.md`
- 确认当前沉淀出的可产品化资产：
  - Anthropic 主工作簿：在职华人员工 / 已离职华人员工 / 投融资历史 / 主要投资方华人成员
  - Scholar 扫描结果
  - 投资机构成员原始 JSON
  - 项目方法论、阶段门、审计要求
- 新建 `sourcing-ai-agent/`，开始实现后端 MVP
- 完成后端 MVP 第一版实现：
  - `AssetCatalog` 自动发现解压资产
  - 标准库 `.xlsx` 解析器
  - SQLite candidate/evidence/job store
  - criteria-driven scoring
  - CLI 与 HTTP API
- 将项目从 Anthropic 特例扩展为通用 sourcing workflow engine：
  - 新增 planning 层，显式输出 `SourcingPlan`
  - 新增 acquisition task 抽象
  - 新增 workflow job，显式阶段：`planning` / `acquiring` / `retrieving` / `completed`
  - 新增 retrieval strategy 抽象：`structured` / `hybrid` / `semantic`
  - API 新增 `/api/plan`、`/api/workflows`、`/api/providers/health`
- 接入 Qwen provider：
  - 本地 secret 读取：`runtime/secrets/providers.local.json`
  - Provider 支持 DashScope Responses API
  - 加入 timeout fallback，模型超时时自动退回 deterministic 实现
- 完成真实链路验证：
  - bootstrap 导入 `258` 位候选人
  - demo job 1：基础设施方向当前 Anthropic 员工，Top 1 为 `Da Yan`
  - demo job 2：直接参与 Anthropic 投资决策的华人投资方成员，返回结构化结果
  - demo workflow 1：`xAI` 进入 `blocked@acquiring`，明确暴露缺失 connector
  - demo workflow 2：`Anthropic` 完整跑通 `planning -> acquiring -> retrieving -> completed`
- 完成 Qwen 联通验证：
  - `test-model` 返回 `QWEN_OK`
  - `plan` / `workflow` 可使用 Qwen 生成 intent summary，失败时自动回退
- 完成自动化验证：`PYTHONPATH=src python3 -m unittest discover -s tests -v` 全部通过
- 落地 connector 第一版：
  - 新增 live company identity resolver
  - 新增 RapidAPI 账号自动发现与排序
  - 新增 LinkedIn `company/people` roster connector，支持多账号 fallback、429 熔断、分页落盘、重复页检测
  - 新增 `runtime/company_assets/{company}/{snapshot_id}` snapshot 目录约定
- 完成真实 xAI acquisition + retrieval 验证：
  - `account_014` 可稳定拉取 xAI roster
  - 首次 xAI workflow 获取 `98` 条 roster 行，其中 `97` 条可见、`1` 条 headless
  - 归一化后向 SQLite 写入 `97` 个 xAI baseline candidates
  - 中文 infra criteria 已可通过 alias expansion 命中英文 headline
  - demo workflow 返回 `Jake Palmer / Jesik Min / Neal Bayya` 等结构化结果
- 完成 enrichment connector 第二版：
  - 新增 provider-first slug resolution：`search/people -> /api/profile -> profile detail`
  - 新增 publication author / acknowledgement / co-author baseline connector
  - 新增跨 snapshot search/basic profile 缓存复用，避免重复消耗 LinkedIn search quota
  - 新增 cached roster snapshot fallback：live roster 超限或空返回时，workflow 可继续复用最近一次成功的本地资产
  - 新增 enrichment 单测，`unittest` 总数增至 `8`
- 完成 enrichment 实测验证：
  - 在已有 xAI snapshot 上，top 5 infra 候选中成功解析 `Jake Palmer / Jesik Min / Neal Bayya` 的 LinkedIn profile detail
  - 最新 live xAI workflow 已能在 end-to-end 链路中落下 enriched artifact，`20260405T220122` snapshot 至少成功合并 `Jake Palmer` 的完整 LinkedIn profile
  - 发现真实约束：`search/people` 所在 z-real-time 账号配额已接近耗尽，后续 live 验证需要继续复用缓存或补充新账号

### 当前结论

- 现有项目已经具备产品雏形，核心不是“再写一次扫描脚本”，而是把已有流程产品化为：
  - 可重复运行的 job
  - 可持续扩展的 source adapter
  - 可审计的结果存储
- 技术栈先采用 Python 标准库，避免因为外部依赖阻塞 MVP
- 模型调用层做接口预留，不把 Codex / Claude 写死在业务逻辑里
- acquisition 已经不再只有本地 Anthropic asset：xAI 的 LinkedIn roster baseline 已能真实执行
- 当前“全量资产”仍是 baseline：受 LinkedIn headless 用户、页数上限、search quota、publication 覆盖率影响，覆盖率还不是最终上限
- 高价值资产的最终形态不应长期只放本地：LinkedIn Profile 和最终结果需要进入云端存储设计

### 正在推进

- 继续补 The Org / Hunter / Scholar / web adapters，并把现有 publication/co-author 真正用于 retrieval
- 设计外部 API 接入策略、云端资产存储方案和人工 review 阶段门

### 下一步

- 在小公司样本上做完整端到端验证，例如 Thinking Machines Lab
- 把 candidate document 向量化或语义索引纳入 retrieval 层
- 设计云端资产存储：本地保留执行日志，云端持久化 LinkedIn Profile 与最终结果 artifact
- 引入真正的 LLM criteria compiler
- 补前端 Demo

### 2026-04-06 Claude Relay 与 Thinking Machines Lab 准备

- 新增通用 `model_provider` 配置层：
  - `settings.py` 新增 `ModelProviderSettings`
  - `build_model_client(...)` 现优先选择通用 `model_provider`，未配置时再回退到 `qwen`
- 新增 `OpenAICompatibleChatModelClient`：
  - 面向 OpenAI-compatible `chat/completions` + `models`
  - 已完成 `summarize / interpret_intent / analyze_page_asset / plan_search_strategy / healthcheck`
  - 当前实现使用 `requests`，避免 relay 对默认 `urllib` 请求头的拦截
- 完成 Claude relay 联通验证：
  - `GET /v1/models` 成功返回 `claude-sonnet-4-6`
  - `POST /v1/chat/completions` 已通过 relay 成功返回结果
  - 项目内 `healthcheck_model()` 已显示 provider `ready`
  - 项目内 `interpret_intent(...)` 已成功通过 `claude-sonnet-4-6` 返回文本
- 新增 `tests/test_model_provider.py`
  - 覆盖通用 provider 选择与 OpenAI-compatible 响应解析
- 新增 `configs/demo_workflow_thinking_machines_lab.json`
  - 用于 Thinking Machines Lab 的 scoped roster 端到端测试起点
- 新增 `configs/demo_workflow_thinking_machines_lab_full_roster.json`
  - 用于 Thinking Machines Lab 全量 current roster acquisition
  - 当前建议纯 roster 采集时设置 `slug_resolution_limit=0`、`profile_detail_limit=0`
- 更新 README：
  - 将“Qwen 配置”改为“模型配置”
  - 明确 `model_provider` 的优先级和 Claude relay 的 base URL 形态
  - 补充 Thinking Machines Lab 的 `plan -> review -> start-workflow` 建议入口
- 新增 HarvestAPI live connector 基础层：
  - `HarvestProfileSearchConnector`
  - `HarvestCompanyEmployeesConnector`
  - `HarvestProfileConnector`
  - `former_employee_search / scoped_search_roster` 现可走 Harvest search fallback
  - `full_company_roster` 现可在允许高成本 source 时优先走 Harvest company-employees
- 完成 Harvest live 参数核对与最小调用验证：
  - `linkedin-profile-search` + `pastCompanies` + `excludeCurrentCompanies` 已成功调用，Thinking Machines Lab 最小预算测试返回 `0` 条结果
  - `linkedin-company-employees` 已成功调用，Thinking Machines Lab 最小预算测试返回 `10` 条当前成员
  - 已确认关键参数坑位：
    - `maxTotalChargeUsd` 不能低于 actor 最小允许值
    - company-employees actor 的 `profileScraperMode` 必须使用完整枚举字符串，如 `Short ($4 per 1k)`
  - company-employees actor 返回字段更接近 `firstName / lastName / currentPositions / location.linkedinText`，而不是旧假设中的 `fullName / headline`
- 完成 Thinking Machines Lab 全量 current roster live acquisition：
  - workflow `1d19590b1e4c` 已完整跑通 `planning -> acquiring -> retrieving -> completed`
  - 使用 Harvest `linkedin-company-employees` 获取 current roster
  - snapshot `20260406T160415` 共捕获 `25` 条 roster rows，其中 `25` 可见、`0` headless
  - 已将 `25` 位成员归一化进 SQLite，并生成 `candidate_documents.json / retrieval_index_summary.json`
  - 当前 live roster 质量良好：实测样本中名字、headline、location、LinkedIn URL 均完整
- 完成 Thinking Machines Lab former / detail 链路校正：
  - 已通过 Harvest live 对照确认 former employee search 更适合先用 `pastCompanies` 做 recall，不默认加 `excludeCurrentCompanies`
  - 两页 `pastCompanies=Thinking Machines Lab` live search 可返回 `19` 条 former leads，并命中 `Alexis Dunn`
  - 已修正 `linkedin-profile-scraper` 的 `profileScraperMode` 枚举，必须使用 `Profile details no email ($4 per 1k)`
  - 已修正 roster -> candidate 映射，`build_candidates_from_roster(...)` 现会保留 `linkedin_url / metadata.profile_url`
  - 已为 known-profile enrichment 增加 Harvest batch profile fetch，避免逐人串行 profile scrape
- 完成 Thinking Machines Lab prioritized current detail live acquisition：
  - 最新有效 workflow `3f5e25153ab0` 已完整跑通 `planning -> acquiring -> retrieving -> completed`
  - snapshot `20260406T165131` 成功落下 `12` 个 `harvest_profiles/*.json`
  - acquisition event 明确记录：`Resolved 12 profile details and matched 0 publications`
  - 合并后的 candidate assets 已成功吸收 profile detail 信息，包括：
    - vanity LinkedIn URL
    - education
    - work_history
    - moreProfiles metadata
  - 实测已成功 enrich 的代表成员包括：
    - `Mira Murati`
    - `Lilian Weng`
    - `Soumith Chintala`
    - `Andy Hwang`
    - `Saurabh Garg`
- 完成 Thinking Machines Lab publication supplementation：
  - enrichment 现已执行 official surface publication collection，而不再只依赖 Anthropic 本地 publication 或通用 arXiv affiliation
  - Thinking Machines Lab 官方 surface 已成功抽出 `4` 个 publication/blog leads：
    - `Kevin Lu`
    - `John Schulman`
    - `Jeremy Bernstein`
    - `Horace He`
  - publication raw/index/page assets 已落到 snapshot `20260406T172439/publications/official_surfaces`
  - publication lead 现会自动进入 targeted Harvest resolution，再进入 exploration fallback
  - 当前 TML publication lead 的 live 现状是：
    - targeted Harvest current/past exact-name search 可执行且可审计
    - 但在这 `4` 个 lead 上返回 `0`
    - exploration fallback 当前受 DuckDuckGo SSL 问题影响，仍保留为 unresolved lead / manual review 输入
- 完成 Harvest runtime 级 cache/live-test bridge：
  - `harvest_profile_search` 与 `harvest_company_employees` 现会按 payload hash 复用：
    - 当前 snapshot 本地 raw asset
    - `runtime/provider_cache/*`
    - `runtime/live_tests/*` 下历史手工 live 资产
  - 这样即便当前进程没有 Harvest token，也能把已有 live asset 重新接入 workflow，而不是重复调用或退化成 0 结果
  - 同时修复了 `HarvestProfileConnector.fetch_profiles_by_urls(...)` 中未定义 `take_pages` 的 batch profile bug
- 完成 Thinking Machines Lab former fallback 修复与复跑：
  - 新增 refreshed plan review `9`，former strategy 的 `provider_people_search_min_expected_results` 已从 `10` 提升到 `50`
  - former workflow 现已通过 Harvest cached live bridge 重新接回历史 `pastCompanies` live asset
  - 最新有效 former snapshot 为 `20260406T175245`
  - `search_seed_discovery/summary.json` 当前记录：
    - `entry_count=25`
    - `query="__past_company_only__"`
    - `mode="harvest_profile_search"`
    - `seed_entry_count=25`
  - 这批 former candidates 已成功归一化进 `candidate_documents.json`，其中包含：
    - `Alexis Dunn`
    - `Andrew Tulloch`
    - `Barret Zoph`
    - `Joshua Gross`
    - `Songlin Yang`
    - 以及其他 former leads
  - 这说明 current roster、current detail、publication supplement、former fallback 四条 Thinking Machines Lab 主链都已具备可复盘的端到端资产
- 完成 manual review resolution 正式写回链路：
  - `manual_review_resolution.py` 已接到 orchestrator/store，而不是只停留在 helper
  - 人工 review 现在可以：
    - 直接写回 `candidate`
    - upsert `evidence`
    - 在 `runtime/manual_review_assets/...` 下落盘 source manifest、fetched html、analysis input/output
  - 已对 Thinking Machines Lab 的 publication leads 做真实写回：
    - `Kevin Lu`：confirmed current employee，LinkedIn 已写回
    - `John Schulman`：confirmed current employee，homepage evidence 已写回
    - `Jeremy Bernstein`：confirmed current employee，homepage + CV evidence 已写回
    - `Horace He`：homepage 未直接确认 TML affiliation，保留为 unresolved lead
- 完成 publication lead low-cost-first 调整：
  - `targeted Harvest name search` 不再默认执行
  - 当前默认顺序改为：
    - slug/web exploration
    - manual review / unresolved lead 保留
    - 只有显式批准时才进入 targeted Harvest name search
- 完成 manual review / exploration 信号质量修正：
  - 页面信号提取已过滤 `static.licdn.com / gstatic / fonts` 等静态资源 URL
  - 避免像 Kevin Lu 这类 LinkedIn 页面把静态资源误写成 `media_url`
- 复核 DuckDuckGo SSL 问题：
  - 旧 snapshot 中的报错不是单纯的 `urllib` 代码问题
  - 已用新的 `requests + endpoint fallback` live 复测，当前环境对 DuckDuckGo HTML endpoint 依然会返回 `SSL: UNEXPECTED_EOF_WHILE_READING`
  - 结论是：
    - 当前网络路径对 DuckDuckGo HTML 搜索不稳定
    - low-cost web search 必须继续走 provider abstraction / best-effort 策略
    - unresolved lead 必须保留，不能因为搜索失败就静默丢弃
- 新增 Thinking Machines Lab 复盘文档：
  - `docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md`
  - 记录了：
    - Harvest 参数修正
    - current roster / current detail / former fallback / publication supplement 结果
    - manual review 写回结果
    - DuckDuckGo SSL 问题与后续建议
- 完成稳定 search provider abstraction 第一版：
  - 新增 `src/sourcing_agent/search_provider.py`
  - 当前 provider chain 支持 `serper_google -> duckduckgo_html`
  - `search_seed_discovery / slug_resolution / exploratory_enrichment` 已统一接到 provider abstraction
  - search raw payload 现按 provider 的 `html/json` 形态落盘，便于缓存复用、审计和后续替换 provider
  - 这意味着 low-cost search 不再写死到 DuckDuckGo HTML endpoint
- 补齐 search provider 回归测试：
  - 新增 `tests/test_search_provider.py`
  - 覆盖 DuckDuckGo HTML parsing、Serper parsing、provider chain fallback、response roundtrip
- 完成 monorepo 级 GitHub 同步准备：
  - 新增根目录 `README.md`
  - 新增根目录 `ONBOARDING.md`
  - 新增根目录 `.gitignore`
  - 新增 `docs/DEVELOPMENT_GUIDE.md`
  - 明确忽略：
    - 原始 zip 包
    - `runtime/` 数据资产
    - `providers.local.json`
    - 历史 `api_accounts.json`
    - SQLite / cache / Python 临时文件
  - 当前结论是可以先整理成 publish-ready monorepo，但不能把现有目录原样直接推到 GitHub
- 2026-04-10（并行与恢复稳定性更新）：
  - 修复 workflow 恢复门控：`run_queued_workflow` 与 daemon 恢复现在都覆盖 `running+acquiring`，不再只支持 `blocked+acquiring`。
  - 修复 Harvest company filter 归一化误改写：避免把 `past_companies=[google, deepmind]` 错误写成 `[google, google]`。
  - 同义 query 泛化去重：`-`/空格/下划线变体统一签名，避免 `Vision-language` 与 `Vision Language` 重复执行。
  - `company-employees` shard worker 改为并行提交，减少 current roster 侧串行等待。
  - former 与 current 采集并行化：
    - current roster queued 时并行触发 former seed。
    - 非 queued 同步路径也并行启动 former，并在 enrichment 前 join。
  - former queued 后自动阻塞等待恢复，以保证后续会进入增量 enrichment，而不是“former 到了但没有再吃进召回”。
  - paid fallback 增加 probe overlap 剪枝：高重叠 query 标记 `skipped_high_overlap`，降低大组织重复调用成本。
  - 2026-05-05 historical results-board contract follow-up: public lifecycle rendering now normalizes legacy current-snapshot-serving rows whose materialized counter proves completion while old board-visible patch mirrors still show zero. Frontend sync/facet/hydration freshness now shares `resultViewLifecycle.ts`, and function facet all-selected summary renders `全量`. Validation: frontend contract tests, targeted lifecycle tests, ruff, diff check, and frontend build passed.
  - 2026-05-05 candidate board/detail serving projection follow-up: paginated artifact repair/build now writes public serving card projections instead of normalized-only rows, preserving normalized/index semantics plus materialized profile/card fields. `authoritative_candidates` returns materialized shard records for detail reads; single and batch candidate detail overlay rank/confidence metadata without letting stale job-results or candidate-table fields override shard-owned timeline/avatar fields. `/candidates?lightweight=1` and `/results?include_candidates=1` now read materialized serving rows without raw LinkedIn timeline hydration, and the frontend candidate board requests lightweight pages by default. Remaining risk: raw profile timeline resolver still exists for legacy ranked-results/profile-completion/backfill paths; do not add new normal public-read dependencies on it. Validation: targeted results API tests, candidate artifact repair regression, ruff, and frontend build passed.
	  - 2026-05-05 public-read raw timeline quarantine follow-up: `/api/jobs/{job_id}/results` now serializes ranked results with materialized/summary fields only, so dashboard fallback and public results reads cannot trigger raw profile timeline parsing. The bare `/api/jobs/{job_id}/candidates` endpoint defaults to lightweight serving-card mode, and single-candidate public detail only enters legacy raw timeline hydration through explicit `hydrate_legacy_timeline=1` diagnostic opt-in. Internal `get_job_results(...)`, export, profile-completion, and backfill paths can still use the resolver deliberately. Added regression coverage for ranked `/results` API and public detail no-hydration behavior.
	  - 2026-05-05 overlay source-selection follow-up: job-scoped baseline+delta overlay reads now treat overlay rows as serving projection membership, not profile-field truth. If an overlay row is sparse, public candidate page/detail/profile-progress serialization loads the current and baseline materialized serving shards and merges their card/timeline/avatar/location fields before returning the row. This closes the reuse/history shape where a second run could select an overlay or historical job-scoped projection and show roster-preview cards even though the full materialized candidate record already existed. Regression coverage proves sparse Lovable overlay rows are enriched from materialized shards while raw timeline hydration remains disabled.
	  - 2026-05-05 raw candidate-doc fallback quarantine follow-up: normal public reads no longer auto-materialize or fallback to raw `candidate_documents.json`. `_candidate_source_allows_candidate_documents_fallback(...)` now requires explicit legacy opt-in metadata, old tests were moved to materialized serving fixtures, and completed-workflow current-snapshot repair now synchronizes candidate docs into serving artifacts before publishing the result view source. This closes the path where a historical/full-reuse board could appear correct only because `/dashboard` or `/candidates` rebuilt artifacts on read. Validation: targeted candidate-doc fallback tests passed, related asset-population source-selection slice passed, full `tests/test_results_api.py` passed with `131 passed, 1 existing thread-cleanup warning`, and ruff/diff-check passed for touched files.
	  - 2026-05-05 raw profile timeline default-off follow-up: internal `get_job_candidate_detail(...)` now defaults to no legacy raw timeline hydration, matching the HTTP public detail contract. `hydrate_legacy_timeline=1` remains the explicit diagnostic opt-in, and target-candidate export now calls detail in no-hydration mode before using its own export-only raw profile resolver for CSV/profile packaging. Added regression coverage for default internal no-hydration, explicit HTTP opt-in, and export not relying on implicit detail hydration. Validation: targeted candidate-detail/export regressions passed, full `tests/test_results_api.py` passed with `132 passed, 1 existing thread-cleanup warning`, and ruff/diff-check passed for touched files.
	  - 2026-05-05 job result-view consistency maintenance follow-up: added `audit-job-result-view-consistency [--job-id ...] [--company ...] [--apply]` to compare `job_result_view.snapshot_id`, job summary candidate source, and latest authoritative organization registry. The command defaults to read-only and only applies repoints for jobs with explicit full-local/full-asset reuse proof; scoped/delta/overlay jobs are reported as `manual_review_required` so maintenance cannot silently rewrite scoped history to the latest full-company snapshot. Regression coverage proves a full reuse job is auto-repoint eligible while a scoped overlay job remains manual-only.
	  - 2026-05-05 hot-cache serving artifact governance audit: added read-only `audit-hot-cache-serving-artifacts [--company ...] [--snapshot-id ...] [--limit ...] [--output ...]`. The audit scans local hot-cache serving manifests for missing manifest-referenced shard/page/backlog/aux files, orphan JSON files, and canonical serving source availability, then emits cleanup/rehydrate plans without changing public-read behavior. Regression coverage proves missing hot-cache shard emits a rehydrate plan from canonical artifacts while orphan-only drift emits cleanup-only guidance.
	  - 2026-05-05 hot-cache explicit cleanup operator path: added `cleanup-hot-cache-serving-artifacts [--company ...] [--snapshot-id ...] [--apply]`, defaulting to dry-run. The command delegates to the existing hot-cache cleanup/retention engine with explicit operator flags for compatibility-export cleanup, TTL, size budget, per-company budget, snapshot retention, and generation compaction, so cleanup remains maintenance-owned instead of request-time repair.
	  - 2026-05-05 hot-cache governance validation: `PYTHONPATH=src .venv-tests/bin/pytest tests/test_candidate_artifacts.py -q` -> `54 passed`; `PYTHONPATH=src .venv-tests/bin/pytest tests/test_cli.py -q` -> `53 passed`; `PYTHONPATH=src .venv-tests/bin/ruff check src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/cli.py tests/test_candidate_artifacts.py tests/test_cli.py` -> passed; both new CLI help commands and `git diff --check` passed.
	  - 2026-05-05 hot-cache heat/LRU and post-publish/hydrate compaction follow-up: hot-cache access markers now persist `access_count`, `first_access_at`, `last_accessed_at`, and a recency-weighted `heat_score`; `collect_hot_cache_inventory` and runtime summary expose `hottest_snapshots` for service-level governance. `publish-candidate-generation`, `hydrate_cloud_generation`, and generation-first imports now run a hot-cache governance cycle by default and return `hot_cache_governance`, with CLI opt-out via `--skip-hot-cache-governance`.
	  - 2026-05-05 hot-cache heat/LRU validation: `PYTHONPATH=src .venv-tests/bin/pytest tests/test_candidate_artifacts.py -q` -> `55 passed`; `PYTHONPATH=src .venv-tests/bin/pytest tests/test_cloud_asset_import.py -q` -> `10 passed`; `PYTHONPATH=src .venv-tests/bin/pytest tests/test_cli.py -q` -> `54 passed`; ruff passed for `artifact_cache.py`, `cloud_asset_import.py`, `orchestrator.py`, `cli.py`, and touched tests; new publish/import/hydrate CLI help and `git diff --check` passed.
	  - 2026-05-05 scripted environment guardrail follow-up: restored the local live UI boundary to `runtime` + `local_dev` on `4173/8765` and the scripted UI boundary to `runtime/test_env/openai_agent_delta_streaming` + `scripted` on `4185/8785`. During post-patch scripted regression, the Whisper late-webhook case exposed an SLO metric ambiguity rather than a recovery failure: the actionable watcher event arrived quickly, while the deliberately late duplicate webhook exceeded 30s and was incorrectly counted as terminal wakeup lag. Service metrics now split full audit lag, actionable wakeup lag, and late-duplicate audit lag; `max_remote_provider_event_lag_ms` gates actionable wakeup only.
	  - 2026-05-05 frontend contract/root-cause audit: candidate-board user state now keys only on `historyId/jobId`, not `snapshotId`; standalone results route was aligned with SearchFlow, hydration polling no longer restarts solely on snapshot changes, and the unused legacy `CandidateBoard` implementation was deleted. `SearchPage` keeps a latest renderable dashboard ref so transient completed/reuse result refresh failures do not clear an already-visible board and reset filters. Plan strategy display no longer infers `全量本地资产复用` from `dispatch_preview.strategy` alone; frontend fallback requires explicit execution semantics or reuse/coverage proof. Validation: `PYTHONPATH=src .venv-tests/bin/pytest tests/test_frontend_candidate_filters.py tests/test_frontend_plan_contract.py tests/test_frontend_dashboard_hydration.py -q` -> `13 passed`; `.venv-tests/bin/ruff check tests/test_frontend_candidate_filters.py tests/test_frontend_plan_contract.py tests/test_frontend_dashboard_hydration.py` -> passed; `cd frontend-demo && npm run build` -> passed.
	  - 2026-05-05 board-visible progress/detail serving convergence: harvest-prefetch board-visible delta apply now chunks large candidate deltas into durable `board_visible_delta_apply` items and writes canonical lifecycle after each completed chunk, so `delta_profile_materialized_count` / `delta_profile_board_visible_count` can advance event-by-event instead of jumping from an early patch to final current-snapshot serving. Candidate board hydration now keeps full-board rows lightweight and overlays rich detail only for the current page via `getCandidateDetailsBatch`; filtering, counts, pagination, and sync copy remain owned by canonical lightweight rows and lifecycle. Regression coverage pins chunked lifecycle progress and prevents frontend detail hydration from using `dashboard.candidates` / `visibleCandidates` as a global rich-load source.
	  - 2026-05-05 board-runtime contract follow-up: the candidate board now has a backend-driven `board_runtime_state` contract that owns sync numerator, publication phase, facet readiness, and filter scope across `/progress`, `/dashboard`, and `/candidates`. Frontend sync cards, hydration banners, and facet gating now prefer `board_runtime_state`; `result_view_lifecycle` remains a compatibility/serving-view contract rather than a competing board business source. Validation: frontend dashboard hydration, candidate sync summary, candidate filter contract tests, `ruff`, `py_compile`, and backend board-runtime/lifecycle regressions passed; one broader lifecycle batch still shows an existing tempdir cleanup warning in `tests/test_results_api.py` when run alongside the threaded workflow smoke.
	  - 2026-05-06 backend-filtered candidate paging closure: `/api/jobs/{job_id}/candidates` now accepts board filter parameters and filters the complete materialized served population before `offset/limit`, returning `filtered_candidate_count`, `applied_filter`, `filter_signature`, and `filter_contract.row_filter_scope=backend_filtered_served_population`. The candidate board now fetches the current backend-filtered page for active filters/page changes and treats local row hydration as cache only, so facet selections no longer show false empty/wrong counts when matches live outside the already-loaded window. Validation: targeted backend candidate-page filter test, board-runtime/patch regressions, frontend candidate filter/hydration/sync tests, ruff, and frontend build passed. Follow-up validation on 2026-05-06: fast scripted OpenAI Agent and clean Lovable live-roster smoke both passed with no expectation failures; the smoke gate now treats same-second preview/finalization timestamps as valid only when durable materialization and finalization boundaries are present, avoiding false failures from second-precision timestamps while still rejecting missing finalization boundaries.
	  - 2026-05-06 unified card-ready board streaming closeout: live roster and baseline+delta now share the same backend-driven board contract. Board-visible patch writers freeze `card_materialization_summary` at event time, so `/progress`, `/dashboard`, `/candidates`, and `/board-patches` no longer recompute old patch quality from final mutable overlays. Roster-only/basic shell rows are deferred from board-visible completion until profile/detail fields or explicit low-richness/needs-completion card state exists. Lovable scripted smoke now shows display-ready progression `23 -> 71 -> 119 -> 140` with first board visibility around `9s`; seeded OpenAI Agent baseline+delta shows baseline `300` plus delta card-ready progression `25 -> 73 -> 121 -> 125 -> 173 -> 221 -> 224 -> 272 -> 297`, ending at `597/597` with first board visibility around `8s`. Validation: seeded OpenAI and Lovable strict scripted smokes, `190` targeted backend/frontend/smoke tests, ruff, py_compile, and frontend build passed; the only observed warning is the existing temporary `job_locks` cleanup race in threaded results-api tests.
	  - 2026-05-05 scripted provider timing metrics follow-up: out-of-order profile batch detection now uses provider submit/remote-wait start (`provider_started_at`) rather than local recovery trace span start, because late webhook handling can create spans only when the terminal event is processed. This keeps Lovable/OpenAI scripted smoke gates measuring provider behavior instead of local observer timing. Validation: out-of-order service metric regressions, `ruff`, OpenAI hosted scripted smoke, and Lovable hosted scripted smoke passed.
	  - 2026-05-07 PG-only workflow confidence closeout: scripted/manual/browser workflow confidence no longer has a SQLite control-plane fallback. `isolated_hosted_test_runtime`, `run_simulate_smoke_matrix.py --runtime-dir`, `run_explain_dry_run_matrix.py --runtime-dir`, `seed_reference_smoke_runtime.py`, and `dev_scripted_openai_agent_delta.sh` require runtime-scoped PG-only env files with `postgres_only + shared_memory`; startup now prepares a safe `sourcing_scripted*` / `sourcing_simulate*` / `sourcing_replay*` / `sourcing_test*` schema and fails closed if PG is missing, unreachable, or unsafe. The manual launcher rejects the removed `--sqlite-control-plane` option. Validation so far: `tests/test_scripted_test_runtime.py` -> `20 passed`; `tests/test_run_simulate_smoke_matrix.py` -> `7 passed`; representative hosted smoke now reaches business explain-matrix expectation failures instead of fake-DSN SQLite fallback. Remaining follow-up: migrate the older hand-rolled hosted smoke harness fully onto shared `isolated_hosted_test_runtime` and separately fix existing explain-matrix expectation drift.
	  - 2026-05-11 event-level drain budget follow-up: Google large baseline + large shard pressure smoke exposed a slow non-total recovery phase where `profile_refill_event_level_materialization_followup` serialized heavy `local_apply_closure` and `board_visible_delta_apply` work into one measured tick. `_drain_event_level_materialization_for_job(...)` now records local-apply elapsed time and skips same-phase board-visible apply when local apply already exceeds `EVENT_LEVEL_BOARD_VISIBLE_AFTER_LOCAL_APPLY_BUDGET_MS` (default 12s), leaving the durable board-visible item for the next bounded recovery/service phase. This preserves user-visible eventual progress while keeping recovery phases small-step and measurable.
	  - 2026-05-20 Phase 12 scripted matrix closeout: full PG-backed nightly long-latency matrix passed strict for all 5 scripted cases (`full_matrix_contract_rerun4`). The remaining Lovable failure was a scheduler accounting bug, not a real provider-slot underuse: a repeated submit containing already provider-owned `planned_dispatch` URLs hit registry lease contention and was incorrectly written back to `deferred_budget`. `_execute_harvest_profile_batch_worker(...)` now rechecks contended URLs against the same provider-owned/same-scope contract used by `_partition_already_queued_profile_urls(...)`, reports them as queued, and only records true registry lease contention as deferred. Targeted regressions cover provider-owned planned dispatch, provider-owned race, provider-owned lease contention, and true lease contention.
	  - 2026-05-20 projection layering checkpoint follow-up: the Google large matrix exposed that `projection_facet_layering_build` was bounded by chunk/time, but its state file persisted full `processed_records`; Google large produced a 37MB checkpoint for 7384 candidates. The builder now checkpoints compact `layer_assignments` during partial progress, strips assignments on completion, and reconstructs final overlay rows only at publication time. This preserves the public overlay/layering contract while preventing checkpoint/report paths from carrying full candidate payloads.
	  - 2026-05-20 containerized gate environment follow-up: added `make docker-start`, `make docker-doctor`, `scripts/docker_start.sh`, `scripts/docker_doctor.sh`, and updated `docs/TEST_ENVIRONMENT.md` with Docker Desktop/Colima setup. Local Docker CLI + Colima are installed and the Make/Testcontainers path now exports the current Docker context socket, uses the ECR mirror for Postgres, and disables Ryuk for explicit-cleanup contract tests. `make docker-doctor`, `make ci-pg-contract`, and `make ci-workflow-fake-provider` pass locally.
	  - 2026-05-20 containerized frontend browser gate follow-up: added `make ci-frontend-browser-gate`, `tests/testcontainers_frontend_browser_gate.py`, and `frontend-demo/scripts/run_projection_board_probe.mjs` / `npm run browser:projection-board`. The gate starts disposable PG, seeds a canonical `run_scope_projection`, builds the projection person search index, starts the real backend API plus Vite preview in same-origin mode, and drives `/projections/{projection_id}` with Playwright. It validates canonical sync count `30/30`, first page render, page 2 pagination, backend-filtered search, and `filter_contract.fallback_used=false`. Validation: `make ci-frontend-browser-gate`, `py_compile`, `ruff`, `npm run browser:check --prefix frontend-demo`, and `git diff --check` for touched gate files passed.
	  - 2026-05-20 projection public-reader fallback cleanup: active `/api/projections/{projection_id}/candidates` filters now fail closed with `projection_person_search_index_unavailable` when `projection_person_search_index` is missing, instead of scanning all `serving_projection_members` as a request-path legacy fallback. The old scan path is now explicit migration/debug opt-in via `SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK=1` and remains report-visible if used. This narrows the public-reader boundary: unfiltered pages may page membership rows; active search/filter must use the index or report not-ready. Validation: `tests.test_serving_projection_writer`, projection/person/CRM API contract tests, targeted projection index recovery tests, ruff, py_compile, and `make ci-frontend-browser-gate` passed.
	  - 2026-05-20 containerized pre-release aggregation follow-up: added `make ci-containerized-pre-release` as the current required aggregation target for Docker preflight, disposable PG storage/writer contracts, fake-provider webhook/object-store integration, and frontend projection-board browser validation. Added `.github/workflows/containerized-pre-release.yml` so the aggregation is no longer only documented; CI bootstraps Python, frontend dependencies, Playwright, and then runs the same Make target from the `sourcing-ai-agent` workspace. This closes the single-command pre-release entrypoint gap without pretending the fake-provider workflow is already browser-driven end-to-end. Remaining Phase 12 work is a workflow-driven browser gate that starts a run through the fake provider and opens the resulting projection. Validation: `make ci-containerized-pre-release` passed locally under Colima; workflow YAML was syntax-reviewed locally but not executed by GitHub Actions in this session.
	  - 2026-05-20 legacy public-reader retirement coverage follow-up: expanded the cutover regression from `/api/jobs/{job_id}/results` to all old public read endpoints that can otherwise compose board state (`/results`, `/dashboard`, `/candidates`, and candidate detail). With `SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS=1`, each returns `410` plus the canonical projection pointer and `read_contract.fallback_used=false`. Validation: `./.venv-tests/bin/python -m unittest tests.test_projection_crm_api_contracts -v` passed.
- 2026-05-22 01:35 CST durable runtime W2b profile-refill submit cutover:
  - LinkedIn profile prefetch/refill submit chunks now plan `CommandPlanRequested` through `DurableRuntimeWriter` and materialize `workflow_commands(command_type='linkedin.profile_refill.submit_batch')` before provider submit.
  - Added `run_linkedin_profile_refill_submit_command_once(...)` as the `linkedin_profile_owner` execution entrypoint. The existing scheduler calls this owner entrypoint synchronously for now, but command claim/running/provider-submit/terminal recording semantics are no longer scheduler-local code.
  - Matching claimed/running/retry/terminal commands now block duplicate provider submit and return `runtime_command_contention=true` evidence instead of silently resubmitting.
  - Added `legacy_job_workflow_run_id`, `legacy_job_operation_id`, and scoped profile-refill submit idempotency helpers. Submit idempotency includes job, snapshot, URL set, and submit scope so retry waves are not suppressed by a prior normal-wave command.
  - Public profile-prefetch summaries now expose `workflow_commands`, `workflow_command_count`, and status counts for signoff visibility.
  - Remaining gap: the owner entrypoint is still drained synchronously by the scheduler. Next W2b follow-up should move it into a daemon/service loop so the scheduler only emits events/commands and wakes the owner.
- 2026-05-22 02:20 CST durable runtime W2b owner phase closeout:
  - Split profile-refill command planning from provider-submit execution inside recovery/service ticks. `_run_profile_prefetch_refill_queue_once(...)` now uses `execute_profile_refill_submit_commands=false` and reports `planned_command_count` / `planned_url_count`; it no longer drains typed commands itself.
  - Added the distinct `profile_refill_command_owner` recovery phase owned by `linkedin_profile_refill_command_owner`. It drains ready `linkedin.profile_refill.submit_batch` commands and reports `command_count`, `executed_command_count`, `dispatched_url_count`, and `queued_worker_count`.
  - Same-tick refill scanning now treats planned commands as a handoff signal (`profile_refill_command_handoff_to_owner`) so the daemon does not re-scan and re-plan the same registry rows before the owner phase runs.
  - Service heartbeat/activity compaction now includes planned profile-refill command counts and owner execution counts, so a tick that only plans commands is not misclassified as idle.
  - Validation: `py_compile` and `ruff` for touched runtime/recovery/service/tests; targeted durable/enrichment/recovery/service suite (`20 passed`); `make ci-pg-contract` passed. Full PG-backed nightly matrix remains deferred until W2c/W4 boundaries are tighter.
- 2026-05-22 03:10 CST durable runtime W2c.1 profile URL terminal-state owner:
  - Added the typed `linkedin.profile_url_terminal.record` command contract, owner registry mapping, and idempotency helper. The idempotency key is based on job, snapshot, terminal scope, and normalized terminal entries so replayed profile-worker chunks do not duplicate URL terminal writes.
  - Harvest profile terminal persistence now plans reducer-owned `CommandPlanRequested` events and executes the `linkedin_profile_url_terminal_record_command_owner` path to mutate `linkedin_profile_registry`. Normal ControlPlaneStore paths fail closed/report-visible if command planning fails; claimed/running terminal-record commands leave the worker recoverable instead of advancing `terminal_persist_progress` without registry proof.
  - Added `drain_linkedin_profile_url_terminal_record_commands(...)` and a distinct `profile_url_terminal_record_command_owner` recovery/service phase. Recovery/service summaries now expose `command_count`, `executed_command_count`, `recorded_count`, `fetched_count`, and `failed_count`, and service activity/cumulative summaries count terminal-record work as real daemon activity.
  - Profile-worker terminal-persist progress now carries cumulative terminal-record command evidence across bounded partial replays, so reports can prove all chunks reached registry terminal state before local apply/board-visible follow-up.
  - Validation so far: `py_compile` and `ruff` passed for touched runtime/recovery/service/test files; targeted durable/enrichment/recovery/service tests passed (`14 passed`). Broader W2b/W2c targeted suite is next. Full PG-backed nightly matrix remains intentionally deferred until local apply and board-visible command owners are tighter.
- 2026-05-22 W3b/W6 durable runtime command-owned payload checkpoint:
  - Adopted W6 方案 A for W2c normal paths: local apply, board-visible publish, projection person-search index build, and collection authoritative merge now store full execution payloads in `workflow_commands`; `legacy_materialization_item` is emitted only for report-visible migration-adapter commands.
  - Removed the unused collection-authoritative legacy enqueue helper so new run-scope projection publication cannot accidentally write `collection_authoritative_merge` rows on the normal path.
  - Tightened W3b completion policy semantics: `serving_finalized` is now a hard completion gate backed by `workflow_current_state.completion_proofs.serving_finalized`. `job.status=completed` is not enough for workflow terminal truth without typed policy proof.
  - Updated `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md` and `docs/NEXT_TODO.md` to record W6 方案 A, command-owned normal-path payloads, migration-adapter-only legacy refs, and the remaining signoff/deletion scope.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_completion_policy.py tests/test_workflow_completion_policy.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_workflow_completion_policy.py -q` -> `5 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_durable_runtime.py::LegacyMaterializationAdapterTest tests/test_durable_runtime.py::DurableRuntimeStorageTest -q` -> `25 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_results_api.py -k "terminal_proof or projection_event_time or collection_authoritative_merge or projection_person_search_index" -q` -> `13 passed, 268 deselected`
  - Remaining: full W6 signoff still needs targeted OpenAI/Lovable/Google, full PG-backed scripted matrix, fake-provider/containerized/browser gates, and service metrics proof that `normal_path_write_count=0` and `legacy_bridge_used=false`. Legacy `job_materialization_items` remains available only for historical migration-adapter input until that signoff passes.
- 2026-05-22 W6 scheme-A signoff preflight expansion:
  - Extended command-owned payload migration to the remaining W6 normal-path queues: search-seed discovery, projection facet/layering build, and snapshot compaction now plan/drain through `workflow_commands`; the legacy adapter can still convert historical rows for those kinds, but normal execution does not write or require `job_materialization_items`.
  - Public materialization diagnostics and smoke service metrics now include command-owned queue evidence, so snapshot/search backlog and status are visible without reviving legacy queue reads as a normal source of truth.
  - Updated W6 targeted regressions to assert strict normal-write mode with `SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES=1`, no normal-path `legacy_materialization_item` payload refs, and command-owned projection-facet partial/completed progress.
  - Validation so far:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_durable_runtime.py tests/test_workflow_service_metrics.py tests/test_results_api.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_durable_runtime.py tests/test_workflow_service_metrics.py tests/test_results_api.py -k "workflow_command or legacy_materialization or snapshot_full_materialization or search_seed_discovery or projection_facet_layering or materialization_items_api" -q` -> `12 passed, 357 deselected`
  - Remaining: run strict W6 service/signoff gates and then the full containerized/scripted signoff before physical deletion or permanent hard-disable of migration-era bridge code.
- 2026-05-23 W6b durable causality / PG-only preflight closeout:
  - Upgraded W6b from a loose auditability goal to a stricter durable-execution gate. Normal durable runtime storage for `workflow_events`, `workflow_current_state`, `workflow_commands`, and `runtime_outbox` now fails closed outside PG-only mode; SQLite durable runtime access is limited to an explicit pytest-only escape hatch while legacy unit tests are migrated.
  - `workflow_commands` causality is now persisted as physical columns, not only `payload.causality`: stage, causal group, parent/source event, artifact refs, produced counts, no-op reason, readiness effect, downstream refs, and causality schema version. Metrics and preflight read those physical columns as source of truth.
  - The reducer now fills default `stage_id`, `readiness_effect`, and inferred produced counts for known command types so normal `CommandPlanRequested` output forms a typed causality graph instead of sparse payload metadata.
  - Post-profile SLO pairing now exposes typed causal-group pair counts versus legacy snapshot heuristic pair counts. W6/nightly cases require `require_no_post_profile_heuristic_slo_pairing=true`, so long signoff validates latency/stability after contract proof instead of discovering hidden timestamp/snapshot metric drift.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_durable_runtime.py -q` -> `35 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_workflow_service_metrics.py -q` -> `67 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_scripted_smoke_signoff.py -q` -> `42 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok`
    - `make ci-pg-contract` -> passed and verified physical workflow command causality columns in disposable Postgres
    - `make ci-workflow-fake-provider` -> passed
    - `make ci-containerized-pre-release` -> passed for Docker doctor, PG contract, fake provider, seeded projection browser, and workflow-driven projection browser gates
  - Remaining: fresh targeted OpenAI/Lovable/Google scripted rerun and full W6/nightly matrix + Pre-Manual Signoff with `workflow_causality_contract.violation_detected=false`, `heuristic_pairing_used=false`, `normal_path_write_count=0`, and `legacy_bridge_used=false`; then delete or permanently hard-disable migration-era bridge paths and remove the pytest SQLite durable-runtime escape hatch after PG-backed unit fixture migration.
- 2026-05-23 W5c.4 cold archive manifest foundation:
  - Added `asset_consolidation_cold_archive_manifest_v1` plus `scripts/build_asset_consolidation_cold_archive_manifest.py` to turn a W5c plan into a read-only cold-backup manifest.
  - The manifest records snapshot source dirs, backup keys, file counts/sizes, optional per-file sha256, and a stable manifest digest, but it explicitly sets `deletion_allowed=false` and performs no file or registry mutation.
  - Archive candidates fail closed unless the plan decision is `archive_ready_for_cold_backup_review`, overlap status is `subsumed_by_reference`, unique identity count is zero, deletion blockers are absent, the local snapshot directory exists, and the file listing is complete.
  - Blocked archive candidates now fail before any directory/file/hash scan. This keeps W5c.4 bounded by contract gates first and avoids turning a mostly-blocked manifest into large unnecessary I/O.
  - Real local evidence was generated at `runtime/audits/asset_consolidation/w5c4_cold_archive_manifest_20260523T000000Z/large_companies_cold_archive_manifest.json`: status `partial_ready_for_cold_backup_review`; `22` archive candidates; `1` manifest-ready OpenAI snapshot `20260501T222111` (`2280` files, `87342301` bytes); `21` blocked. This closes the previous gap between overlap evidence and a reviewed cold-backup plan without introducing an archive executor or silent normal-reuse exclusion.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/asset_consolidation_cold_archive_manifest.py scripts/build_asset_consolidation_cold_archive_manifest.py tests/test_asset_consolidation_cold_archive_manifest.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_asset_consolidation_cold_archive_manifest.py -q` -> `4 passed`
- 2026-05-24 W7/Public Web bridge hardening:
  - Retired the remaining normal-path CRM-to-target Public Web compatibility bridge. Legacy `/api/target-candidates/public-web...` aliases still require their migration/test override, but they now operate only on existing legacy target-candidate ids and no longer synthesize `target_candidates` rows for CRM records.
  - `sync_public_web_batch_to_crm_owner(...)` is now disabled by default and returns report-visible migration evidence instead of writing `target_candidate_public_web_v1` runs into `crm_public_web_*` owner tables. Historical target->CRM sync requires explicit `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC=1`.
  - Removed unused orchestrator sync helpers that copied target-candidate Public Web batches/runs/promotions into CRM owner tables with the retired execution backend.
  - Moved projection/CRM API contract tests onto the PG-only durable runtime fixture because CRM Public Web promotion can enqueue projection person-search index work through `workflow_commands`; SQLite is not a valid normal-path runtime for that contract.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/target_candidate_public_web.py tests/test_projection_crm_api_contracts.py tests/test_target_candidate_public_web.py tests/test_results_api.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/target_candidate_public_web.py tests/test_projection_crm_api_contracts.py tests/test_target_candidate_public_web.py tests/test_results_api.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_target_candidate_public_web.py -q` -> `14 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_projection_crm_api_contracts.py -k "crm_public_web_reads_fail_closed_without_owner_projection or public_web" -q` -> `7 passed, 4 deselected`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_results_api.py -k "legacy_target_public_web_endpoint_does_not_bridge_crm_record_ids or target_candidate_public_web_api_queues_idempotent_runs or target_candidate_public_web" -q` -> `8 passed, 288 deselected`
- 2026-05-24 W7/CRM Public Web typed command owner:
  - Added first-class `workflow_commands(command_type='crm.public_web.queue_batch', owner='crm_public_web_owner')` for CRM Public Web worker enqueue. `start_crm_record_public_web_search(...)` still creates CRM-owned batch/run rows and synchronously drains the one command for current UX, but worker creation is now a typed command-owner side effect rather than route-local execution.
  - Added command idempotency/causality coverage for `crm.public_web.queue_batch`; produced counts capture CRM Public Web runs and CRM records, readiness effect is `crm_public_web_workers_queued`, and disabling `CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_OWNER_ENABLED` leaves the command queued with report-visible skip instead of falling back to target-candidate bridges.
  - Updated `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `docs/CRM_STATE_CONTRACT.md`, `docs/TESTING_PLAYBOOK.md`, and `docs/NEXT_TODO.md` with the new owner boundary and follow-up signoff metric work.
- 2026-05-24 W7/CRM Public Web command-owner signoff extraction:
  - Extended `workflow_service_metrics.target_candidate_public_web` with `queue_batch_command_*` counts for `crm.public_web.queue_batch` status, owner, missing proof, and causality completeness. A CRM-owned batch without a succeeded `crm_public_web_owner` queue command now fails the fast contract layer before W6/nightly.
  - Updated hosted smoke action extraction so CRM Public Web's independent workflow command evidence is appended to the case-level `workflow_commands` diagnostics; the legacy job materialization endpoint is no longer the only diagnostic source for this post-workflow action.
  - Added `require_crm_public_web_queue_batch_command` and zero-tolerance pending/failed/invalid-owner/incomplete-causality expectations to the target Public Web service matrix and service-gate manifest.
  - Hard-disabled legacy target-candidate Public Web orchestrator start/cancel/retry/list methods unless `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS=1` is set for explicit migration/test coverage. This closes the internal bypass where API aliases returned `410` but direct orchestrator calls could still enqueue `target_candidate_public_web_v1` workers.
- 2026-05-24 W7/target-candidate Public Web direct-helper retirement:
  - Extended the retirement guard below API/orchestrator into the legacy helper layer. `start_target_candidate_public_web_batch(...)`, `cancel_target_candidate_public_web_run(...)`, `execute_target_candidate_public_web_run_once(...)`, and `sync_public_web_batch_summary(...)` now return a report-visible `legacy_target_public_web_execution_disabled` envelope unless `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS=1` is set.
  - Kept CRM Public Web normal path unchanged: CRM-owned batches/runs still use `crm.public_web.queue_batch`, `crm_public_web_v1`, and `crm_public_web_search` worker recovery.
  - Split worker recovery onto a CRM-named wrapper: `crm_public_web_search` now calls `execute_crm_public_web_run_to_local_idle(...)`, while legacy `target_candidate_public_web_search` remains migration-only. Deleted the generic recovery-kind resolver that defaulted to the target-candidate owner.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/target_candidate_public_web.py tests/test_target_candidate_public_web.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py -q` -> `15 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k "target_candidate_public_web_orchestrator_start_is_retired_without_migration_override or target_candidate_public_web_api_queues_idempotent_runs or legacy_target_public_web_endpoint_does_not_bridge_crm_record_ids or crm_public_web"` -> `3 passed, 294 deselected`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_worker_daemon.py tests/test_target_candidate_public_web.py -q` -> `22 passed`
    - Targeted CRM Public Web service smoke passed at `output/w7_target_public_web_direct_helper_retirement_20260524/`: case `target_public_web_service_slo_from_workflow_result`, `expectation_failures=[]`, Pre-Manual Signoff `passed`, blocking findings `0`, manual-review findings `0`, warnings `0`, all `7` provider invocations scripted, and `target_public_web_contract` passed.
