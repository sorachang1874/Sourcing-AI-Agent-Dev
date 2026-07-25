# Authoritative Asset Coverage Contract

> Status: Current contract. Read with `INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md`, `DATA_ASSET_GOVERNANCE.md`, `archive/STREAMING_WORKFLOW_REBUILD_PLAN.md`, and `PROGRESS.md` before changing asset reuse planning, organization execution profiles, registry promotion, ECS asset migration, or scoped-shard reuse.

## Problem

`organization_asset_registry.authoritative=true` is a serving pointer. It says which company asset should be preferred for serving and planning. It does not prove that the pointed snapshot is a complete full-company roster.

Historical failures came from treating these as the same thing:

- A newer OpenAI Health/Whisper serving snapshot became authoritative and narrowed selected source snapshots, hiding Agent/ChatGPT shard coverage from the planner.
- ECS migration missed `acquisition_shard_registry` rows, so local/ECS plans diverged even with the same snapshot files.
- A small or medium company can have an authoritative snapshot that was produced by a scoped search only. Reusing that snapshot for "all members" is wrong; reusing it for the exact same scoped query can be correct.
- A high-volume full-company baseline can serve "all members", but it should not automatically satisfy a new directional query such as Gemini/Agent/Infra unless exact scoped shard coverage or an explicit directional reuse contract exists.

The service-grade rule is: local reuse must be proven at the population boundary requested by the user. The requested boundary itself is owned by `requested_population_boundary`; coverage proof can satisfy that boundary, but cannot rewrite it.

## Contract Objects

### Authoritative Serving Pointer

Stored in `organization_asset_registry.authoritative`.

Meaning:

- preferred company-level serving snapshot/generation
- default baseline candidate for planner comparison
- provenance source for selected snapshot ids

Non-meaning:

- not proof of complete full-company population
- not proof of every future scoped keyword/team query
- not a license to overwrite scoped shard coverage

### Full-Company Coverage Proof

Normalized by `build_population_coverage_contract(...)` in `asset_coverage_contracts.py`.

Acceptable proof sources:

- Explicit `population_coverage` / `full_company_coverage` payload with `coverage_kind=full_company_roster` and complete/verified status.
- Promoted aggregate coverage proof via `source_snapshot_selection.coverage_proof`.
- Migration/write-time compatibility only: standard bundle proof, company-employee shard proof, or high-volume lane coverage that covers almost the full candidate population.

Normal planner, organization profile, execution semantics, and public explain paths run in strict mode. They do not treat standard bundles, high-volume lane counts, or company-employee shards as hidden full-company proof unless that proof has first been persisted as explicit `population_coverage`. `allow_legacy_inference=True` is reserved for `backfill-authoritative-population-coverage` and guarded authoritative write-time publication, where the output is an auditable explicit contract.

This proof unlocks full-company roster reuse, but not arbitrary directional scoped reuse.

### Exact Scoped Shard Coverage

Stored in `acquisition_shard_registry`.

Meaning:

- exact current/former profile-search or company-employee shard reuse can satisfy the same compatible scoped request
- selected source snapshots remain reusable even when the authoritative serving snapshot changes

Non-meaning:

- scoped coverage does not prove full-company roster completeness
- unrelated scoped shards cannot satisfy a new query family

## Planning Rules

- Full-company/all-members query requires full-company coverage proof. If only scoped coverage exists, plan `delta_from_snapshot` or live acquisition.
- Scoped directional query first looks for exact shard coverage in selected snapshots. If current/former requested shards are covered, it may plan `reuse_snapshot_only`.
- Scoped directional query without exact shard coverage must plan delta acquisition, even when the baseline is large and authoritative, unless `directional_scope_reuse_allowed` is explicitly present in the coverage contract.
- Directional queries with all-members wording may use full-company assets first only when `requested_population_boundary.full_company_filter_allowed=true` and full-company coverage is proven; otherwise they remain scoped directional.
- `selected_snapshot_ids` are provenance inputs. They are not proof by themselves.
- `latest_snapshot.json` is not a planning proof. It is a local file pointer and should be regenerated or ignored in favor of registry/generation facts.

## Implementation Status

Landed 2026-05-02:

- Added `asset_coverage_contracts.py` with `build_population_coverage_contract`.
- `asset_reuse_planning` now separates `baseline_full_company_coverage_proven`, `baseline_population_coverage_contract`, exact scoped shard coverage, and directional reuse eligibility.
- `organization_execution_profile` reads the same coverage contract, so profile defaults and planner behavior do not diverge.
- `audit-authoritative-reuse-planning` provides a read-only report over request boundary, authoritative pointer, selected planning row, full-company proof, scoped shard rows, planner outcome, execution semantics, and warnings.
- `audit-authoritative-reuse-planning-matrix` and `compare-authoritative-reuse-planning-matrix` provide a default read-only ECS/local parity report from `configs/planner_parity/authoritative_reuse_planning_matrix.json`.
- Matrix summaries include profile-query missing counts, exact-overlap gap counts, missing selected-shard registry rows, and `baseline_generation_lags_same_snapshot_shard_materialization` so same-snapshot serving-generation drift is visible without inspecting full audit payloads.
- `backfill-authoritative-population-coverage` converts already-proven full/scoped coverage into explicit registry metadata. It defaults to dry-run and only writes `population_coverage` with `--apply`; it does not call providers, rebuild artifacts, or repair materialization generations.
- `repair-authoritative-serving-generation` repairs same-snapshot serving-generation lag as an explicit offline operator flow. It defaults to dry-run, standardizes already-persisted shard bundles, creates a new repair snapshot on `--apply`, rebuilds normalized artifacts, publishes the new authoritative registry row, and preserves selected source snapshot provenance. It does not call providers and does not mutate historical snapshots in place.
- `normalize-authoritative-source-provenance` normalizes an authoritative row after migration or repair so `selected_snapshot_ids` contains the serving snapshot plus only source snapshots with reusable shard registry proof. Historical selected ids without shard rows are archived into metadata instead of remaining planner inputs.
- New authoritative writes enforce the same provenance rule before publication: a selected snapshot id must either be the serving snapshot or have reusable `acquisition_shard_registry` proof. No-shard ids are archived into `archived_source_snapshot_ids_without_shard_registry_rows`, not left as planner inputs.
- New authoritative writes also persist explicit `population_coverage` before publication when the coverage contract can be proven from explicit metadata, selected shard rows, standard bundles, or high-volume lane coverage. This makes future coverage decisions auditable even while historical rows still require the backfill command.
- Normal planning/profile reads now suppress legacy population proof inference. `build_population_coverage_contract(...)` defaults to strict mode and emits `legacy_inference_suppressed=true` when a row has only legacy standard-bundle/high-volume/company-employee signals. Backfill/write-time publication must opt into `allow_legacy_inference=True` and persist the resulting explicit `population_coverage` before the row can unlock full-company reuse.
- `sync_company_asset_registration` now enforces the same-snapshot serving-generation invariant before authoritative pointer promotion. It refreshes shard registry/bundles first, checks that selected same-snapshot reusable shard materializations are subsumed by the candidate serving generation, and automatically republishes a no-provider repair snapshot before publishing when the generation lags. Offline repair remains for historical/ECS rows created before this invariant existed.
- Legacy high-volume/full-bundle baselines still work for full-company population reuse.
- Large-org directional queries without exact shard coverage remain `delta_from_snapshot`.
- Small-company scoped-only authoritative snapshots no longer unlock full-company reuse, while exact current scoped shard reuse still works.
- `execution_semantics` also reads full-company coverage proof before labeling a snapshot-backed run as "full local asset reuse", so display state cannot reintroduce scoped-only full reuse.

## Regression Coverage

Tests that guard this contract:

- `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_small_company_scoped_only_authoritative_asset_does_not_unlock_full_population_reuse`
- `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_small_company_exact_scoped_shard_still_reuses_without_delta`
- `tests/test_pipeline.py::PipelineTest::test_large_org_scoped_query_without_matching_shard_requires_current_and_former_delta`
- `tests/test_pipeline.py::PipelineTest::test_asset_reuse_plan_reuses_selected_snapshot_directional_shards_without_delta`
- `tests/test_workflow_explain.py::WorkflowExplainTest::test_explain_workflow_large_directional_baseline_without_shard_requires_delta`
- `tests/test_execution_semantics.py::ExecutionSemanticsTest::test_compile_execution_semantics_does_not_label_scoped_only_snapshot_as_full_reuse`
- `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_directional_all_members_query_uses_full_company_filter_only_with_full_coverage`
- `tests/test_asset_reuse_audit.py::AssetReuseAuditTest::test_audit_warns_full_company_request_when_only_scoped_authoritative_asset_exists`
- `tests/test_asset_reuse_audit.py::AssetReuseAuditTest::test_audit_reports_full_company_filter_from_baseline_when_coverage_is_proven`
- `tests/test_authoritative_serving_repair.py::AuthoritativeServingRepairTest::test_authoritative_publication_repairs_same_snapshot_shard_gap_before_promotion`
- `tests/test_authoritative_source_provenance.py::AuthoritativeSourceProvenanceTest::test_authoritative_guard_drops_new_selected_source_ids_without_shard_proof`
- `tests/test_asset_coverage_backfill.py::AssetCoverageBackfillTest::test_population_coverage_contract_suppresses_legacy_inference_by_default`
- `tests/test_workflow_explain.py::WorkflowExplainTest::test_explain_workflow_does_not_use_legacy_standard_bundle_as_hidden_full_coverage_proof`

## Audit CLI

Use this command to generate an ECS/local comparable report before migration or parity testing:

```bash
PYTHONPATH=src python -m sourcing_agent.cli audit-authoritative-reuse-planning \
  --company Google \
  --query "帮我找Google做Gemini方向的人" \
  --query "帮我找Google做Veo方向的人" \
  --output runtime/audits/google-authoritative-reuse-planning.json
```

The report is intentionally offline and read-only. If it reports `cached_ledger_missing_and_rebuild_suppressed_for_read_only_audit`, run the explicit asset coverage backfill or rebuild flow separately; do not let public planning reads repair state implicitly.

Use the matrix before ECS migration or after ECS asset import:

```bash
PYTHONPATH=src python -m sourcing_agent.cli audit-authoritative-reuse-planning-matrix \
  --matrix configs/planner_parity/authoritative_reuse_planning_matrix.json \
  --summary-only \
  --output runtime/audits/local-authoritative-reuse-planning.json

PYTHONPATH=src python -m sourcing_agent.cli compare-authoritative-reuse-planning-matrix \
  --left runtime/audits/local-authoritative-reuse-planning.json \
  --right runtime/audits/ecs-authoritative-reuse-planning.json \
  --strict
```

Important warning meanings:

- `selected_snapshot_ids_missing_shard_registry_rows`: non-serving selected source snapshots exist but the environment cannot prove reusable scoped shards for all selected source ids. Treat this as a migration/backfill or provenance-normalization signal. The serving snapshot itself is not required to have shard rows.
- `baseline_generation_lags_same_snapshot_shard_materialization`: a shard row exists for the authoritative snapshot, but the serving generation does not subsume that shard materialization. Treat this as a serving-generation repair/republication issue before expecting `reuse_snapshot_only`.

Use the coverage backfill after importing registry/shard rows and before relying on long-term planner behavior:

```bash
PYTHONPATH=src python -m sourcing_agent.cli backfill-authoritative-population-coverage \
  --company OpenAI \
  --company Meta

PYTHONPATH=src python -m sourcing_agent.cli backfill-authoritative-population-coverage \
  --company OpenAI \
  --company Meta \
  --apply
```

Do not use this command to mask `baseline_generation_lags_same_snapshot_shard_materialization`. If the matrix reports that warning, repair or republish the serving generation separately so the authoritative baseline actually contains the selected shard members.

Use the serving-generation repair command only after the audit proves that selected shard rows exist but the authoritative serving generation does not subsume those same-snapshot shard materializations:

```bash
PYTHONPATH=src python -m sourcing_agent.cli repair-authoritative-serving-generation \
  --company OpenAI \
  --query "我想要OpenAI在health组的人" \
  --output runtime/audits/openai-health-serving-repair-dry-run.json

PYTHONPATH=src python -m sourcing_agent.cli repair-authoritative-serving-generation \
  --company OpenAI \
  --query "我想要OpenAI在health组的人" \
  --output runtime/audits/openai-health-serving-repair-apply.json \
  --apply
```

Repair status meanings:

- `dry_run`: gaps were found and the command reports which existing shard bundles would be standardized and merged.
- `no_repair_needed`: the current authoritative serving generation already satisfies the audited query.
- `repaired`: `--apply` created and published a repair snapshot, the planner lag warning cleared, and the repair generation subsumes selected shard rows with matching employment scope.
- `repaired_with_scope_mismatch`: the repair generation contains the selected shard members and planner reuse is fixed, but some members are present under a different current/former scope. Treat this as a data-quality cleanup signal, not as a failed serving-generation repair.
- `repair_incomplete`: the new generation still does not subsume selected shard rows or the planner still reports same-snapshot generation lag.
- `blocked`: required baseline directories, shard rows, or standardized bundle payloads are missing.

Normalize selected source provenance after a repair or asset migration if the audit reports selected source ids without shard proof:

```bash
PYTHONPATH=src python -m sourcing_agent.cli normalize-authoritative-source-provenance \
  --company OpenAI \
  --output runtime/audits/openai-source-provenance-normalize-dry-run.json

PYTHONPATH=src python -m sourcing_agent.cli normalize-authoritative-source-provenance \
  --company OpenAI \
  --output runtime/audits/openai-source-provenance-normalize-apply.json \
  --apply
```

Operational rule: run registry/backfill/repair maintenance commands serially against the same PG control plane. Even read-oriented dry-runs can trigger schema preflight and should not be parallelized with another control-plane maintenance command.

## Next Work

- Run the authoritative reuse planning matrix on ECS and compare it with the local report.
- Run `backfill-authoritative-population-coverage` on ECS in dry-run, then apply after the matrix comparison is understood.
- Local OpenAI Health serving-generation repair is complete: the current local dry-run reports `no_repair_needed` against repair snapshot `20260501T222111`; ECS should wait until the broader local work is complete, then run the same audit/repair flow as part of final sync.
- Local OpenAI/Meta selected source provenance has been normalized; the local planner parity matrix now reports `selected_snapshot_missing_shard_registry_row_count=0` and no warnings for all default cases.
- After this contract is stable, continue queue-first scheduler and partial delta board streaming. Those workstreams depend on planner correctness; otherwise downstream queues may never be created.
