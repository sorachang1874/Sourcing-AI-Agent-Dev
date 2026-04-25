# Semantic Refactor Progress

> Status: Living tracker. Use the latest entries as the source of truth, and assume older bullets may describe superseded intermediate states.


Updated: 2026-04-23

## Goal

Replace the old `intent_view -> _infer_*` heuristic chain with a cleaner pipeline:

1. `intent_view`
2. `semantic_brief`
3. `execution semantics / acquisition strategy`
4. `task projection`

Secondary goals:

- reduce patch-style branches across planning / acquisition-related modules
- keep one clear contract instead of parallel legacy semantics
- standardize local execution on `.venv-tests` / venv-style entrypoints so path and shell differences do not keep breaking across Mac / ECS / previous WSL flows
- extend tests so manual-review-visible failures become deterministic automated checks

## Current Plan

- [x] Read `request_normalization.py`, `acquisition_strategy.py`, `organization_execution_profile.py`, `query_signal_knowledge.py`
- [x] Finish `semantic_brief` contract and fully wire it into `request_normalization` + `acquisition_strategy`
- [x] Remove duplicated semantic inference from acquisition strategy where `semantic_brief` already provides the signal for role targeting / directional intent / explicit full-roster intent
- [x] Record environment/path contract and make venv-first execution explicit in scripts/docs
- [ ] Audit obvious patch branches in adjacent modules and extract reusable helpers/contracts
- [x] Add tests that assert:
  - semantic brief role/function targeting
  - acquisition strategy / generated filter hints
  - generated search queries / shard hints
  - plan payload carries semantic brief into visible strategy explanation
- [x] Unify search-seed lane persistence / legacy backfill into a shared helper instead of keeping separate compatibility logic in acquisition + asset reuse + runtime rebuild
- [x] Make runtime rebuild backfill legacy aggregate search-seed assets into materialized `current/` and `former/` lane payloads and resync acquisition shard registry
- [ ] Add explicit automated coverage for execution timeline / stage-visible frontend-facing invariants
- [ ] Run broader simulate/e2e coverage after semantic/planning refactor settles

## Notes

- The current repo worktree is intentionally treated as the source of truth; avoid reverting unrelated local changes.
- Former/current lane storage semantics were previously refactored at the search-seed registry layer, but this file tracks the newer semantic-planning refactor only.
- Current environment contract:
  - repo runtime python: prefer `./.venv/bin/python`, fallback `./.venv-tests/bin/python`
  - test/runtime validation: `make bootstrap-test-env && make dev-doctor`
  - do not assume shell-specific PATH / login profile state
- Historical data note:
  - the earlier former/current lane split at the search-seed registry layer is still backward compatible on read
  - historical aggregate search-seed summaries now have a rebuild/backfill path that materializes per-lane payload files and shard-registry rows
  - the remaining gap is not asset backfill anymore, but live backend smoke coverage and the still-unsplit single-table lane storage in Postgres

## Latest Update

- Added `src/sourcing_agent/search_seed_registry.py` as the single contract for:
  - dedupe / persist / merge of search-seed lane payloads
  - legacy aggregate-summary backfill into per-lane materialized assets
  - lane-summary loading used by asset reuse planning
- `acquisition.py` now persists `SearchSeedSnapshot` immediately for current-only runs, so lane payloads are no longer only materialized after a former/current merge path.
- `runtime_rebuild.py` now:
  - backfills legacy aggregate search-seed snapshots into `search_seed_discovery/current|former`
  - re-syncs `acquisition_shard_registry` from rebuilt lane payloads
- Added regression coverage for:
  - current-only snapshot lane materialization
  - runtime rebuild of legacy aggregate search-seed assets into lane files + shard registry rows
- Validation completed:
  - targeted `py_compile`
  - targeted `ruff`
  - targeted `mypy` on touched backend modules
  - contract / planning / explain / execution tests
- Added `compile_asset_reuse_lane_semantics(...)` in `src/sourcing_agent/execution_semantics.py` so these paths now read the same lane-level reuse/delta state instead of each recomputing it differently:
  - dispatch delta gating
  - execution semantics
  - workflow lane preview
  - registry-backed snapshot reuse explanation
- `src/sourcing_agent/orchestrator.py` now uses registry fallback to hydrate missing baseline lane fields before deciding `reuse_snapshot` vs `delta_from_snapshot`, reducing drift between plan-time and dispatch-time semantics.
- `/api/jobs/{job_id}/results` is now summary-first by default:
  - API callers get asset-population counts/metadata without full candidate payloads
  - full candidates are only returned with `?include_candidates=1`
  - company-scale candidate paging stays on `/api/jobs/{job_id}/candidates`
- API route job-id matching was normalized to accept `[A-Za-z0-9_-]+` consistently across progress/dashboard/results/detail-like endpoints.
- Explain/smoke expectations were updated to the canonical large-org shard strategy id `adaptive_us_technical_partition`; older `adaptive_us_eng_research_partition` references were stale test/matrix leftovers.
- Interface benchmark completed on a synthetic 1200-candidate snapshot job:
  - summary-only `/results`: `14.5 KB`, `~115 ms` average
  - full `/results?include_candidates=1`: `~800 KB`, `~3.0 s` average
  - this confirms the main payload bottleneck moved off the default results endpoint
- Remaining gaps after this round:
  - browser E2E is still pending
  - adjacent-module patch-branch cleanup is still not fully finished; this round only removed the highest-value storage/API drift points
  - execution timeline / stage-visible frontend invariants still need one more explicit browser-facing assertion layer

## Latest Update 2

- PG `acquisition_shard_registry` has now completed the physical former/current cutover:
  - live Postgres no longer treats shard registry as a single physical table
  - storage/runtime still read the logical `acquisition_shard_registry` contract
  - PG writer/import/migration paths now route rows into:
    - `acquisition_shard_registry_current`
    - `acquisition_shard_registry_former`
  - the logical `acquisition_shard_registry` name is now a compatibility view over the split physical tables
- The cutover is now enforced in all relevant PG entry points instead of keeping long-lived dual-write branches:
  - live control-plane writer bootstrap
  - SQLite snapshot -> PG sync
  - direct-stream SQLite -> PG sync
  - Postgres -> Postgres migration script
  - PG snapshot export
- `/api/jobs/{job_id}` is now summary-first by default:
  - default payload is reduced to job summary / status / lightweight request preview
  - full `events` + `intent_rewrite` are opt-in via `?include_details=1`
  - this matches the earlier `/api/jobs/{job_id}/results` summary-first change and removes another heavy default payload from frontend polling fallback
- Regression coverage added/updated:
  - `tests/test_control_plane_postgres.py`
    - split current/former routing for acquisition shard registry upserts
  - `tests/test_results_api.py`
    - `/api/jobs/{job_id}` summary-only default + opt-in detail contract
  - `tests/test_hosted_workflow_smoke.py`
    - hosted smoke callers that genuinely need full asset-population candidates now explicitly request `?include_candidates=1` or `?include_details=1`
- Cross-environment path drift cleanup:
  - `harvest_connectors.py` no longer eagerly resolves runtime ancestors before scanning for `runtime/`, so macOS `/var` vs `/private/var` does not leak into cache-bridge assertions
  - `service_daemon.py` no longer forces `.resolve()` into generated systemd working directories, keeping operator-facing config stable across dev hosts
- Full validation status after this round:
  - full repo `pytest`: passed
  - full repo `mypy`: passed
  - targeted hosted smoke / results API / control-plane suites: passed
