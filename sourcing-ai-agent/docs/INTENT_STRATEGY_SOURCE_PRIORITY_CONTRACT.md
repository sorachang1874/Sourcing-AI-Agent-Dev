# Intent Strategy Source Priority Contract

> Status: Current contract. Read before changing request normalization, semantic intent, acquisition strategy selection, asset reuse planning, execution semantics, plan review rewrites, or planner parity tests.

## Problem

Planner state used to mix three different meanings:

- user-requested population boundary
- organization profile defaults for efficient acquisition
- local asset coverage proofs

That made source priority easy to invert. A scoped-only authoritative asset could make an "all members" query look reusable, while a directional query with "all members" wording had no explicit rule for whether to use full-company local assets first or run scoped search.

The service-grade rule is: user intent defines the requested population boundary; asset coverage can satisfy that boundary, but cannot rewrite it.

## Source Priority

The canonical order is:

1. Explicit operator override in `execution_preferences`.
2. User population boundary from `requested_population_boundary`.
3. Population status constraints such as former-only or investor-only.
4. Organization execution profile defaults such as large-org scoped search.
5. Asset reuse coverage proofs from `baseline_population_coverage_contract`.
6. Authoritative serving pointer as a candidate baseline only.

`organization_asset_registry.authoritative=true` is never allowed to override the requested population boundary.

## Requested Population Boundary

`semantic_intent.compile_semantic_brief(...)` emits `requested_population_boundary`, and `resolve_request_intent_view(...)` exposes the same payload in the request intent view.

Current boundary types:

- `full_company_roster`: the user asks for the whole company population.
- `scoped_directional`: the user asks for a team/topic/product/role/direction subset.
- `former_only`: the user asks for former employees only.
- `scoped_search`: a non-company-wide scoped request.

Important distinction:

- `target_scope=full_company_asset` is the serving domain.
- `requested_population_boundary.boundary_type` is the user-intent population boundary.

Do not treat `target_scope` alone as proof of a full-company request.

## Directional + All-Members Rule

Queries such as "xAI Coding direction all members" are `scoped_directional` with `full_company_filter_allowed=true`.

Execution rule:

- If a full-company coverage proof exists, reuse the full-company asset and filter locally.
- If only scoped shards exist, run scoped directional acquisition or exact scoped shard reuse.

This is why xAI with a complete local baseline can use full-company filtering, while OpenAI with selected shards for Agent/Health/ChatGPT must remain scoped directional unless exact shard coverage exists for the query.

## Consumer Requirements

- Acquisition strategy must honor `scoped_directional` before scoped-only coverage can be widened by organization profile defaults. Directional queries against scoped-only authoritative assets stay `scoped_search_roster`; `scoped_directional` + full-roster-language may promote to `full_company_roster` only when organization profile/coverage already proves full-company coverage. When no cached coverage exists, live acquisition may still use the existing profile/default strategy rules.
- Asset reuse planning may clear missing profile queries for that shape only when `baseline_population_default_reuse_sufficient=true` and `full_company_filter_from_baseline=true`.
- Execution semantics may label "full local asset reuse" only when `baseline_full_company_coverage_proven=true`; a scoped-only snapshot candidate source is not enough.
- Planner/explain payloads must carry `requested_population_boundary`, `baseline_population_coverage_contract`, and final planner mode so ECS/local drift can be audited.

## Read-Only Audit

Use this before ECS/local parity work or asset migration decisions:

```bash
PYTHONPATH=src python -m sourcing_agent.cli audit-authoritative-reuse-planning \
  --company OpenAI \
  --query "帮我找OpenAI做Agent方向的人" \
  --query "我想要OpenAI在health组的人" \
  --output runtime/audits/openai-authoritative-reuse-planning.json
```

The audit must remain read-only: it may read registry, cached completeness ledgers, and shard rows, but it must not warm organization profiles, rebuild missing ledgers, call providers, or persist plan history.

For ECS/local parity, use the default matrix and compare reports:

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

The matrix summary intentionally includes service-level signals such as missing profile-query counts, exact-overlap gap counts, missing selected-shard registry rows, and `baseline_generation_lags_same_snapshot_shard_materialization`. These fields catch cases where intent parsing is correct but the serving generation or migrated registry still prevents full reuse.

## Regression Coverage

- `tests/test_semantic_intent.py::SemanticIntentTest::test_requested_population_boundary_distinguishes_full_roster_from_directional_filter`
- `tests/test_planning_modules.py::PlanningModulesTest::test_directional_all_members_language_without_full_coverage_stays_scoped_search`
- `tests/test_planning_modules.py::PlanningModulesTest::test_directional_all_members_language_with_full_coverage_uses_full_company_filter`
- `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_directional_all_members_query_uses_full_company_filter_only_with_full_coverage`
- `tests/test_execution_semantics.py::ExecutionSemanticsTest::test_compile_execution_semantics_does_not_label_scoped_only_snapshot_as_full_reuse`
- `tests/test_asset_reuse_audit.py::AssetReuseAuditTest::test_audit_keeps_ordinary_directional_query_scoped_before_profile_default`

## Next Work

- Run the matrix on ECS and compare it with the local report before judging hosted planner drift.
- Backfill coverage and serving-generation gaps exposed by the matrix before queue-first scheduler work.
- Keep plan-review rewrites from bypassing this contract: any rewrite that changes population boundary must update `requested_population_boundary` and record the source.
