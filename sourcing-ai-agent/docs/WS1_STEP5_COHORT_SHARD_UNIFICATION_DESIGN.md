# WS1 Step 5 — Cohort→Shard-Planner Unification Design (full merge)

> Status: DESIGN DRAFT 2026-07-23, awaiting independent review (contract-heavy → review gate required before any implementation slice). Dual operator rulings RATIFIED 2026-07-22: ① full merge — cohort becomes an input mode of `company_shard_planning`, `CohortProviderCompiler` retires; ② `investor_firm_roster` stays outside the abstraction as an explicitly contracted standalone strategy (`source_kind=funding_graph` exception). Factual base: the Step 5 call-surface/contract inventory (2026-07-23, session artifact). No code changes land from this document until the review verdict.

## 1. Goal and ratified rulings

Two plan-artifact mints exist today for the same product question ("which provider queries acquire this population"): the shard planner (`company_shard_planning.py`, 1,414 lines — roster/keyword/former lanes, probe-driven, honesty contracts) and `CohortProviderCompiler` (`cohort_provider_compiler.py`, 1,572 lines — per-(status, role) lane manifests, capability-gated). The operator ruled (2026-07-22):

1. **Full merge**: cohort becomes an input mode of `company_shard_planning`; `CohortProviderCompiler` retires; the reviewed cohort identity/authorization/selection contracts survive intact and are re-pinned item-by-item by the merged suites.
2. **investor_firm_roster stays outside**: an independent strategy with its own explicit contract, marked `source_kind=funding_graph` in the unified artifact schema as the declared out-of-abstraction exception — no implicit special case.

Interpretation boundary (design decision D0): the ruling unifies the **planning abstraction** (one plan-artifact mint). The compiler also carries **execution-boundary safety** (capability gate, role-proof verifier, canonical-URL ownership) that was never a planning concern; those contracts relocate to execution-boundary owners rather than dissolving or moving into a planning module. "Compiler retires" = the module reaches zero references and is deleted; every contract it carried has a named new owner.

## 2. Target architecture

### 2.1 Cohort input mode in the shard planner

- New policy builder `build_cohort_lane_shard_policy(...)` in `company_shard_planning.py`, following the Step 4a template (`build_request_scoped_keyword_union_shard_policy`, company_shard_planning.py:173): explicit policy dict, `strategy_id="request_scoped_cohort_lanes"`, new mode value **`cohort_lanes`** beside `partition_mece`/`keyword_union` (normalize at company_shard_planning.py:474–476 extends; unknown still → `partition_mece`).
- Shard axes: one shard per (employment_status, role_bucket) lane, carrying `employment_status`, `role_bucket`, resolved `function_ids`/keyword patches, and server-derived `cohort_lane_membership` provenance. Lane count bounded by `MAX_COHORT_PROVIDER_LANES` (constant moves with the mode).
- **No probe calls in cohort mode** (D1): the compiler's `auto_probe=false` + single-budget allocation semantics are preserved as an explicit policy field (`"probe": "disabled"`); `plan_company_employee_shards_from_policy` must not invoke `probe_fn` for `cohort_lanes` policies. This keeps the paid-call surface identical (inventory risk R4).
- Authority: the mode is minted **only** from `source=user_explicit` cohort objects via `explicit_cohort_selection` (already imported at company_shard_planning.py:5; authority ladder at `resolve_roster_lane_function_ids`:367 unchanged). Non-explicit sources yield no cohort-mode policy (inventory risk R1).
- Completion honesty (D2): cohort lanes keep **all-or-nothing publication** (`cohort_execution_result.v2` requires complete lane coverage) — expressed as a policy field (`"publication": "all_lanes_complete"`) checked at the execution boundary; roster/keyword modes keep partial-with-honesty via `resolve_segmented_roster_completion`:325. The two semantics coexist as explicit policy values, never inferred from mode name (inventory risk R5).

### 2.2 Plan artifact

- The merged planner mints a **new exact schema version** `cohort_provider_manifest.v3`, registered in `cohort_execution_contract.py` beside v1/v2 (exact-version lookup, auto-upgrade forbidden — that module's standing rule). v1/v2 remain readable history; no in-place mutation of either (inventory risk R3).
- The dispatch-time exact-recompile equality guard (acquisition.py:3451; plan_review.py:562 for v1) is preserved shape-for-shape: execution recompiles v3 via the shard planner and requires byte-equality before any provider call (contract C12).
- During coexistence (slice ladder §4), a **corpus parity gate** runs both mints over a pinned request corpus and asserts byte-agreement on the semantic core (lanes, filters, budgets, membership) so the flip is evidence-backed, not asserted.

### 2.3 Execution-boundary safety re-homing

New bounded module `cohort_execution_safety.py` (extracted verbatim-first from the compiler, ~400 lines), owner of:

- `CohortExecutionCapability` + `cohort_execution_capability_for_runtime()` (fail-closed issuance for validated isolated non-live runtime; compiler:360,1101) — consumed by acquisition.py:2757 and harvest_connectors.py:1293–1416 exactly as today. Live triple-gate semantics untouched (inventory risk R2).
- `CohortRoleProofVerifier` protocol + `CohortHeadlineRoleProofVerifier` (role_match=all proof; provider-authored role fields ignored; compiler:480–525) (risk R7).
- `canonical_cohort_profile_url` + forged-field overwrite in lane-result validation (compiler:1439) (risk R8).
- `cohort_execution_not_ready_result` / `cohort_execution_unavailable_result` (orchestrator fail-closed gates at orchestrator.py:2991–4394,60515).

### 2.4 Role-targeting resolution

`resolve_effective_role_targeting` (compiler:526, consumed by acquisition_strategy.py:221) moves to its contract family owner **`cohort_selection.py`** (C9: user-explicit role list is complete role authority; empty list = all roles + suppresses inference).

## 3. Contract survival matrix

C1–C16 numbering from the Step 5 inventory. Contracts already owned by `cohort_selection.py` / `cohort_execution_contract.py` (C1–C8, C15, C16) are untouched — their pins must merely stay green through every slice.

| Contract | Today | New owner | Pin relocation |
|---|---|---|---|
| C9 role-targeting authority | compiler:526 | cohort_selection.py | pins in test_cohort_selection.py stay; compiler-suite pins move to test_cohort_selection.py |
| C10 lane compilation (one lane per (status, role); any=union+dedupe, all=intersection; server-derived membership; stable lane ids) | compiler:561 | company_shard_planning.py cohort mode | test_cohort_provider_compiler.py dissolves → new `CohortLaneShardPolicyTest` in test_company_shard_planning.py + parity pins |
| C11 execution capability fail-closed | compiler:360,1101 | cohort_execution_safety.py | test_cohort_provider_runtime_safety.py re-targets imports (module move, assertions unchanged) |
| C12 provider-plan authority (capability-free mint + exact recompile before dispatch) | planning.py:114 / plan_review.py:562 / acquisition.py:3451 | same call sites, recompile via shard planner (v3) | test_plan_review_location_apply.py + test_d1n_acquisition_plan_preview.py updated to v3 in the flip slice, plus corpus parity gate |
| C13 role-proof verifier | compiler:480–525 | cohort_execution_safety.py | runtime-safety pins move with module |
| C14 canonical URL ownership | compiler:1439 | cohort_execution_safety.py | forged-field pin moves with module |

Monkeypatch/module-path couplings (test_cohort_selection.py:733,756; test_tml_flexible_targeting_fixtures.py:128; agent_runtime_namespace_ref.py:54 comment) are updated at each move — **no permanent re-export shim**; if a bridge is unavoidable within a slice it is report-visible with a recorded removal condition (risk R10).

## 4. Migration ladder (four slices, each independently green)

1. **S5-1 relocate**: extract `cohort_execution_safety.py` + move `resolve_effective_role_targeting` to cohort_selection.py (verbatim-first, mixin-style zero-semantic move); re-target imports/patch paths; relocate investor pins and T-005 evidence out of test_cohort_provider_compiler.py (§5); provenance_baseline row moves. Compiler shrinks to planning-only.
2. **S5-2 coexist**: land `build_cohort_lane_shard_policy` + `cohort_lanes` mode + `cohort_provider_manifest.v3` registration + corpus parity gate (both mints, pinned corpus, byte-agreement). Nothing dispatches v3 yet.
3. **S5-3 flip**: planning.py:114, plan_review.py:541–562, acquisition_plan_preview.py mint via the shard planner (v3); acquisition.py:3451 equality guard recompiles v3; d1n preview/v2 pins updated; T-002 acceptance tests land (§6). Execution-boundary gates unchanged.
4. **S5-4 retire**: delete cohort_provider_compiler.py + test_cohort_provider_compiler.py; zero-reference sweep for `cohort_provider_compiler` (including comments); tombstone/doc closure (§6); monolith-budget and provenance registries updated.

Each slice: targeted tests + contract preflight + pin commit + review-request queued per master-plan §7 protocol. S5-3 (public-semantics flip) and S5-4 (retirement) each require their own review verdict before landing the *next* slice's paid-path validation.

## 5. investor_firm_roster standalone contract (ruling ②)

- New contract section (rides this doc until the implementation slice creates `docs/INVESTOR_FIRM_ROSTER_CONTRACT.md`): strategy membership in the closed 4-strategy set; resolution triggers (investor category / explicit override / "investor firm" alias); `roster_sources=[funding_graph, investor_firm_roster, linkedin_people_search]`.
- **Single-sourcing**: the strategy whitelist is duplicated across 6 modules (execution_semantics.py:12, execution_preferences.py:141,549, review_plan_instructions.py:70,271,534, model_provider.py:356, acquisition_strategy.py:569,715) and roster_sources twice (acquisition_strategy.py:1090, plan_review.py:865) — S5-1 introduces one owner constant module and rewires the copies.
- **source_kind migration**: code emits `source_kind="investor_firm_roster"` today (acquisition.py:3999,5538,5553); the ratified marker is `funding_graph`. This is a contract-field change: owner matrix row + fast contract preflight + explicit migration/aliasing in the unified artifact schema (Contract Field Ownership rules apply). It lands as its own reviewed change, not silently inside a merge slice (risk R11).
- **Non-participation pin**: a test outside the dissolving compiler suite asserts investor strategy never routes through the cohort input mode / shard-policy machinery.

## 6. Tombstone and evidence reconciliation

- **T-002** (REGRESSION_INDEX.md:62): the two recovered test intents (cohort roles → per-function roster shards; plan-metadata ↔ builder parity, from `2198dd3^` lines 300/759) become the S5-3 acceptance tests; `superseded_by` flips to the concrete new test ids in the same slice.
- **T-005 shard-B evidence** (NONBAND_OWNERSHIP_R2_SHARD_B_2026-07-22.md:122–123): two rows cite `SUPERSEDED(test_cohort_provider_compiler.py)` — re-pointed in S5-1 when the investor pins relocate, before the file dissolves.
- **S1f0c decision literal** (test_d1n_s1f0c_lineage_terminal_owner_decision.py:511 pins `"CohortProviderCompiler.validate_planning_manifest"`; :663 pins the filename): changed only through a new decision record in that contract's own process (S5-3/S5-4), never a silent string edit.

## 7. Risks and guards

The inventory's risk register R1–R11 binds each failure mode (authority bypass, capability-gate loss, manifest drift, paid probe-surface change, completion-honesty collision, identity-fence loss, proof-verifier loss, URL ownership, dangling tombstones, module-path shims, investor drift) to the guard/test that must exist before cutover; the ladder in §4 is ordered so every guard lands before the behavior it protects can change. Paid-path notes: cohort mode adds zero probe calls (D1); dispatch allocation stays one run-id per lane bounded by `MAX_COHORT_PROVIDER_LANES`; any live validation is delta-only under the standing fail-closed triple gate.

## 8. Acceptance criteria

1. Zero references to `cohort_provider_compiler` anywhere (code, tests, comments, docs routing) after S5-4.
2. All C1–C16 pins green at every slice boundary; byte-compat baseline (test_cohort_selection.py:543) and hard-identity fences untouched throughout.
3. Corpus parity gate green before and during the S5-3 flip.
4. T-002 flipped to concrete ids; no dangling evidence rows; provenance/monolith registries consistent.
5. Independent review GO on this design before S5-1, and on S5-3/S5-4 before their paid-path validation.
