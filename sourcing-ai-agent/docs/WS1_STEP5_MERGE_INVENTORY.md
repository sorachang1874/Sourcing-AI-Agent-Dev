# WS1 Step 5 — Merge Inventory (factual base for the unification design)

> Status: EVIDENCE ARTIFACT 2026-07-23 — read-only inventory backing docs/WS1_STEP5_COHORT_SHARD_UNIFICATION_DESIGN.md. Compiled by a read-only recon pass at the current branch head; file:line references are as of this date and are not maintained forward — the design doc is the living document.


Date: 2026-07-23 · Branch: `governance-phase0-ttl-20260611` · Repo: `sourcing-ai-agent`
Ratified rulings (docs/REFACTOR_MASTER_PLAN.md:37): ① full merge into company_shard_planning — cohort becomes an input mode of the shard planner, CohortProviderCompiler retires, and the cohort identity/authorization/selection contracts (explicit cohort authority, execution identity, refinement fences from `cohort_selection`) must survive intact and be pinned item-by-item by the post-merge suite; ② `investor_firm_roster` stays OUTSIDE the abstraction as an independent, explicitly contracted strategy (`source_kind=funding_graph` marked as an out-of-abstraction exception in the unified artifact schema — no implicit special case).

All paths below are relative to `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/` unless absolute.

---

## Section 1 — Compiler call-surface inventory

Module: `src/sourcing_agent/cohort_provider_compiler.py` (1,572 lines). Public surface (module head, lines 34–47 constants; classes/functions below):

| Public name | Def line |
|---|---|
| `COHORT_PROVIDER_MANIFEST_VERSION = "cohort_provider_manifest.v1"` | 34 |
| `COHORT_PROVIDER_PLANNING_MANIFEST_VERSION = "cohort_provider_manifest.v2"` | 35 |
| `COHORT_PROVIDER_COMPANY_TARGET_VERSION = "canonical_company_target.v1"` | 36 |
| `COHORT_PROVIDER = "harvest_profile_search"` | 37 |
| `COHORT_EXECUTION_NOT_READY = "cohort_selection_execution_not_ready"` | 38 |
| `COHORT_EXECUTION_CAPABILITY_VERSION` / `_OWNER` | 39–40 |
| `COHORT_NON_LIVE_RUNTIME_POLICY_VERSION = "cohort_non_live_runtime.v1"` | 41 |
| `COHORT_HEADLINE_ROLE_PROOF_VERIFIER_ID` / `_REVISION` | 42–43 |
| `COHORT_PUBLIC_HEADLINE_SOURCE = "harvest_profile_search.headline"` | 44 |
| `COHORT_CANONICAL_PROFILE_URL_FIELD = "cohort_canonical_profile_url"` | 45 |
| `DEFAULT_COHORT_RESULT_LIMIT = 25` / `MAX_COHORT_PROVIDER_LANES = 10` | 46–47 |
| `cohort_provider_planning_manifest_schema()` | 210 |
| `class CohortProviderCompilationError(ValueError)` | 317 |
| `class CohortProviderExecutionError(RuntimeError)` | 335 |
| `class CohortExecutionCapability` (+ `.from_record`/`.to_record`) | 360 |
| `class VerifiedCohortRoleProof` | 480 |
| `class CohortRoleProofVerifier(Protocol)` | 485 |
| `class CohortHeadlineRoleProofVerifier` | 492 |
| `resolve_effective_role_targeting()` | 526 |
| `class CohortProviderCompiler` (`.compile`, `.compile_planning_manifest`, `.validate_planning_manifest`, `.validate_lane_result_rows`) | 561 |
| `cohort_execution_not_ready_result()` | 1051 |
| `cohort_execution_unavailable_result()` | 1080 |
| `cohort_execution_capability_for_runtime()` | 1101 |
| `canonical_cohort_profile_url()` | 1439 |

**scripts/: ZERO call sites** (no `cohort_provider_compiler` or symbol references in `scripts/` — the retirement has no live-ops script surface).

### src/ call sites (8 modules + 1 comment)

| File:line | Symbol(s) | Caller role |
|---|---|---|
| `src/sourcing_agent/planning.py:7,114` | `CohortProviderCompiler().compile(...)` → sets `acquisition_strategy.provider_execution_manifest` | **planner** — the primary planning-side mint of the v1 execution manifest |
| `src/sourcing_agent/plan_review.py:9,541,554,562` | `COHORT_PROVIDER_MANIFEST_VERSION`, `CohortProviderCompiler().compile(...)` | **plan-review** — recompiles `provider_execution_manifest` from the reviewed canonical request when stored schema_version matches v1; compiler is documented as "sole owner" (line 541) |
| `src/sourcing_agent/acquisition_plan_preview.py:29–34,50,173,314,803,817,1070,1084,1108,1143` | `COHORT_PROVIDER_COMPANY_TARGET_VERSION`, `MAX_COHORT_PROVIDER_LANES`, `CohortProviderCompilationError`, `CohortProviderCompiler` (`compile_planning_manifest` owner at 803/1070), `cohort_provider_planning_manifest_schema` | **api/planner (D1n plan preview)** — mints + validates the v2 planning manifest; re-exports company-target schema version as `CANONICAL_COMPANY_TARGET_SCHEMA_VERSION` (line 50); budget bound check vs MAX lanes (1143) |
| `src/sourcing_agent/acquisition_strategy.py:6,221` | `resolve_effective_role_targeting` | **planner (strategy resolution)** — role-targeting resolution imported FROM compiler into strategy; on merge this helper is a natural early move into the planning layer |
| `src/sourcing_agent/acquisition_start_v2.py:46,1237` | `MAX_COHORT_PROVIDER_LANES` | **api (start v2)** — budget `max_provider_calls` bound check |
| `src/sourcing_agent/orchestrator.py:94–97,2991,3000,4332,4341,4394,60515,60521` | `CohortProviderCompilationError`, `cohort_execution_not_ready_result`, `cohort_execution_unavailable_result` | **orchestrator** — execution fail-closed gates: not-ready result minted at plan/dispatch boundaries (2991/3000/4332/4341/60515), unavailable result at 4394; raises CompilationError at 60521 |
| `src/sourcing_agent/acquisition.py:30–34,2740,2757,2782,2966,3451` | `CohortProviderCompiler` (2740 compile, 3451 expected-manifest recompile equality check), `cohort_execution_capability_for_runtime` (2757), `CohortHeadlineRoleProofVerifier` (2782), `COHORT_CANONICAL_PROFILE_URL_FIELD` (2966) | **executor/dispatcher (paid path)** — runtime capability gate + manifest-equality guard before dispatch; canonical profile URL read on results |
| `src/sourcing_agent/harvest_connectors.py:18–24,149–184,1293–1416,2838` | `COHORT_PUBLIC_HEADLINE_SOURCE`, `CohortExecutionCapability`, `CohortProviderCompilationError` (raised 149–184, 1318), `CohortProviderCompiler` (instantiated 1308), `CohortProviderExecutionError` (raised 1356–1416), `CohortRoleProofVerifier` | **provider executor (paid dispatch surface)** — executes compiled lanes against harvest; capability + verifier are the live-safety enforcement points; headline source stamped at 2838 |
| `src/sourcing_agent/agent_runtime_namespace_ref.py:54` | comment only: "path-bearing record owned by cohort_provider_compiler" | ownership note — must be re-pointed at the merged owner |

### tests/ call sites

| File | Lines | Symbols / role |
|---|---|---|
| `tests/test_cohort_provider_compiler.py` | 16 (import block), 189/208/227 (patches `current_runtime_environment`) | **dedicated compiler suite** — dissolves into the shard-planner suite on merge |
| `tests/test_cohort_provider_runtime_safety.py` | 10–18, 58–203, 317–378 | runtime-safety pins: `CohortExecutionCapability` round-trip (79), `cohort_execution_capability_for_runtime` fail-closed on missing runtime (95), compile blocked without capability (136–178), `CohortHeadlineRoleProofVerifier` (317), forged `cohort_canonical_profile_url` overwrite via `validate_lane_result_rows` (369–378) |
| `tests/test_cohort_selection.py` | 17–20, 116–117, 733–760 | selection→compiler bridge: compiles under `CohortExecutionCapability`; patches `cohort_execution_capability_for_runtime` module path (733/756 — **module-path-coupled patch targets break on merge**) |
| `tests/test_d1n_acquisition_plan_preview.py` | 14, 40–46, 283–300, 312–319, 549–573, 602, 642–655, 1038 | planning-manifest v2 pins: schema_version/provider/execution_blocker (283–289), `validate_planning_manifest` idempotence (300), monkeypatches `CohortProviderCompiler.compile` (312–319), v2-vs-v1 rejection (642–655) |
| `tests/test_plan_review_location_apply.py` | 32, 103, 320, 483, 599, 748 | plan-review recompile equality: expected v1 manifest = `CohortProviderCompiler().compile(...)` for persisted/resolved requests |
| `tests/test_cohort_location_request.py` | 28, 551, 583 | location-request compile behavior |
| `tests/test_tml_flexible_targeting_fixtures.py` | 10–14, 69, 110, 127–129 | TML fixtures pin manifest constants + `execution_blocker`; asserts on `"cohort_provider_compiler"` module name literal at 128 |
| `tests/test_d1n_s1f0c_lineage_terminal_owner_decision.py` | 511, 663 | pins literal string `"CohortProviderCompiler.validate_planning_manifest"` as `planning_validator` (511) and a `"cohort_provider_compiler.py"` filename entry (663) — **both are name-literal tombstone risks on merge** |
| `tests/provenance_baseline.py` | 32 | baseline registry lists `"test_cohort_provider_compiler.py"` — registry row must move with the suite |
| `tests/test_frontend_targeting_preview.py` | 756 | comment referencing "backend CohortProviderCompiler" in a production-shaped fixture |

---

## Section 2 — Cohort contract inventory (the contracts that must survive)

Contract owner module: `src/sourcing_agent/cohort_selection.py` (941 lines). Canonical doc: `docs/COHORT_SELECTION_CONTRACT.md` (with full ownership matrix — each row there is a survival obligation). Successor contract family: `src/sourcing_agent/cohort_execution_contract.py` (S1f0c FF-SCHEMA; pins `cohort_execution_capability.v2`, `cohort_execution_envelope.v1`, `cohort_execution_lane_result.v2`, `cohort_candidate_member.v1`, `cohort_candidate_set.v1`, `cohort_execution_result.v2`, `cohort_execution_commit.v1`; retains v1/manifest.v1/v2 as history — exact-version lookup, auto-upgrade forbidden).

Primary pin suite: `tests/test_cohort_selection.py` (three classes: `CohortSelectionContractTest`:65, `CohortSelectionIngressTest`:625, `CohortSelectionApiTest`:1605, `HardIdentityReuseGuardIntegrationTest`:1817).

| # | Contract | Owner (src) | Current pin (test file:line) |
|---|---|---|---|
| C1 | v1 wire object validation + canonical ordering + fail-closed unknowns/sources (`normalize_cohort_selection`, `validate_external_cohort_selection_payload`) | cohort_selection.py:112,174 | test_cohort_selection.py:149,171,183 |
| C2 | Registry + selection identity digests (`cohort_selection_digest`:91, `cohort_selection_registry_digest`:85, `COHORT_SELECTION_REGISTRY_VERSION`) | cohort_selection.py:16–17,85,91 | test_cohort_selection.py:94,114 (digest pins registry+semantics, not source/order) |
| C3 | **Explicit cohort authority** — user-explicit object wins over model patches; inferred/legacy_adapter are provenance snapshots not locks (`apply_user_explicit_cohort_authority`:404, `remove_explicit_cohort_mirror_patch_fields`:435) | cohort_selection.py:404,435 | test_cohort_selection.py:626 (model normalization cannot override), :661 (inferred is not a lock), :1509 (refinement model cannot mutate) |
| C4 | **Refinement fences** — external flat refinement conflicting with authority rejected (`validate_refinement_patch_against_explicit_cohort`:463; error codes `cohort_selection_refinement_not_supported`, `cohort_selection_mirror_conflict`) | cohort_selection.py:463 | test_cohort_selection.py:1557 (+4 more mirror-conflict pins in file), tests/test_request_scope_owner_fencing.py (1 pin), tests/test_tml_flexible_targeting_fixtures.py (1 pin) |
| C5 | **Execution identity signature** — user-explicit cohort bound to reuse identity via registry-pinned digest; absent object keeps legacy signature byte-for-byte (`cohort_execution_identity_for_signature`:518, `source_request_covers_explicit_cohort`:535) | cohort_selection.py:518,535 | test_cohort_selection.py:424,490,543 (byte-compat baseline), :1107–1431 (reuse/idempotency/asset-plan/force-fresh fences); symmetric hard-identity fences in `HardIdentityReuseGuardIntegrationTest`:1982–2204; consumed by `src/sourcing_agent/request_matching.py` (pinned also in tests/test_request_matching.py) |
| C6 | Mirror discipline — flat mirrors exact-equal or 400 pre-write; missing mirrors installed | cohort_selection.py:804 (`_verify_or_install_flat_mirror`), :836 (intent-axes mirrors) | test_cohort_selection.py:253,282,1686,1785 |
| C7 | Plan-review merge owner (`merge_plan_review_cohort_selection`:303) — exact replay accepted, legacy atomically upgraded, zero-write on failure | cohort_selection.py:303 | test_cohort_selection.py:320,366,596,770,816,885,973,1016 |
| C8 | External criteria request aliases single owner (`prepare_external_criteria_request_payload`:186) | cohort_selection.py:186 | test_cohort_selection.py:196,221,238,1716,1752 |
| C9 | Effective role targeting — user-explicit role list is COMPLETE role authority; empty list = all roles + suppresses inference (`resolve_effective_role_targeting`) | **cohort_provider_compiler.py:526** (must be re-homed by the merge; consumed by acquisition_strategy.py:221) | test_cohort_selection.py:456,490,526; test_cohort_provider_compiler.py |
| C10 | Lane compilation semantics — one lane per (status, role); `any`=ordered union+identity dedupe, `all`=per-status intersection then union; server-derived `cohort_lane_membership`; registry-ordered stable lane ids | cohort_provider_compiler.py:561 (`CohortProviderCompiler`) | test_cohort_provider_compiler.py (dedicated), test_tml_flexible_targeting_fixtures.py:69–129 |
| C11 | Execution readiness capability — typed `CohortExecutionCapability` issued only by `cohort_execution_capability_for_runtime()` for validated isolated runtime; fail-closed absent/mismatched; non-live-only policy | cohort_provider_compiler.py:360,1101 | tests/test_cohort_provider_runtime_safety.py:58–203; test_cohort_selection.py:717,733–760 |
| C12 | Provider plan authority — capability-free `provider_execution_manifest` planner-minted and exact-recompiled before any provider call | planning.py:114 + plan_review.py:562 + acquisition.py:3451 | tests/test_plan_review_location_apply.py:103,320,483,599,748; test_d1n_acquisition_plan_preview.py:549–655 |
| C13 | All-role proof — versioned verifier (`CohortHeadlineRoleProofVerifier`), provider-authored role fields ignored | cohort_provider_compiler.py:480–525 | test_cohort_provider_runtime_safety.py:317–346 |
| C14 | Canonical cohort person URL — server-owned `cohort_canonical_profile_url`, forged input overwritten (`canonical_cohort_profile_url`:1439, `validate_lane_result_rows`) | cohort_provider_compiler.py:1439 | test_cohort_provider_runtime_safety.py:369–378 |
| C15 | Execution artifact/commit family (capability v2, envelope, lane result, candidate set/member, result v2, commit v1; anti-cycle digests; insert-once commit) | src/sourcing_agent/cohort_execution_contract.py (consumes `docs/modules/serving-product/contracts/filter_projection_lineage_fixed_forward_decision_v1.json` verbatim) | tests/test_cohort_execution_contract.py; tests/test_d1n_s1f0c_lineage_terminal_owner_decision.py:511 (pins validator literal `"CohortProviderCompiler.validate_planning_manifest"`) |
| C16 | Legacy adaptation (`effective_cohort_selection`:379, lazy `source=legacy_adapter`, no write-back) | cohort_selection.py:379 | test_cohort_selection.py:400 |

Key merge fact: `company_shard_planning.py` ALREADY consumes cohort authority — `company_shard_planning.py:5` imports `explicit_cohort_selection`; :70–73 treats ONLY user-explicit cohort as function selection (`request_scoped_roster_function_ids`); :377 documents user-explicit cohort role buckets as shard-policy input priority 1. The "cohort input mode" is a widening of an existing dependency, not a new one.

---

## Section 3 — Shard-planner input surface (what "cohort input mode" sits beside)

Module: `src/sourcing_agent/company_shard_planning.py` (1,414 lines). Consumers: `planning.py`, `plan_review.py`, `acquisition.py`, `harvest_connectors.py`, `asset_reuse_planning.py`, `snapshot_materializer.py`, `scripts/live_former_lane_run.py`. Pin suites: `tests/test_company_shard_planning.py`, `tests/test_request_scoped_roster_shards.py`, `tests/test_harvest_connectors.py`.

### Current input modes / policy builders

| Surface | Location | Notes |
|---|---|---|
| Policy `mode` enum: `partition_mece` \| `keyword_union` | company_shard_planning.py:474–476 (`normalize_company_employee_shard_policy`; unknown → `partition_mece`; auto-flip to `keyword_union` when only keyword_shards present at :486–487) | A cohort mode would either become a third `mode` value or (more consistent with Step 4a precedent) a new request-scoped policy builder emitting existing modes |
| `build_default_company_employee_shard_policy` | :404 | THE unified roster policy — no large/small-org fork (operator directive 2026-07-20, re-ratified 2026-07-22); per-function probe roots; `strategy_id="adaptive_us_technical_partition"` retained as legacy identifier; single-writer `locations` semantics (None→US default, `[]`→opt-out) |
| `build_request_scoped_company_employee_query_plan` | :106 | request-scoped roster query plan (locations + function_ids) |
| `build_request_scoped_keyword_union_shard_policy` | :173 | **WS1 Step 4a precedent (2026-07-22)** — scoped/keyword request expressed as keyword_union policy inside the same probe-driven machinery; `strategy_id="request_scoped_keyword_union"`; one keyword shard per request keyword; function ids + location axes in root scope; design intent preserved from tombstone T-001. This is the template shape a cohort input mode should follow |
| `build_request_scoped_former_search_shard_plan` | :233 | FORMER-member recall lane: one shard per function id, `FORMER_FUNCTION_SHARD_PLAN_MARKER`(:54) lets exactly the plan-derived id through the Harvest broad-former guardrail (GDM functionIds ["19"] incident); empty selection → one broad markerless shard |
| Planner entry `plan_company_employee_shards_from_policy(policy, probe_fn)` | :526 | request_function_ids own the partition axis FIRST (`_plan_request_function_shards`:1124), then mode dispatch: `keyword_union` → `_plan_keyword_union_shards`:837, else MECE partition / `_plan_single_scope_root`:1020 |

### Cohort authority already inside the planner

- `request_scoped_roster_function_ids`(:57–73) — imports `explicit_cohort_selection` (:5); ONLY `source=user_explicit` cohort counts as a function selection; empty role list = all roles.
- `resolve_roster_lane_function_ids`(:367) — single owner of the roster lane's PAID function selection with documented authority order: (1) user-explicit cohort role buckets → (2) AI-authored buckets only in `MODEL_WRITTEN_PLANNING_MODES`(:364) → (3) `TECHNICAL_ROSTER_FUNCTION_IDS`(:35) default. Heuristic-mode request fields never count (GDM "pm"-alias incident).

### Probe/cap/completion honesty (must extend to the cohort mode)

- `shard_summary_is_truncated`(:314) + `TRUNCATED_ROSTER_STOP_REASONS`(:311) — truncation evidence per shard.
- `resolve_segmented_roster_completion`(:325) — one honest completion contract: `completed` only when every expected shard present AND no truncation evidence; both direct segmented fetch and background worker reconciliation route through it (Step 2b-i, master plan line 98).
- `allow_overflow_partial` + `provider_result_cap` (`FULL_COMPANY_EMPLOYEE_RESULT_CAP=2500`:11) — capped shard keeps explicit overflow metadata; capped coverage never reported complete.
- Probe discipline: `probe_max_pages`/`probe_page_limit` policy fields; probing via injected `probe_fn` (:526–530); `_normalize_probe_summary`:1327.

### Gap the merge must bridge

The compiler's lane semantics have no shard-planner analogue today: (a) employment-status axis (current/former lane split — the shard planner splits former via a SEPARATE plan builder, the compiler splits per (status, role) lane in one manifest); (b) `role_match=all` intersection semantics + role-proof verifier; (c) `cohort_lane_membership` provenance rows; (d) the v1/v2 manifest schema + digests + `MAX_COHORT_PROVIDER_LANES` budget allocation; (e) execution capability gating (compiler refuses to compile without a valid non-live capability — the shard planner has no capability concept; execution-side gates live in acquisition.py/harvest_connectors.py). A "cohort input mode" must either carry these as policy/plan fields or explicitly relocate them to the execution boundary.

---

## Section 4 — T-002 tombstone homes

Tombstone row: `docs/governance/REGRESSION_INDEX.md:62` — T-002, file `tests/test_request_scoped_roster_shards.py`, tests `test_plan_builds_request_shards_for_cohort_role_request` + `test_plan_manifest_parity_for_cohort_role_request`. Protected: "cohort roles produce roster shards via legacy metadata". Retired because the landed design has cohort bypass legacy metadata entirely via CohortProviderCompiler. `superseded_by`: "`tests/test_cohort_*.py` suite; unification see master plan WS1 Step 5". Companion evidence: `docs/RESIDUAL_LEDGER.md:53` (R-034 closed, exit evidence 2026-07-22 — these two were part of the 17 tests pinning 233a31a intermediate-state contracts).

Recovered design intent (from `git show 2198dd3^:./tests/test_request_scoped_roster_shards.py`, lines 300 and 759):

1. **`test_plan_builds_request_shards_for_cohort_role_request`** (source :300–326): a cohort role request (`research`+`engineering`, US default) planned via `build_sourcing_plan` yields `strategy_type=full_company_roster` with `company_employee_shard_strategy == REQUEST_FUNCTION_PARTITION_STRATEGY_ID`, empty shard policy, exactly 2 per-function shards (`function_ids ["24"]` then `["8"]`, locations `["United States"]`), `company_employee_base_filters == {"locations": ["United States"]}`, and `intent_view` mirroring shards+strategy. **Design intent: cohort role buckets drive the roster lane's per-function shard partition through plan metadata.**
2. **`test_plan_manifest_parity_for_cohort_role_request`** (source :759–781): for a cohort request with `target_locations=["Germany"]`/`exclude=["France"]`, the shards in plan task metadata are byte-equal to `build_request_scoped_company_employee_query_plan(...)` output for the same axes — **single-source parity between plan metadata and the unified query-plan builder**.

**Where the behavior re-pins after the merge**: exactly these two assertions become the acceptance tests of the cohort input mode — (a) explicit cohort → per-function shard roots inside `plan_company_employee_shards_from_policy` / the cohort-mode policy builder (T-002 intent #1), and (b) parity between the planner-stored cohort plan artifact and the single request-scoped builder output (T-002 intent #2, now generalized to the merged manifest/policy artifact). Until the merge lands, the intent is carried indirectly by: `company_shard_planning.py:57–73` (`request_scoped_roster_function_ids` — cohort as function selection), `resolve_roster_lane_function_ids`:367 authority ladder, and the compiler-side lane tests (`tests/test_cohort_provider_compiler.py`, `tests/test_plan_review_location_apply.py` parity pins). The REGRESSION_INDEX rule "deleting a grandfathered baseline file requires a tombstone row first" (:50) means the merge slice that lands the cohort input mode must flip T-002's `superseded_by` to the concrete new test ids.

---

## Section 5 — investor_firm_roster surfaces (the out-of-abstraction standalone strategy)

Ruling ②: stays outside the shard-planner abstraction as an independent strategy, explicitly contracted, marked `source_kind=funding_graph` in the unified artifact schema as the declared exception.

### Strategy-literal surfaces (src/)

| File:line | Surface |
|---|---|
| `src/sourcing_agent/acquisition_strategy.py:569,582` | strategy resolution: explicit-override whitelist member; `"investor" in categories` → `investor_firm_roster` |
| `acquisition_strategy.py:715,738` | structured resolver: override whitelist + `investor_population` reason-code branch (`decision_source="population"`) |
| `acquisition_strategy.py:1090,1101–1102` | `roster_sources` map: `["funding_graph", "investor_firm_roster", "linkedin_people_search"]` (the ONLY strategy whose roster_sources include `funding_graph` — 3 total `funding_graph` refs in src/: acquisition_strategy.py:1090,1102 + plan_review.py:865) |
| `acquisition_strategy.py:1634` | strategy-specific explanation branch |
| `src/sourcing_agent/planning.py:52,462,499` | `FULL_PROFILE_PREFETCH_STRATEGY_TYPES` member; plan-build branches |
| `src/sourcing_agent/plan_review.py:95,111,848,864–865` | review display/apply branches; desired-strategy switch re-mints the funding_graph roster_sources |
| `src/sourcing_agent/execution_semantics.py:12` | `_ACQUISITION_MODE_OVERRIDES` member |
| `src/sourcing_agent/execution_preferences.py:141,549` | override whitelist + text alias `"investor firm"` |
| `src/sourcing_agent/review_plan_instructions.py:70,271,534` | review instruction whitelist + alias mapping |
| `src/sourcing_agent/model_provider.py:356` | model prompt enumerates the 4-strategy closed set |
| `src/sourcing_agent/agent_runtime.py:79` | runtime branch on strategy_type |
| `src/sourcing_agent/orchestrator.py:77253` | orchestrator strategy branch |
| `src/sourcing_agent/acquisition.py:991–992,996,3934,3984–4011,5475–5565,7751` | executor: `_acquire_investor_firm_roster`(:5475); emits `source_kind="investor_firm_roster"` today (:3999,5538,5553 — **note: artifact source_kind is currently the strategy literal, NOT `funding_graph`; the ratified marker is a contract change to make**); enrichment_mode `investor_firm_roster_existing_assets` |

### Test pins

- `tests/test_cohort_provider_compiler.py:1097,1110,1456` — investor strategy behavior is pinned INSIDE the dedicated compiler test file that dissolves on merge.
- `docs/governance/NONBAND_OWNERSHIP_R2_SHARD_B_2026-07-22.md:122–123` — two T-005 tombstone rows list `SUPERSEDED(test_cohort_provider_compiler.py)` as the ownership evidence for `test_investor_firm_workflow_uses_tiered_firm_roster` and `test_investor_firm_roster_uses_snapshot_assets_without_sqlite_fallback`. **If test_cohort_provider_compiler.py dissolves without relocating these pins, two tombstone evidence chains dangle.**

### What the standalone explicit contract must cover

1. Strategy membership in the closed 4-strategy set (execution_semantics/execution_preferences/review_plan_instructions/model_provider whitelists — 6 duplicated literal lists today; contract should name one owner).
2. Resolution triggers: `investor` category → strategy; explicit override; text alias "investor firm".
3. `roster_sources = [funding_graph, investor_firm_roster, linkedin_people_search]` duplicated in acquisition_strategy.py:1090 and plan_review.py:865 — single-source it.
4. Artifact marking: ratified `source_kind=funding_graph` vs current `source_kind="investor_firm_roster"` (acquisition.py:3999,5538,5553) — migration/aliasing must be explicit in the unified artifact schema.
5. Executor entry `_acquire_investor_firm_roster` (tiered firm plan + existing investor asset normalization, docs/ARCHITECTURE.md:197) and its full-profile-prefetch membership (planning.py:52).
6. Non-participation guarantee: investor strategy never routes through the cohort input mode / shard policy machinery, pinned by a test that lives OUTSIDE the dissolving compiler suite.

---

## Section 6 — Risk register (ordered; each with the guard/test that must exist before cutover)

| # | Failure mode | Mechanism | Guard/test required before cutover |
|---|---|---|---|
| R1 | **Cohort authority bypass on the paid path** | The cohort input mode reads role/status fields without the `source=user_explicit` check that `explicit_cohort_selection` enforces (company_shard_planning.py:70–73); inferred/legacy/model-authored buckets drive paid shard queries — GDM functionIds ["19"] incident class | Authority-ladder pins re-run against the merged path (test_cohort_selection.py:626,661,1509; resolve_roster_lane_function_ids pins in tests/test_request_scoped_roster_shards.py); NEW pin: cohort-mode policy builder yields empty/legacy behavior for non-user_explicit source |
| R2 | **Execution-capability gate loss (fail-closed regression)** | Compiler refuses to compile without a valid isolated-runtime `CohortExecutionCapability` (compiler:360,1101; pinned test_cohort_provider_runtime_safety.py:58–203); shard planner has NO capability concept — a merged plan-time path could mint dispatchable cohort plans without the non-live gate | Capability issuance + exact-match enforcement relocated intact to the execution boundary (acquisition.py:2757, harvest_connectors.py:1293–1416) and every runtime-safety pin re-homed BEFORE the compiler deletes; live triple-gate env semantics untouched (CLAUDE.md fail-closed rule) |
| R3 | **Manifest drift** | v1 (`cohort_provider_manifest.v1`) exact-recompile equality guards dispatch (acquisition.py:3451, plan_review.py:562) and v2 planning manifest pins (test_d1n_acquisition_plan_preview.py:283–300,642–655); cohort_execution_contract.py forbids auto-upgrade/shape inference — a merged artifact that is "almost" the old manifest breaks equality guards or, worse, passes with changed semantics | Corpus parity test: old `CohortProviderCompiler().compile/compile_planning_manifest` output byte-equal to merged-planner output for a pinned request corpus (run while both paths coexist); any successor schema lands as a NEW exact version in cohort_execution_contract.py, never a mutated v1/v2 |
| R4 | **Paid-dispatch surface change via probe machinery** | Shard planner is probe-driven (`probe_fn`, probe_max_pages/probe_page_limit); the compiler path allocates one global budget across lanes with `auto_probe=false` and single async run-id dispatch (COHORT_SELECTION_CONTRACT.md compilation section) — cohort-as-shard-mode could silently add probe provider calls or re-shape lane→call allocation (cost + duplicate-run risk) | Call-count/budget assertion tests on the cohort mode (`MAX_COHORT_PROVIDER_LANES` bound, one-run-id-per-lane, no probe calls unless explicitly contracted); delta-only discipline in any live validation |
| R5 | **Completion-honesty semantics collision** | Cohort: whole-manifest failure, zero partial publication, `cohort_execution_result.v2` requires `lane_coverage_status=="complete"`; shard roster: partial allowed WITH honesty metadata (`resolve_segmented_roster_completion`:325). Merging could relax cohort's all-or-nothing publication or infect roster with all-or-nothing stalls | Explicit design decision in the blueprint + pins on both sides: cohort-mode publication blocked on incomplete lane coverage (re-pin of compiler combiner tests); roster partial-honesty pins (test_request_scoped_roster_shards SegmentedCompletionContractTest) stay green |
| R6 | **Reuse-identity fence loss** | `cohort_execution_identity_for_signature`/`source_request_covers_explicit_cohort` (cohort_selection.py:518,535) bind explicit cohorts out of legacy reuse families; a merged plan artifact that changes what feeds the signature (or the absent-object byte-compat baseline, test_cohort_selection.py:543) corrupts idempotency/asset-reuse/projection fences | Byte-compat baseline test stays untouched; HardIdentityReuseGuardIntegrationTest (test_cohort_selection.py:1817–2204) + test_request_matching.py green on the merged path |
| R7 | **role_match=all proof-verifier loss** | Intersection semantics + versioned `CohortRoleProofVerifier` preflight (provider-authored role fields ignored) have no shard-planner analogue; a naive port could accept provider-authored role evidence | Verifier preflight + provider-field-ignore pins re-homed (test_cohort_provider_runtime_safety.py:317–346); `cohort_lane_membership` server-derivation pin survives |
| R8 | **Canonical identity/URL ownership loss** | `canonical_cohort_profile_url` + forged-field overwrite (`validate_lane_result_rows`, pinned test_cohort_provider_runtime_safety.py:369–378) and person-identity dedupe must keep a single owner post-merge | Re-homed pins + reference sweep that no second normalization appears (Contract Field Ownership rule) |
| R9 | **Tombstone/evidence chains dangle** | T-002 superseded_by = "test_cohort_*.py suite" (REGRESSION_INDEX.md:62); shard-B rows 122–123 = SUPERSEDED(test_cohort_provider_compiler.py); tests/provenance_baseline.py:32 lists the file; lineage decision pins literals `"CohortProviderCompiler.validate_planning_manifest"` (test_d1n_s1f0c...:511) and `"cohort_provider_compiler.py"` (:663) | Same-slice doc/pin updates: T-002 flipped to concrete new test ids; shard-B evidence rows re-pointed; provenance baseline updated; the S1f0c decision-manifest literal changed only through its own decision process (new decision record), never a silent string edit |
| R10 | **Module-path coupling breaks silently** | Monkeypatch targets `sourcing_agent.cohort_provider_compiler.*` (test_cohort_selection.py:733,756; test_cohort_provider_compiler.py:189–227), module-name literal assert (test_tml_flexible_targeting_fixtures.py:128), ownership comment (agent_runtime_namespace_ref.py:54) — a re-export shim would keep these green while hiding the true owner (hidden-fallback anti-pattern) | Cutover reference sweep for `cohort_provider_compiler` (must reach zero, incl. comments); NO permanent import shim — if a bridge is unavoidable it is report-visible with a recorded removal condition |
| R11 | **investor_firm_roster contract drift** | Standalone contract requires `source_kind=funding_graph` marking, but code emits `source_kind="investor_firm_roster"` today (acquisition.py:3999,5538,5553); strategy whitelist duplicated in 6 modules; its only surviving behavior pins live inside the dissolving compiler test file (test_cohort_provider_compiler.py:1097–1456) | Explicit contract doc + owner for the strategy literal set; source_kind migration handled as a contract-field change (owner matrix + preflight per Contract Field Ownership); investor pins relocated to a standalone suite BEFORE test_cohort_provider_compiler.py dissolves; non-participation pin (investor never enters cohort mode) |

Cutover ordering implied by the register: (1) relocate contracts + pins that live in the retiring module/suite (R2, R7, R8, R11 pins) → (2) land cohort input mode beside existing modes with corpus parity (R3) while both paths coexist → (3) flip callers planner-first (planning.py, plan_review.py, acquisition_plan_preview.py), keeping execution-boundary gates (R2) → (4) retire the compiler + zero-reference sweep + tombstone/doc closure (R9, R10).
