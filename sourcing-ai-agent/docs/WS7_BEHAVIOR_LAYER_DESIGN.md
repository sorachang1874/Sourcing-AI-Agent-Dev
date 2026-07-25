# WS7/W7.5 — Post-Chain Behavior Layer Design, two tiers (operator directive #5)

> Status: DESIGN DRAFT 2026-07-24 — **awaiting operator rulings on OQ1–OQ9 (§11) + independent review gate**. No implementation slice lands before the ruling batch is answered and the review verdict for the contract slices is recorded (contract-heavy + cross-package + external-provider-touching → §12 protocol). Authority: operator directive #5 RATIFIED 2026-07-22 ([REFACTOR_MASTER_PLAN.md](REFACTOR_MASTER_PLAN.md) §6.5, verbatim: "后链路行为层抽象（两层）：①个人基本信息补充层——如经 Grok 定位 X 账号与 Bio，**执行一次足够**；②特定方向信息收集与判断层——按需执行（例：pre-train 研究方向、TBD 组织/团队方向、Gemini 模型方向、Codex 产品方向）。X-First 项目对方向划分已有规则与维护方法，**动刀前先了解**。semantic scholar（论文与研究方向的获取、总结）未来纳入同一「定位来源→收集→判断」抽象方法论。目标 = 数据资产更完整的后链路。"); factual base: [WS7_STRONG_AGENT_RECON_2026-07-22.md](WS7_STRONG_AGENT_RECON_2026-07-22.md) §3/§5-Q13–Q15 plus a dedicated read-only behavior-layer recon (2026-07-24) whose findings are transcribed with anchors throughout. Design shape mirrors the proven [WS7_AI_BATCH_DIVIDER_DESIGN.md](WS7_AI_BATCH_DIVIDER_DESIGN.md) (议案①), [WS7_AI_PROMOTE_DESIGN.md](WS7_AI_PROMOTE_DESIGN.md) (议案③) and [WS7_COMPENSATION_DESIGN.md](WS7_COMPENSATION_DESIGN.md) (议案④).

```
status: design-draft    owner: operator (rulings pending)
canonical-path: sourcing-ai-agent/docs/WS7_BEHAVIOR_LAYER_DESIGN.md
drafted: 2026-07-24     oracle-pin: NONE EXISTS — S0 is mandatory (§8/§12)
```

Unprefixed `file:line` anchors refer to `sourcing-ai-agent/src/sourcing_agent/`; test anchors to `sourcing-ai-agent/tests/`; `XF/` anchors to the sibling package `x-first-researcher-sourcing/`. Anchors verified on this tree (branch `governance-phase0-ttl-20260611`, 2026-07-24).

---

## 0. Ratified constraint + what makes this proposal DIFFERENT

Directive #5 is the **only** WS7 directive that spans two packages and whose subject matter is a *data-asset completeness* concern rather than a pipeline-execution concern. Three structural contrasts against the landed proposals:

| Axis | 议案① divider | 议案③ promote | 议案④ compensation | **议案⑤ behavior layer (this doc)** |
|---|---|---|---|---|
| Concern | efficiency (batch shape) | asset correctness (which snapshot serves) | durable retry plumbing | **asset completeness (what the record knows about a person)** |
| AI seat | one plan stage | one contested decision | **none** (no model at all) | **tier 2 only** — tier 1 is locate/idempotency plumbing, not judgment |
| Blast radius | one package, one module | one package, two modules | one package, one daemon | **two packages + a live script lane + an external provider (Grok)** |
| Existing oracle | existed (ruling-① prereq) | none → S0 forced | tick oracle = floor | **none, and the surface it would pin has 6 spot-check tests over a 27-row table (§8)** |
| Fallback direction | rule ladder + audit | keep incumbent | bounded retry → needs-human | **abstain (`ambiguous`) + audit — NOT the existing alias matcher (§6)** |

### 0.1 The load-bearing finding: this behavior layer already runs, on three tracks that do not recognize each other

| Track | Tier ① locate + basic info | Tier ② direction collect + judge | How "direction" is expressed | Durable? |
|---|---|---|---|---|
| **A. SAA public-web lane** | `x_url` is discovered → classified → adjudicated → promoted as a profile link (public_web_search.py:470/:527-535/:590/:883-935) | — (judges whether a link belongs to the person, never a direction) | — | ✅ PG 14-state machine (storage.py:12070-12092) + person-asset assertion (crm_public_web_owner.py:827-845) + typed command owner + recovery phase |
| **B. XF generalized orchestration** | Phase 3 profile hydration, **exactly one** `x_user_search` per resolved handle (`XF/docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:201-262`) | `analysis_mode × dimension_id × target_labels` → 4-state × 4-state + evidence refs (`XF/configs/research_orchestration_policy.v1.json:54-67`) | **typed catalog** (9 `scope_kind`) + runtime taxonomy | ❌ contract + validator only; provider-free, fixture-only |
| **C. live script chain (has really run; produced CSVs)** | `live_grok_collection_run.py` → `account_resolution` + `x_bio` | `live_luna_judge_run.py` → lab/pretraining 4-state | **free string** `target_direction="pre-training"` (`XF/src/x_first/luna_batch_runner.py:600`) and **hard-coded output keys** (`:132-137`) | ❌ operator-driven scripts, JSON/CSV files, no PG, no command owner, no idempotency, no recovery |

**Therefore directive #5 is not "invent a locate→collect→judge abstraction."** All three verbs already have ≥3 mature implementations each. The real engineering object is: **give Track C's capability Track A's durable discipline (idempotency, terminal reuse, force+nonce, recovery, delta-only paid dispatch) expressed in Track B's typed direction semantics (catalog + freshness + 4-state + evidence refs + abstain + trust ceiling)** — one registry-driven methodology, instantiated per source. Suture, do not build.

### 0.2 Hard constraints inherited from already-ratified decisions

1. **R-019 blocks new paid dispatch paths** ([RESIDUAL_LEDGER.md](RESIDUAL_LEDGER.md) R-019, `pending remediation`; 议案④ OQ5 made this a HARD rule). A Grok-backed tier-1 locate *runtime* is a new dispatch path → blocked until R-019 is remediated. §4.4 shows the design lands without one.
2. **议案③'s two-gate shape** — hard deterministic prerequisites fail-closed, AI judges only inside what the gates already admitted; the AI can only be MORE conservative. Tier 2 copies this exactly (§7).
3. **议案③ ruling ④'s conservative-failure direction** — a judgment failure never invents a positive state. Tier 2 fails to `ambiguous`, **consciously not** following 议案①'s rule-ladder fallback (§6).
4. **XF's package boundary** (`XF/AGENTS.md:24-29`) — X account identity ≠ canonical person identity; outputs are observations/proposals with `assertions` **empty**; no writes to PersonAsset/CRM/projection/export/outreach; integration is a **versioned artifact adapter, not direct runtime imports**.

---

## 1. What X-First already owns (动刀前先了解) — this design CONSUMES it, never forks it

### 1.1 The safety boundary (transcribed, not up for redesign)

`XF/AGENTS.md`: `:9-17` three independent recall axes (target-lab affiliation temporality / role / pretraining-experience temporality); `historical` is **not** a negative signal. `:18-19` **forbidden**: inferring ethnicity, nationality, race, citizenship, religion, gender or protected identity from name/language/region/school/community/graph position. `:20-23` fixture mode is default and never touches network/provider/model/credentials/live X. `:24` external-account identity = stable platform user id + handle history. `:25-26` provisional people use opaque `pp_x_<ULID>`; cross-source links are reversible proposals requiring human review. `:27` outputs are raw observations + evidence proposals, `assertions` must stay empty. `:29` integration = versioned artifact adapter, **not direct runtime imports**.

### 1.2 "Direction" in X-First is TWO orthogonal legs

**Leg 1 — the scope catalog**: 9 `scope_kind` values, enumerated identically in `XF/contracts/x.research_scope.catalog.v1.schema.json:74-86` (`$defs.scope_node.scope_kind`) and `XF/configs/research_orchestration_policy.v1.json:77-87` (`supported_scope_node_kinds`), mirrored a third time at schema `:151-167` (`$defs.coverage.scope_kinds`): `organization, model, product, application, research_program, capability, industry, team, initiative`. Runtime authority is the **policy** list (`XF/src/x_first/research_orchestration.py:190` raises `scope_catalog_node_kind_invalid`).

The operator's four worked examples map 1:1 onto this enum: pre-train research direction → `research_program`/`capability` (or a taxonomy label, leg 2); TBD org/team → `organization`/`team`; Gemini model → `model`; Codex product → `product`.

**Leg 2 — the runtime taxonomy** (`XF/docs/GENERALIZED_RESEARCH_ORCHESTRATION.md:10-19`): *"The target direction is a runtime taxonomy, not a pre-training field."* Structure = `dimension_id` + `labels[]{label_id, display_name, aliases[]}` (schema `:103-129`). Judgment output is **two independent axes** (`GENERALIZED_...md:119-127`): `relevance_state ∈ {target_core, target_adjacent, ambiguous, out_of_scope}` (policy `:54-59`) × `{affiliation_state, target_activity_state} ∈ {current, historical, ambiguous, unsupported}`.

### 1.3 The maintenance method (this is the part with teeth)

`research_orchestration.py:175-253` (validation) + `:528-592` (fail-closed freshness resolution):

| # | Rule | Anchor |
|---|---|---|
| M1 | Whole-table content addressing: `catalog_sha256 == canonical_sha256(catalog − that field)`; changing ANY field forces a full-table rehash | `:182-183` (`scope_catalog_hash_mismatch`) |
| M2 | `scope_id` pattern + uniqueness; alias casefold uniqueness per node | `:187-193` |
| M3 | Self-consistent freshness window: `last_verified_at ≤ generated_at` and `refresh_after > last_verified_at` | `:194-197` |
| M4 | Parents must exist, no self-edge, DAG (not tree) enforced by DFS | `:199-203`, `_validate_no_parent_cycle:156-172` |
| M5 | Taxonomy label ids unique; aliases casefold-unique within a label AND non-ambiguous **across** labels of one dimension | `:211-223` (`scope_catalog_label_alias_ambiguous`) |
| M6 | Every alias must be a query-safe literal (no leading/trailing space, no `"`, no `\`, no control chars) — **aliases ARE the recall surface** | `:219` → `_recall_query_term:112-120` |
| M7 | Coverage assertion: sorted member ids + `member_set_sha256`; members ⊆ (kind-matching ∧ reachable-from-root); `complete` ⟹ exactly that set and `open_gaps` empty; `complete` may not be `model_mediated_unverified` | `:225-253` |
| M8 | Use-time fail-closed: for each (root × required kind) **exactly one** coverage row (0 or ≥2 both refuse), status `complete`, `last_verified_at ≤ as_of < refresh_after`; every selected node fresh, `status != unknown`, `source_status != model_mediated_unverified` | `:528-592` |
| M9 | Retirement is a `status` flip (`active|historical|unknown`) + rehash — **no hard delete**; `historical` remains selectable | schema `:87`; `AGENTS.md:16-17` |
| M10 | Model-discovered new directions land in `exploratory_findings` only; they never silently rewrite the request taxonomy or the catalog | `GENERALIZED_...md:69-71`, `:108-110` |

### 1.4 What this design consumes, and where the two would drift

| X-First asset | This design's stance | Drift risk if we get it wrong |
|---|---|---|
| `x.research_scope.catalog.v1` **shape** + validator (M1–M8) | **CONSUME as the single validation authority.** SAA never re-implements a second validator. | Two validators → the enum/freshness semantics diverge silently; today the 9-kind enum is already triple-hardcoded with **no literal-comparison test** (recon G-1), so a fork would be a third and fourth copy. |
| Catalog **content** (which directions exist) | **SAA becomes the producer** — the role the owner matrix names (`GENERALIZED_...md:276`) but which **does not exist today**: the only catalog instance in either package is `XF/fixtures/research_scope_catalog_fixture_v1.json`, all nodes `source_status: fixture_synthetic` (recon G-2). | If SAA keeps its ungoverned tuples *and* consumes a catalog, the same direction exists twice with different aliases. §3.4 single-sources the alias face to kill this. |
| 4-state × 4-state judgment vocabulary + `evidence_refs` + abstain | **CONSUME verbatim** as the tier-2 output contract. | SAA's current facet derivation has no abstain state, no evidence refs, no temporality (recon G-10) — the two vocabularies are not inter-translatable, so any "mapping layer" would be a lossy invention. |
| Trust ladder (`source_bound` > `model_mediated_unverified`; `authorized_transition_count` stays 0) | **CONSUME as a ceiling per source binding** (§3.1). | A model-mediated bio silently becoming a `source_bound` person-asset fact is exactly the failure XF's ladder exists to prevent. |
| `paper_conference_linkage` query family (`XF/configs/query_families.v1.json`) | **Leave in XF.** It resolves *papers → X accounts*; Semantic Scholar in SAA resolves *person → publications → directions*. Different jobs, both legal. | Collapsing them would push publication ingestion into a package forbidden to write PersonAsset facts. |
| Package seam | **The versioned artifact adapter is the ONLY seam** (`AGENTS.md:29`). | Today `scripts/live_grok_collection_run.py:65-66` does `sys.path.insert(...)` + `from x_first import luna_batch_runner`, and `x_first_portable_adapter.py` / `x_first_portable_package.py` / `x_first_simulate_owner.py` have **zero production callers** (only tests + `tests/provenance_baseline.py`). See D-B4/OQ9. |

---

## 2. The current behavior surface, from code

### 2.1 Tier ① today (SAA): X locate exists, **Grok does not**, and X Bio is not a concept

`grep -rni "grok" src/` → **zero hits.** X presence is one link type among many:
- Query planning: `plan_candidate_public_web_queries` (public_web_search.py:470); the X-specific query is `f"{quoted_name} {quoted_company} (site:x.com OR site:twitter.com)"` (`:527-535`).
- `x_url` is an `ENTRY_LINK_TYPES` / `PUBLISHABLE_PROFILE_LINK_TYPES` / `ADJUDICATION_CANDIDATE_PROFILE_LINK_TYPES` member (`:35-69`); model aliases `x_profile`/`twitter_profile` normalize to it (`:70-88`); fetch-queue cap `FETCH_QUEUE_ENTRY_TYPE_CAPS["x_url"] = 1` (`:113-122`) — a **per-run budget**, not an idempotency marker.
- Judgment: `classify_public_web_url` (`:590`), `_looks_like_x_profile_url` (`:1375`), model adjudication (`:883/:905/:935`, prompt `model_provider.py:2612`), quality refusal `x_link_not_profile` (public_web_quality.py:503).
- Canonical home: `_public_web_signal_assertion_type` (crm_public_web_owner.py:827-845) maps `x_url`/`twitter_url` → assertion type `x_url`; the assertion carries `authority` × `verification_status ∈ {active, needs_review, rejected, superseded}` × `valid_from/valid_to` ([PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md](PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md):133-143). **Tier 1's value already has a canonical home with authority, verification state and temporal validity. This design does not need a new asset table.**
- **"X Bio" does not exist in SAA.** `bio` appears only in LinkedIn semantics (person_identity.py:167 `summary|about|bio`). The X bio field face exists only in XF (`XF/src/x_first/grok_profile_hydration.py:96-106`: `platform_user_id, display_name, bio, location, external_urls, professional_category, affiliations, verification, org_affiliation_signals`, all pinned `SOURCE_STATUS = "model_mediated_unverified"` at `:95`) and in the live chain's `x_bio` prompt key (`XF/src/x_first/luna_batch_runner.py:635-637`).

### 2.2 "Execute once is enough" has **no per-person marker** today

| Mechanism | Anchor | Key | Enough? |
|---|---|---|---|
| batch idempotency key | storage.py:12150-12164 (`"target-candidate-public-web-batch:" + sha1(record_ids ∥ source_families ∥ options ∥ force_refresh)`) | (request set) | ❌ batch-level |
| run idempotency key | public_web_runtime_core.py:1727/:1743/:1778/:1796 | (record × batch) | ❌ |
| terminal-run reuse | crm_public_web_owner.py:6181-6188 (`reused_terminal_run_count`) | (record × batch) | ⚠️ closest, still not (person × source) |
| force escape hatch | crm_public_web_owner.py:1225-1240; operation_runtime.py:793-799 | force+nonce | ✅ **the escape-hatch shape is already right — reuse it verbatim** |
| fetch-queue cap | public_web_search.py:113-122 (`x_url: 1`) | per run | ❌ budget, not idempotency |
| XF "exactly once" | `GROK_COMPACT_...md:203-205/:245-248` | per **handle**, per attempt, receipt-enforced (violation voids the whole batch) | ❌ **different semantic** — an *execution* contract inside one run, not "this person is done forever" |

**This is the gap directive #5 names.** Nothing in either package records "person P's X account is located; do not pay to locate it again."

### 2.3 Tier ② today (SAA): direction is a hard-coded Python tuple with zero governance

Measured on this tree (imported `query_signal_knowledge` read-only, 2026-07-24):
- `KNOWN_SCOPE_SIGNAL_SPECS` (query_signal_knowledge.py:20-211) — **10** entries: `Google DeepMind, Gemini, Veo, Nano Banana, Google Research, Brain Team, ChatGPT, Health, Claude, o1`.
- `KNOWN_THEMATIC_SIGNAL_SPECS` (`:214-336`) — **17** entries: `Coding, Agent, Math, Text, Audio, Infra, Vision, Vision-language, Multimodal, Reasoning, RL, Eval, Pre-train (:302), Post-train (:309), World model, Alignment, Safety`.
- Request-side axes (request_normalization.py:71-84): 12 keyword fields including `research_direction_keywords`, `organization_keywords`, `team_keywords`, `sub_org_keywords`, `model_keywords`, `product_keywords` — **1:1 with the operator's four examples**.
- Judgment: `_derive_thematic_signal_facets` (domain.py:1068-1082) — pure alias match → facet slugs. **No evidence refs, no temporality, no confidence, no abstain state.**
- Governance metadata present on those 27 rows: **NONE** (no `status`, `last_verified_at`, `refresh_after`, `source_status`, `evidence_refs`; no schema, no version, no content hash, no coverage assertion, no owner-matrix row). Maintenance = edit the tuple.

**Measured good news (this is why promotion is cheap):** running XF's own alias rules M5/M6 over both SAA tables today yields **zero unsafe aliases and zero cross-label alias collisions**. The content is already catalog-clean; only the *governance envelope* is missing.

### 2.4 The live chain (Track C) — what it proves and what it lacks

Registered in `scripts/README.md:20-23`: `live_xfirst_seed_build.py` → `live_grok_collection_run.py` → `live_luna_judge_run.py` → `live_xfirst_export_csv.py`. It has really run (TML/OpenAI/GDM batches) and produced the 13-column CSV (`live_xfirst_export_csv.py:29-40`: 8 CRM columns + `X账号, X账号已确认, Pre-train方向经历, 判断置信度, 证据摘要`).

Proven disciplines worth keeping: identity-anchored locate prompt (name + current lab + past labs, `luna_batch_runner.py:604-606`), `not_found` discipline with negative-search receipts and "never invent or guess a handle" (`:631-634`), citation whitelist fail-closed (`seed_fact:<ref>` / `x_bio` / `post:<id>`, violation → whole review voided, [HARVESTAPI_PLAYBOOK.md](HARVESTAPI_PLAYBOOK.md):586), abstain semantics (thin bundle → `ambiguous`, rich-but-silent → `unsupported`), trust never upgrades (`authorized_transition_count` stays 0), and a deterministic offline exporter that emits "待采集/待判断" placeholders.

Missing: PG records, a typed command owner, a recovery phase, an idempotency key, a causal event spine, a parameterized direction, and the fail-closed provider triple-gate seam (§9.1).

### 2.5 Recorded discrepancies (recon vs. code — follow the code)

- **D-B1** — the recon summary says "12 条 scope" for `KNOWN_SCOPE_SIGNAL_SPECS`; the measured count is **10** (its own §3.1 note already said 10). Thematic 17 confirmed. The design uses 10 + 17 = 27 rows.
- **D-B2** — `_derive_thematic_signal_facets` is at **domain.py:1068** (`def`), body `:1068-1082`; the recon's `:1072` points inside the body.
- **D-B3** — the recon states `force_refresh` "must be paired with `refresh_nonce`". True on the **company** public-web action path (operation_runtime.py:793-799 raises `company_public_web_refresh_nonce_required` and the reverse `..._requires_force_refresh`), but the **CRM candidate** path *auto-mints* a nonce when force is set without one (crm_public_web_owner.py:1229-1230). Tier 1's escape hatch must state which discipline it adopts (§4.3 chooses the strict one; the auto-mint stays for the legacy CRM caller).
- **D-B4** — `XF/AGENTS.md:29` forbids direct runtime imports; `scripts/live_grok_collection_run.py:65-66` performs one (SAA→XF, in the script lane, with the operator's absolute path hard-coded at `:63`). Meanwhile the sanctioned adapter modules (`x_first_portable_adapter.py`, `x_first_portable_package.py`, `x_first_simulate_owner.py`) have **zero production callers**. OQ9 rules on this.
- **D-B5** — the person-asset contract enumerates 6 `assertion_type` values ([PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md](PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md):133) but the producer emits **10+**: `primary_email, homepage_url, github_url, x_url, substack_url, scholar_url, linkedin_url, profile_link_url` (crm_public_web_owner.py:831-845), and the store takes `assertion_type` as a free string (storage.py:4010/:4248). The doc's list is stale and **the vocabulary is unenforced**. Tier 1 keys its gate on this type, so S0 must pin the real vocabulary (§8) — and note that `scholar_url` **already exists**, which is why the Semantic Scholar binding (§3.5) needs no new assertion type.
- **D-B6** — the live judge's output keys are hard-coded pretraining/lab (`luna_batch_runner.py:132-137`) while `XF/docs/LUNA_LIVE_BATCH_RUNNER_DESIGN.md:246` already specifies replacing the CSV's ad-hoc boolean with `dimension_result.relevance_state` + `target_activity_temporal_state`. **Designed, not implemented.** OQ2 makes that generalization this proposal's job.
- **D-B7** — `enrichment.py:10444` already materializes `"research_direction": list(publication.topics[:8])` with **no normalization bridge** to either the 17 thematic labels or any XF taxonomy. The Semantic Scholar binding must close this, not add a third vocabulary.

---

## 3. Contract: one methodology, instantiated per source

### 3.1 The extension point — `sourcing.behavior_layer.source_binding.v1` (a registry, not a heuristic)

One row per source. **Adding Semantic Scholar is adding a row**; the locate/collect/judge machinery is source-agnostic. Versioned dotted-contract id per repo convention (`sourcing.profile_prefetch.ai_batch_division.v1`, `sourcing.organization_asset.ai_promote_decision.v1`, `sourcing.pipeline.compensation_intent.v1`).

```jsonc
{
  "schema_id": "sourcing.behavior_layer.source_binding.v1",
  "source_kind": "x_account",                 // registry key; "scholar_profile" is the next row (§3.5)
  "locate": {
    "planner": "public_web_search.plan_candidate_public_web_queries",  // an EXISTING planner symbol
    "source_family": "social_presence",       // an EXISTING source family (public_web_search.py:26-34)
    "identity_anchor": ["display_name", "current_organization", "prior_organizations"],  // luna_batch_runner.py:604-606
    "not_found_is_a_result": true,            // a negative locate is durable (§4.2) — never silently re-paid
    "provider_binding": null                  // null = rides an EXISTING owner; non-null = a NEW paid dispatch path (R-019, §9.2)
  },
  "collect": {
    "authorized_surface": ["profile_bio", "authored_post", "authored_reply"],
    "per_account_budget": "one_authorized_surface_pass",   // GENERALIZED_...md:64-67
    "citation_prefixes": ["x_bio", "post:", "seed_fact:"]  // HARVESTAPI_PLAYBOOK.md:586 whitelist, fail-closed
  },
  "judge": {
    "supported_dimension_ids": ["research_workstream", "industry_domain"],
    "supported_scope_kinds": ["organization", "team", "model", "product", "research_program", "capability"]
  },
  "trust_ceiling": "model_mediated_unverified",   // "source_bound" only for payload-returning transports
  "assertion_type": "x_url",                      // the person-asset home of tier 1's value (§2.1, D-B5)
  "once_only": { "enabled": true, "refresh_after_days": 180, "invalidated_by": ["assertion_revoked", "identity_anchor_changed", "source_identity_superseded", "operator_force_refresh"] }
}
```

Design rules: (1) **`provider_binding: null` is the default and the only R-019-safe shape** — the locate rides an existing command owner. (2) `trust_ceiling` is a **ceiling, never a floor**: no locate result or judgment may declare a `source_status` above it, and nothing upgrades it (`authorized_transition_count` stays 0). (3) `citation_prefixes` is a closed allowlist; any other citation voids the judgment (§7 V_SOURCE). (4) The binding names **existing** symbols; a binding that requires new machinery is a new slice, not a registry edit.

### 3.2 Tier ① contract — `sourcing.behavior_layer.locate_result.v1`

```jsonc
{
  "schema_id": "sourcing.behavior_layer.locate_result.v1",
  "locate_id": "<ULID>",
  "person_identity_key": "…",                  // canonical SAA person — never an X handle (XF/AGENTS.md:24)
  "source_kind": "x_account",
  "outcome": "located",                        // "located" | "not_found" | "blocked" | "error"
  "identity_anchor_sha256": "…",               // hash of the binding's identity_anchor field values AT LOCATE TIME (§4.3)
  "located_value": { "canonical_url": "https://x.com/…", "platform_user_id": "…", "handle": "…" },
  "basic_info": {                              // the binding's declared field face; X mirrors grok_profile_hydration.py:96-106
    "display_name": "…", "bio": "…", "location": "…", "external_urls": ["…"],
    "professional_category": "…", "affiliations": ["…"], "verification": "…"
  },
  "source_status": "model_mediated_unverified", // ≤ binding.trust_ceiling — enforced, never self-declared upward
  "evidence_receipts": [ { "receipt_kind": "…", "receipt_ref": "…" } ],   // incl. NEGATIVE-search receipts on not_found
  "located_at": "…", "valid_until": "…",       // self-consistent window (XF M3): located_at < valid_until
  "provenance": { "executed_by_owner": "crm_public_web_owner", "run_id": "…", "input_snapshot_sha256": "…" }
}
```

`basic_info` is **observation only**. Promotion of `located_value` into a person-asset assertion follows the existing publishable/adjudication gates (`_validate_public_web_signal_promotable`, crm_public_web_owner.py:848-861) unchanged. **Tier 1 adds no new promotion authority.**

### 3.3 Tier ② contract — `sourcing.behavior_layer.direction_judgment.v1`

```jsonc
{
  "schema_id": "sourcing.behavior_layer.direction_judgment.v1",
  "judgment_id": "<ULID>",
  "person_identity_key": "…",
  "direction_ref": {                           // §3.4 — a POINTER INTO THE CATALOG, never a free string
    "catalog_id": "…", "catalog_sha256": "…",  // pins the exact table version that was judged against (XF M1)
    "dimension_id": "research_workstream", "label_id": "pretraining",
    "scope_id": null                           // XOR: a scope-node direction (organization/team/model/product) uses this
  },
  "source_kind": "x_account",
  "evidence_snapshot_sha256": "…",             // what the judge actually saw
  "relevance_state": "target_core",            // target_core | target_adjacent | ambiguous | out_of_scope
  "affiliation_state": "current",              // current | historical | ambiguous | unsupported   ┐ two INDEPENDENT axes
  "target_activity_state": "historical",       // current | historical | ambiguous | unsupported   ┘ (GENERALIZED_...md:119-127)
  "evidence_refs": ["seed_fact:exp_3", "post:1234", "x_bio"],   // every ref matches binding.citation_prefixes
  "reason": "…",                               // ≤240 chars free text
  "reason_code": "…",                          // controlled vocabulary
  "source_status": "model_mediated_unverified",
  "authority": "diagnostic_only_unattested",   // CONSTANT — LUNA_LIVE_BATCH_RUNNER_DESIGN.md:211-218
  "provenance": { "model_provider": "…", "requested_model": "…", "response_model": "…",
                  "prompt_sha256": "…", "input_snapshot_sha256": "…",
                  "usage": {"input_tokens": 0, "output_tokens": 0}, "latency_ms": 0 },
  "validator_results": [ {"validator": "V_CATALOG_fresh", "status": "pass"}, … ],
  "fallback": null                             // or the §6 abstain audit
}
```

Wire-shape rule (mirrors 议案① D7 / 议案③ §2.2): **the model authors ONLY** `{relevance_state, affiliation_state, target_activity_state, evidence_refs, reason, reason_code}`. `judgment_id`, every `*_sha256`, `direction_ref`, `source_status`, `authority`, provenance and validator results are caller-authored fact. There is **no schema slot for a new label, a new scope node, a trust upgrade, or a protected attribute** — those are structurally un-authorable, which is how M10 and `AGENTS.md:18-19` become mechanical rather than aspirational.

### 3.4 What a "direction" IS as a contract object

**A direction is a resolvable pointer into a content-addressed, freshness-gated catalog** — `(catalog_id, catalog_sha256, dimension_id+label_id | scope_id)` — resolved through XF's own `_resolve_fresh_scopes` discipline (`research_orchestration.py:528-592`). It is **never** a free string. This single decision retires three recorded gaps at once:
- the free-string `target_direction` (`luna_batch_runner.py:600`) → a catalog pointer;
- the hard-coded pretraining/lab judged keys (`:132-137`) → a `dimension_id`-parameterized result (D-B6);
- publication `topics[:8]` masquerading as `research_direction` (enrichment.py:10444) → labels resolved through the catalog alias index (D-B7).

**Ownership split (the OQ1 recommendation), stated as a Contract Field Ownership matrix:**

| Concern | Owner | Source of truth | Consumer | Deletion condition for the old source |
|---|---|---|---|---|
| Catalog **shape** + validation rules M1–M8 | X-First | `XF/contracts/x.research_scope.catalog.v1.schema.json` + `research_orchestration.py:175-253/:528-592` | SAA producer + SAA judge | n/a (already canonical) |
| Catalog **content** (which directions exist, aliases, freshness, coverage) | **SAA** (becomes the missing producer, recon G-2) | a generated `x.research_scope.catalog.v1` artifact | XF validator (validation), SAA tier 2 (judgment), SAA recall | `query_signal_knowledge.py`'s alias tuples are **generated from / checked against** the catalog once the producer lands; the tuples are retired as an *independent* source only when the recall path reads the catalog (a later slice, not this one) |
| Direction **judgment** for a person | SAA tier 2 | `sourcing.behavior_layer.direction_judgment.v1` | person asset / CRM export | `_derive_thematic_signal_facets` (domain.py:1068) demotes to **recall/facet hinting only**; it never again populates a direction *claim* |
| Direction **recall** (alias → query text) | SAA | catalog `labels[].aliases` (M6-safe by construction) | query planners | duplicate alias lists in `query_signal_knowledge.py` |

Anti-drift mechanism: the producer emits the catalog **from** the promoted content and the XF validator is run over the output; a literal alias-face comparison between catalog and `query_signal_knowledge` is a test, not a convention. Measured today (§2.3): the 27 rows already satisfy M5/M6 with zero violations, so the first producer run is a governance-metadata addition, not a content migration.

**Honest scope cut:** the catalog's `taxonomies` and `scope_nodes` are orthogonal but **unbound** — a scope node does not declare its dimensions/labels and coverage asserts only over `scope_kinds` (recon G-11). This design does not fix that; it consumes both legs independently (a `direction_ref` is a label XOR a scope id) and records the gap. Binding them is an XF-side contract change and belongs to XF's owner.

### 3.5 Worked example: adding Semantic Scholar is ONE registry row

```jsonc
{
  "schema_id": "sourcing.behavior_layer.source_binding.v1",
  "source_kind": "scholar_profile",
  "locate": {
    "planner": "public_web_search.plan_candidate_public_web_queries",   // scholar_profile_discovery family (public_web_search.py:31) + site:scholar.google.com/citations (:521-525)
    "source_family": "scholar_profile_discovery",
    "identity_anchor": ["display_name", "current_organization", "coauthor_roster_anchor"],
    "not_found_is_a_result": true,
    "provider_binding": null                    // Phase 1 rides public-web; the dedicated S2 connector is Phase 2 (LEAD_DISCOVERY_METHODS.md:99-100)
  },
  "collect": { "authorized_surface": ["publication_record"], "per_account_budget": "author_feed_pass",
               "citation_prefixes": ["paper:", "seed_fact:"] },
  "judge": { "supported_dimension_ids": ["research_workstream", "industry_domain"], "supported_scope_kinds": ["research_program", "capability"] },
  "trust_ceiling": "source_bound",              // API-returned publication metadata is source-bound; a model SUMMARY of it is not
  "assertion_type": "scholar_url",              // ALREADY produced (crm_public_web_owner.py:838-839) — no new type
  "once_only": { "enabled": true, "refresh_after_days": 365,
                 "invalidated_by": ["assertion_revoked", "identity_anchor_changed", "operator_force_refresh"] }
}
```

Everything it needs already exists on the SAA side: `semanticscholar.org` is whitelisted in two places (public_web_search.py:159-160, document_extraction.py:60); `scholar_url` is a first-class entry-link type (public_web_search.py:105) and `scholar_profile_discovery` a first-class source family (`:31`); the arXiv author feed and publication search are production paths (enrichment.py:9851/:10236); `PublicationRecord.topics` exists (`:2389-2400`). The two **genuinely new** pieces are (a) the dedicated connector [LEAD_DISCOVERY_METHODS.md](LEAD_DISCOVERY_METHODS.md):99-100 lists as "当前仍未接入", and (b) the `topics → label_id` resolution through the catalog alias index (D-B7). Both are contained; neither touches the locate/collect/judge machinery. The safety rules of that method stay untouched: coauthor ≠ membership, prospects only, second independent evidence chain required, `scholar_coauthor_follow_up_limit` default 0 ([LEAD_DISCOVERY_METHODS.md](LEAD_DISCOVERY_METHODS.md):57-88; enrichment.py:4947).

---

## 4. Tier ① once-only semantics — what makes 一次足够 durable

### 4.1 The gate record and its key

A durable row per `(person_identity_key, source_kind)` — the **execution gate**. Not a second copy of the value:

| Field | Meaning |
|---|---|
| `person_identity_key` × `source_kind` | the key. This is the level the directive names, and the level nothing has today (§2.2) |
| `outcome` | `located | not_found | blocked | error` — **the negative is durable too** |
| `identity_anchor_sha256` | hash of the binding's `identity_anchor` values at locate time |
| `located_at` / `valid_until` | XF M3-shaped self-consistent window |
| `assertion_id` | non-null iff `outcome == located`; points at the person-asset assertion that holds the VALUE |
| `attempt_count` / `last_error_class` | bounded escalation (§6 F7) |
| `invalidated_at` / `invalidated_by` | audit of why the gate reopened |

**Ownership split, explicit (Contract Field Ownership rule 4 — do not derive one field from another implicitly):** the **value** authority is the person-asset assertion (`x_url`/`scholar_url`, with `authority` + `verification_status` + `valid_from/valid_to`); the **execution** authority is the gate. The one documented derivation: `outcome == "located"` REQUIRES a live assertion of `binding.assertion_type` for that person; if that assertion goes `rejected` / `superseded` / `needs_review`, the gate is invalidated. Nothing else is derived across the two.

### 4.2 Why the negative must be durable

`not_found` is the expensive outcome: the locate prompt's discipline (`luna_batch_runner.py:631-634`) requires exhaustive negative-search receipts before returning it. Without a durable negative, every re-run re-pays for the same absence — the exact "never re-pay" violation. So `not_found` is a **first-class stored outcome** with its own (shorter) freshness window, and its receipts are retained as the evidence that the absence was actually established.

### 4.3 What invalidates the gate (the complete list — nothing else reopens it)

1. **TTL expiry** — `as_of ≥ valid_until` (binding `refresh_after_days`). Mirrors XF M8's `last_verified_at ≤ as_of < refresh_after` exactly.
2. **Assertion revoked** — the backing assertion moves to `rejected` / `superseded` / `needs_review`.
3. **Identity anchor changed** — `identity_anchor_sha256` differs from a recomputation over current person facts (e.g. the person changed labs, so the name+lab anchor that produced a `not_found` is stale). This is what makes re-locate *correct* rather than merely permitted.
4. **Source identity superseded** — the platform user id behind a located handle no longer matches (`XF/AGENTS.md:24`: identity is the stable platform user id + handle history, not the handle string).
5. **Operator force refresh** — `force_refresh=true` **AND** a caller-supplied `refresh_nonce`, adopting the **strict** discipline of operation_runtime.py:793-799 (both directions validated: force without nonce refuses, nonce without force refuses). Per D-B3 the CRM path's auto-mint is legacy and is **not** extended to the behavior layer.

Nothing else — in particular, a new batch, a new run, a re-tick, a recovery replay, or a different requesting operation does **not** reopen the gate. **Freshness is the gate's decision, never recovery's** (OQ4): recovery re-drives an *interrupted* locate (same gate row, same idempotency key) and never re-decides whether a settled locate should run again.

### 4.4 Where the gate sits — and why it needs no new dispatch path

For `x_account` with `provider_binding: null`, the gate is a **pre-filter on the existing public-web owner**: at batch normalization the requested record set is intersected with live gate rows, and already-`located`/`not_found`-fresh persons are dropped from the dispatch set before any query is planned. Structurally this is the same move as the existing terminal-run reuse (crm_public_web_owner.py:6181-6188) lifted from (record × batch) to (person × source_kind). **Zero new dispatch path, zero new `update_*_state` call site, zero new `_connect_with_transaction_lock` caller → the R-019 ceiling of 24 is untouched** (议案④ OQ5 discipline applied verbatim).

A Grok-backed binding (`provider_binding != null`) is the opposite: a genuinely new paid dispatch path. It is therefore **out of the landable scope** until R-019 is remediated AND the operator explicitly approves (§9.2, OQ5, slice S9).

---

## 5. Model invocation surface (tier ② only)

### 5.1 Provider path — follow the `ModelClient` conventions (as 议案①/③ did)

- Add **one** Protocol method `judge_candidate_direction_relevance(self, payload: dict[str, Any]) -> dict[str, Any]` to `ModelClient` (model_provider.py:705; the Protocol currently carries **19** methods, pinned by `tests/test_model_client_v1_characterization.py:1308` — this bumps the golden 19→20, exactly as 议案① bumped 17→18 and 议案③ 18→19).
- `DeterministicModelClient` (`:745`) returns `{}` — the structural "judge unavailable" marker → F1 abstain. `OfflineModelClient` (`:1007`) inherits it, so simulate/replay abstain by construction (the 议案① D2 property).
- `OpenAICompatibleChatModelClient` (`:1708`) implements it like its siblings (`_safe_text_prompt_with_error` + `_safe_json_object` + the shared per-(provider, base_url, model) circuit). `QwenResponsesModelClient` (`:1277`) gets the method but **has no circuit machinery** (议案① D6) → any Qwen failure is F3, never F2.
- Scripted offline client `ScriptedCandidateDirectionJudgeModelClient` behind a dedicated env `SOURCING_SCRIPTED_CANDIDATE_DIRECTION_JUDGE`, wired into `build_model_client`'s offline branch with the established non-composable precedence (planning > divider > promote judge > direction judge, model_provider.py:2892-2908).
- Live billing clears the same fail-closed triple-gate as every other provider: `assert_live_provider_access_allowed` (model_provider.py:2915). **This is the seam Track C does not clear today** (§9.1).

### 5.2 Inputs, parsing, seat

- **Payload**: the direction_ref's resolved label/scope object (display name + aliases from the catalog), the collected evidence bundle for that (person × source) with every item carrying its citation ref and `source_status`, plus the person's professional-facts envelope where the binding authorizes it (the operator-mandated full-LinkedIn-profile input, `live_xfirst_seed_build.py:11-22` / [HARVESTAPI_PLAYBOOK.md](HARVESTAPI_PLAYBOOK.md):583-588). Hashed into `evidence_snapshot_sha256` + `input_snapshot_sha256`.
- **Payload carries no protected-attribute signal and no free-form direction string.** The direction is the resolved catalog object; the model cannot rename it.
- **Parse**: strict manual validation, key allowlist, enum membership on all three state axes, `evidence_refs` prefix allowlist, `reason` non-empty ≤240 chars. **No partial acceptance, no repair** — a repaired judgment would launder an unauditable model error into a person-asset direction claim.
- **Seat / cadence**: one call per `(person × direction_ref × source_kind × evidence_snapshot_sha256)`. Re-judging an unchanged bundle for the same direction is a cache hit, not a call — this is tier 2's own "on-demand" idempotency, and it is why the same person can be judged for four different directions without four collections. Not a hot serving path; timeout 20 s (a judge-specific override of `ModelProviderSettings.timeout_seconds` default 45 s, settings.py:29), mirroring both prior 议案.

---

## 6. Failure / fallback semantics — abstain, never invent

Every failure class → **`relevance_state = "ambiguous"`, both temporal axes `unsupported`, empty `evidence_refs`, a recorded audit, and NO write to any direction field.** This follows 议案③ ruling ④'s conservative direction, and **consciously does NOT follow** 议案①'s rule-ladder fallback.

**Why the existing alias matcher is not a legal fallback:** `_derive_thematic_signal_facets` (domain.py:1068) produces facet slugs with no evidence refs, no temporality, no confidence and no abstain state (recon G-10). Falling back to it would silently relabel a rule-grade recall hint as a judgment-grade direction claim — the exact laundering 议案③ ruling ④ exists to prevent. The matcher survives, unchanged, as a **recall/facet hint**; it never sources a direction claim.

| Class | Trigger | Detection | `fallback_reason` |
|---|---|---|---|
| F1 judge unavailable | deterministic/offline client, no client injected | structural `{}` return | `direction_judge_unavailable` |
| F2 circuit open | prior failure within the 900 s cooldown (OpenAI-compatible only, D6) | `_model_provider_circuit_error` | `direction_judge_circuit_open` |
| F3 timeout / transport | >20 s, HTTP/URL error, any Qwen failure | `_safe_text_prompt_with_error` | `direction_judge_call_failed` |
| F4 invalid output | parse failure, schema violation, state ∉ enum, empty reason | §5.2 strict validation | `direction_judge_invalid_output` |
| F5 validator rejection | any §7 validator FAIL | §7 battery | `direction_judge_validator_rejected:<validator>` |
| F6 stale evidence | the bundle changed between snapshot and apply | `evidence_snapshot_sha256` mismatch | `direction_judge_evidence_stale` |
| F7 locate escalation (tier 1) | gate `attempt_count` reaches its ceiling with `outcome ∈ {blocked, error}` | gate ladder | gate terminalizes `locate_exhausted_needs_human` — board-visible, **never a silent re-loop, never fail-open** (议案④ §4 shape) |

---

## 7. Validators-from-rules — the hard prerequisites the AI cannot bypass

Each is a deterministic gate derived from an existing rule; **the AI runs only inside what they already admitted, and can only be more conservative.** Any FAIL → F5 abstain.

| # | Validator | Retained rule | Anchor |
|---|---|---|---|
| V_CATALOG | `direction_ref` resolves in a catalog whose `generated_at ≤ as_of`, whose node/label is fresh (`last_verified_at ≤ as_of < refresh_after`), `status != unknown`, `source_status != model_mediated_unverified`; `catalog_sha256` matches | XF M8 fail-closed freshness | `XF/research_orchestration.py:528-592` |
| V_SOURCE | every `evidence_refs` entry matches a `binding.citation_prefixes` prefix **and** resolves to an actually-collected artifact; any other ref voids the judgment | citation whitelist, `luna_output_citation_not_judged` | [HARVESTAPI_PLAYBOOK.md](HARVESTAPI_PLAYBOOK.md):586 |
| V_TRUST | declared `source_status` ≤ `binding.trust_ceiling`; `authority` is the constant `diagnostic_only_unattested`; no transition is authorized | trust never upgrades | `XF/docs/LUNA_LIVE_BATCH_RUNNER_DESIGN.md:211-218` |
| V_ABSTAIN | thin/empty bundle ⟹ `ambiguous`; rich-but-silent bundle ⟹ `unsupported`; a positive state with zero evidence refs is impossible | abstain semantics | `LUNA_LIVE_BATCH_RUNNER_DESIGN.md:88-92` |
| V_TEMPORAL | the three state axes are independent; a judgment deriving activity state from affiliation state (or vice versa) fails | two independent temporal axes | `GENERALIZED_...md:119-127`; `XF/AGENTS.md:9-17` (`historical` is not a negative signal) |
| V_IDENTITY | no protected-attribute inference: the payload carries no such signal and the schema has no slot; the optional China/Asia experience axis stays **off** unless explicitly requested and is recorded as a professional/educational-experience question with `identity_inference_performed=false` | `XF/AGENTS.md:18-19`; `research_orchestration.py:501-509` | — |
| V_SCOPE | a model-proposed new direction is inexpressible (no schema slot); discovery lands in `exploratory_findings` for human review and never rewrites the catalog | XF M10 | `GENERALIZED_...md:69-71/:108-110` |
| V_GATE (tier 1) | a locate dispatch is admitted only when no live gate row covers `(person, source_kind)`, or an §4.3 invalidation fired | never re-pay | §4.3/§9.2 |

---

## 8. Characterization prerequisite — **S0 is mandatory**

**Finding: no oracle exists for either tier, and the tier-2 surface is materially less pinned than 议案③'s promote surface was.** Measured on this tree:

- `tests/test_query_signal_knowledge.py` is **80 lines / 6 spot-check tests** over a 27-row table (§2.3). No whole-table golden, no alias-face pin, no cross-label ambiguity pin, no facet-derivation golden.
- `_derive_thematic_signal_facets` (domain.py:1068) — the function that produces every direction facet today — has **zero test references anywhere in `tests/`** (grep over `tests/` + `src/`: the only hits are its definition and its single call site, domain.py:1007).
- The tier-1 chain has broad behavioral coverage (`test_public_web_search.py` 2,213 lines, `test_target_candidate_public_web.py`, `test_public_web_quality.py`, `test_person_asset_crm_projection_contracts.py` 1,876 lines) but **no end-shape pin of "public-web x_url discovery → adjudication → assertion"** as one contract, which is precisely what the gate must prove it does not perturb.
- The `assertion_type` vocabulary is unenforced and its doc list is stale (D-B5).
- XF-side: `XF/tests/test_research_orchestration.py` pins the catalog validator (M1–M8) — that suite **is** the maintenance-method oracle and stays authoritative; but the 9-kind enum's three hard-coded copies have no literal-comparison test (recon G-1).

**S0 (before any promotion of the direction tuples, before any gate):**
1. **Direction-registry whole-table golden** — the 10 scope + 17 thematic rows as shape-exact goldens (canonical labels, alias faces, keyword/facet projections), plus a machine check of XF's M5/M6 rules over them (measured clean today, §2.3 — pin it so promotion cannot silently break recall).
2. **Facet-derivation goldens** — `_derive_thematic_signal_facets` input→output grid including the `canonical_label`-to-slug mechanical degradation path (domain.py:1078-1081) and the more-specific-alias-wins disambiguation (`query_signal_knowledge.py:750-782`).
3. **Tier-1 chain end-shape pin** — one PG-backed golden of the x_url path from discovery through adjudication to the emitted assertion, including the real `assertion_type` vocabulary (D-B5) and the terminal-reuse behavior the gate generalizes.
4. **(Scope question, OQ7)** the XF triple-enum literal pin (G-1) — an XF-side test in a package with different rules.

Rationale, stated as the 议案③ precedent: promote had no oracle, so S0 was forced; here the *judgment* surface is even less pinned (zero tests) **and** the promotion of ungoverned tuples into a governed catalog changes the recall surface, not just the judgment surface. **Answer: yes, an oracle must be pinned first — items 1–3 are hard prerequisites; item 4 is an operator scope call.**

---

## 9. Paid / provider safety (NON-NEGOTIABLE)

### 9.1 The honest finding: Track C does not clear the triple-gate seam

`scripts/live_grok_collection_run.py` and `scripts/live_luna_judge_run.py` contain **zero references** to `SOURCING_EXTERNAL_PROVIDER_MODE`, `external_provider_mode()` or `assert_live_provider_access_allowed`. They drive the local `grok` CLI via subprocess (`GROK_HOME = ~/.grok`, `cwd` pinned at `:25/:40`) and an XF-side DeepSeek transport. This is **not** a violation of the CLAUDE.md rule (nothing sets the live env; these are committed operator scripts in the sanctioned `scripts/live_*.py` lane), but it **is** a gap in the fail-closed seam: an external, billable provider is reachable without passing the gate every other provider passes. **Productizing tier 1 or tier 2 therefore requires routing the provider through a typed binding that clears `assert_live_provider_access_allowed` (model_provider.py:2915)** — recorded as a prerequisite of slice S9, not papered over.

### 9.2 How a re-run cannot re-pay for an already-located account

1. **The gate is upstream of dispatch.** For `x_account`/`scholar_profile` (`provider_binding: null`) the gate drops already-settled persons from the dispatch set *before* query planning — no plan, no fetch, no model call, no charge (§4.4).
2. **Delta-only by construction.** The gate intersection **is** the "inventory local history first, delta-only" step: a batch of 500 persons where 480 are fresh-`located` dispatches 20.
3. **The negative is durable too** — `not_found` cannot be re-paid inside its window (§4.2).
4. **The escape hatch is explicit and audited** — only force+nonce (strict, both-direction-validated) reopens a fresh gate, and the nonce lands in the audit trail (§4.3 rule 5).
5. **Two-layer enforcement, honestly distinguished.** XF's receipt discipline (exactly one `x_user_search` per handle; zero/missing/duplicate/extra calls void the entire batch, `GROK_COMPACT_...md:245-248`) enforces *within* one run. The gate enforces *across* runs. Neither substitutes for the other — recon D-3 recorded this conflation, and this design keeps them separate.
6. **No new paid dispatch path lands under this proposal.** Any binding with `provider_binding != null` is R-019-blocked and operator-gated (§0.2, OQ5).
7. **Live env, live schema, live script lane unchanged.** Triple-gate, committed `scripts/live_*.py`-only, and no mutation of `sourcing_live_tml_path_20260719`.

---

## 10. Simulate-first validation ladder

Every slice keeps S0 green; everything before the flips is additive and record-only.

1. **S0 — characterization** (§8 items 1–3). Offline pure functions + one PG-backed chain pin. Hard prerequisite.
2. **S1 — behavior-layer contract module.** The three schemas (§3.1/§3.2/§3.3) with strict fail-closed exact-version parsers and key allowlists at every level, the V_* battery as pure functions, the F1–F7 audit shapes, and the gate-decision pure function (`should_dispatch_locate(gate_row, binding, as_of, force, nonce) → admit | skip(reason) | invalidate(reason)`). Offline. ADDITIVE.
3. **S2 — catalog producer.** Emit an `x.research_scope.catalog.v1` artifact from the promoted 27-row content + governance metadata (status/freshness/source_status/evidence_refs/coverage), validated through XF's validator via the versioned adapter. Own review request (contract-field ownership + cross-package).
4. **S3 — tier-1 gate SHADOW.** Compute the gate decision at the public-web batch-normalization seam; record only, dispatch set untouched. Divergence counter: "persons the gate would have skipped" × "of those, how many the run re-located identically" — free before/after evidence that once-only is safe, and a direct measurement of today's re-pay rate.
5. **S4 — tier-2 model surface.** Protocol method (19→20), deterministic `{}`, provider impls, scripted client, orchestration helper. ADDITIVE.
6. **S5 — tier-2 SHADOW.** Judge already-collected bundles for a resolved `direction_ref`; record only; never writes a facet or a person-asset field. Divergence counter vs `_derive_thematic_signal_facets`.
7. **S6 — FLIP tier 1.** The gate becomes a real pre-filter on the existing owner (delta-only). Review **GO** required.
8. **S7 — FLIP tier 2.** The judgment becomes the source of the direction claim; `_derive_thematic_signal_facets` demotes to recall-only; the live CSV's `Pre-train方向经历` column generalizes to `relevance_state` + `target_activity_state` + evidence refs (D-B6, closing `LUNA_LIVE_BATCH_RUNNER_DESIGN.md:246`). Review **GO** required.
9. **S8 — Semantic Scholar binding** (§3.5) + the `topics → label_id` bridge (D-B7). Own review request.
10. **S9 — live** (Grok-backed tier 1 productization / any `provider_binding != null`): **BLOCKED** on R-019 remediation **AND** the §9.1 triple-gate routing **AND** explicit operator go **AND** quota. Red lines unchanged.

---

## 11. Operator questions (the AskUserQuestion batch)

Mapping against the recon's 18-question list: 议案① consumed Q1–Q7/Q16 and the divider half of Q18; 议案③ consumed Q8–Q12 and the promote half of Q18; 议案④ consumed Q17 and the compensation halves of Q14/Q18. **Q13, Q14 and Q15 were explicitly reserved for this proposal** ([WS7_AI_PROMOTE_DESIGN.md](WS7_AI_PROMOTE_DESIGN.md) §8 closing cross-reference). They map to OQ1, OQ3/OQ4 and OQ8 below; OQ2/OQ5/OQ6/OQ7/OQ9 are new questions this design surfaces.

- **OQ1 (= recon Q13) — direction taxonomy ownership + who is the catalog producer.** Adopt X-First's `x.research_scope.catalog.v1` (9 `scope_kind` + runtime taxonomy + M1–M8 maintenance/freshness rules) as the **single** direction contract, with **SAA becoming the catalog producer** (the role the owner matrix names but nobody fills today) and XF's validator remaining the single validation authority — rather than SAA building a parallel registry and reconciling via an adapter?
  **Recommended**: yes. XF owns shape + validation (M1–M8 have teeth and are already tested); SAA owns content (it is where the 27 governed-to-be rows and the request axes live) and consumes through the versioned artifact adapter. Two validators would drift; the 9-kind enum is already triple-hardcoded with no literal pin (§1.4).
  *Alternatives*: (b) SAA builds its own registry + an adapter mapping — doubles the maintenance method and re-opens every M1–M8 rule in a second place; (c) XF becomes the producer too — impossible under `XF/AGENTS.md:27` (it may not read/write SAA person data).

- **OQ2 (new) — a direction is a catalog pointer, not a string.** Adopt `direction_ref = (catalog_id, catalog_sha256, dimension_id+label_id XOR scope_id)` as the contract object everywhere, retiring the free-string `target_direction` (`luna_batch_runner.py:600`) and generalizing the hard-coded pretraining/lab judged keys (`:132-137`) into `dimension_id`-parameterized results (D-B6)?
  **Recommended**: yes — this single decision retires three recorded gaps (free string, hard-coded keys, `topics[:8]` as `research_direction`) and is the precondition for "on-demand, per-direction" execution at all.
  *Alternatives*: (b) keep a free string with a lookup at use time — no content addressing, so a judgment cannot state *which version* of the direction it judged against; (c) parameterize only the new lane and leave the live chain's keys — permanent two-vocabulary drift.

- **OQ3 (= recon Q14, placement half) — where the once-only marker lives.** A durable gate row keyed `(person_identity_key, source_kind)` carrying `outcome / identity_anchor_sha256 / valid_until / assertion_id / attempt ladder` (§4.1), with the **value** authority staying on the existing person-asset assertion (`x_url` / `scholar_url`) and exactly one documented derivation between them?
  **Recommended**: yes. The assertion already carries authority × verification_status × temporal validity — it is the right value home and needs no new table; but it can only record a *positive*, and a `not_found` must be durable too (§4.2), so the execution gate is a separate, thin record. Today nothing is keyed at (person × source): batch/run keys and terminal reuse are all coarser (§2.2).
  *Alternatives*: (b) marker on the assertion alone — negatives re-pay forever; (c) reuse the public-web batch idempotency key — batch-scoped, not person-scoped, and it changes with the request set; (d) a per-source column on the person record — an unversioned wide table that grows a column per source, defeating the registry.

- **OQ4 (= recon Q14, ownership half) — who decides a re-trigger.** Confirm the invalidation set is exactly the five triggers in §4.3 (TTL / assertion revoked / identity anchor changed / source identity superseded / operator force+nonce), that **freshness is the behavior layer's decision and never recovery's** (recovery only re-drives an interrupted locate under the same key), and that the force escape hatch adopts the **strict** both-direction nonce rule (operation_runtime.py:793-799) rather than the CRM path's auto-mint (D-B3)?
  **Recommended**: yes to all three. A recovery daemon that could re-decide freshness would become an unbounded re-pay source — exactly the 议案④ §4 failure mode.
  *Alternatives*: (b) let any new operation reopen the gate — restores today's re-pay behavior; (c) keep the CRM auto-mint for uniformity — an auto-minted nonce means a force refresh has no operator fingerprint in the audit trail.

- **OQ5 (new) — tier-1 execution seat + R-019.** Confirm tier 1 lands as a **pre-filter on the existing public-web command owner** (`provider_binding: null` — zero new dispatch path, zero new `update_*_state`/`_connect_with_transaction_lock` caller, R-019 ceiling stays 24), and that any Grok-backed binding is a **new paid dispatch path blocked until R-019 remediation + the §9.1 triple-gate routing + explicit operator go**?
  **Recommended**: yes — this is what makes the proposal landable now. The X locate capability already exists in the durable lane (§2.1); Grok adds *bio* and a stronger identity anchor, which are worth paying for later, not worth an R-019 collision now.
  *Alternatives*: (b) build the Grok locate runtime in this proposal — collides with R-019 head-on (议案④ OQ5 is a HARD rule) and couples directive #5 to an unlanded remediation; (c) leave tier 1 in the script lane permanently — the once-only gate then has no enforcement point and the directive is unmet.

- **OQ6 (new) — the tier-2 AI seat and its hard prerequisites.** Confirm the 议案③ shape: deterministic gates first (V_CATALOG / V_SOURCE / V_TRUST / V_GATE), the model judges only inside what they admitted, it authors only `{relevance_state, affiliation_state, target_activity_state, evidence_refs, reason, reason_code}`, and every failure class abstains to `ambiguous` + audit — explicitly **not** falling back to `_derive_thematic_signal_facets` (§6)?
  **Recommended**: yes. The alias matcher has no evidence, temporality or abstain state (recon G-10); using it as a fallback would relabel a recall hint as a judgment claim. Abstaining is always a safe state for a completeness concern.
  *Alternatives*: (b) fall back to the alias matcher (议案① ruling ④a shape) — **rejected**, and recorded as a conscious divergence exactly as 议案③ did; (c) block the record entirely on judge failure — loses the audit trail that says why nothing was claimed.

- **OQ7 (new) — S0 scope.** Accept that S0 is mandatory and covers (1) the 27-row direction-registry whole-table golden + XF M5/M6 machine check, (2) `_derive_thematic_signal_facets` goldens (currently **zero** tests), and (3) the tier-1 x_url→assertion chain end-shape pin incl. the real `assertion_type` vocabulary (D-B5)? Is (4) the XF-side triple-enum literal pin (recon G-1) in scope for this proposal or deferred to XF's owner?
  **Recommended**: (1)–(3) yes, hard prerequisite; (4) **defer to XF's owner** — a cross-package test written by the consumer is exactly the kind of ownership blur this design is trying to remove; raise it as an XF residual-ledger item instead.
  *Alternatives*: (b) proceed on the 6 existing spot-checks — the tuple→catalog promotion would silently change recall with no oracle to catch it; (c) include (4) — faster, but SAA would then own a pin over an XF contract.

- **OQ8 (= recon Q15) — how Semantic Scholar enters.** Land it as a **SAA behavior-layer source binding** (`scholar_profile`, §3.5) reusing the existing `scholar_url` assertion type / `scholar_profile_discovery` family / arXiv paths, rather than as a new X-First portable seed kind or evidence channel?
  **Recommended**: SAA binding. Publication evidence must ultimately become person-asset evidence, and `XF/AGENTS.md:27` forbids XF from writing PersonAsset/CRM. XF's `paper_conference_linkage` family stays in XF for the different job of resolving *papers → X accounts*. The two coexist without overlap.
  *Alternatives*: (b) a new XF portable seed kind — a scholar profile already fits the existing `professional_profile` kind, and anything more would be an XF contract bump for a consumer's benefit; (c) both — two ingestion paths for one source, guaranteed drift.

- **OQ9 (new) — the cross-package seam.** Make `x_first_portable_adapter.py` the **single** SAA↔XF seam (it has **zero production callers** today, only tests) and retire the direct runtime import in `scripts/live_grok_collection_run.py:65-66` (which contradicts `XF/AGENTS.md:29` and hard-codes an operator-local absolute path at `:63`) — or explicitly sanction the script-lane import as operator tooling that is out of the adapter's scope?
  **Recommended**: **retire it at S9** (when the live chain is productized), and until then explicitly sanction the script-lane import as operator tooling with a recorded removal condition (Contract Field Ownership rule 5: a temporary bridge must be report-visible with a removal condition). Do not retire it before S9 — that would break a working, operator-driven live lane for a doc-conformance win.
  *Alternatives*: (b) retire immediately — breaks the only lane that has produced real output; (c) leave it unrecorded — the boundary rule becomes aspirational.

---

## 12. Implementation slices + review-gate routing

Each slice is independently green; the §7-protocol applies (implement + targeted tests + pin commit + record review request, then continue; NO-GO freezes only the affected scope's promotion). **No slice lands before the OQ1–OQ9 ruling batch is answered.**

| Slice | Content | Gate/oracle state | Review gate |
|---|---|---|---|
| **S0** | **Characterization (hard prerequisite, §8)**: direction-registry whole-table golden (10+17 rows) + XF M5/M6 machine check; `_derive_thematic_signal_facets` goldens (zero tests today); tier-1 x_url→adjudication→assertion chain end-shape pin incl. the real `assertion_type` vocabulary. Offline pure functions + one PG-backed pin. | establishes the oracle | request recorded (trigger 1: pins a contract surface); blocks nothing before S2 |
| **S1** | Behavior-layer contract module: `source_binding.v1` / `locate_result.v1` / `direction_judgment.v1` strict fail-closed parsers (exact-version lookup, key allowlists at every level; **no schema slot** for a new label, a trust upgrade, a protected attribute, or a paid payload) + the V_* battery + F1–F7 audit shapes + the `should_dispatch_locate` gate function. Offline, ADDITIVE. | S0 untouched | request recorded (trigger 1: new contract schemas) |
| **S2** | **Catalog producer**: emit `x.research_scope.catalog.v1` from the promoted 27-row content + governance metadata; validate through the XF validator via the versioned adapter; alias-face single-sourcing check against `query_signal_knowledge`. | S0 green (the golden proves the alias face is unchanged) | **own review request** (trigger 1 + trigger 4: cross-package contract + new compatibility path) |
| **S3** | **Tier-1 gate SHADOW**: gate decision computed at the public-web batch-normalization seam, record-only; dispatch set byte-identical shadow on/off; divergence counter (would-skip count + re-located-identically count = the measured re-pay rate). | S0 byte-identical | rides S1 |
| **S4** | **Tier-2 model surface**: `ModelClient.judge_candidate_direction_relevance` (Protocol golden 19→20) + deterministic `{}` + OpenAI/Qwen impls (20 s judge timeout; D6 Qwen-no-circuit) + `ScriptedCandidateDirectionJudgeModelClient` behind `SOURCING_SCRIPTED_CANDIDATE_DIRECTION_JUDGE` + orchestration helper. ADDITIVE. | S0 untouched | rides S1 (trigger 3: model behavior) |
| **S5** | **Tier-2 SHADOW**: judge already-collected bundles for a resolved `direction_ref`; record-only; divergence counter vs the alias matcher. No person-asset write. | S0 byte-identical | rides S1/S4 |
| **S6** | **FLIP tier 1**: gate becomes a real pre-filter on the existing owner (delta-only, R-019 count stays 24). Simulate e2e gate on/off. | S0 green; gate-skip asserted as a no-op on outcomes | **GO required** (trigger 3 + trigger 5) + operator confirmation of OQ1–OQ9 |
| **S7** | **FLIP tier 2**: judgment sources the direction claim; `_derive_thematic_signal_facets` demoted to recall-only; live CSV column generalized to `relevance_state` + `target_activity_state` + evidence refs (D-B6). | S0 facet goldens demoted to recall-only assertions in-slice | **GO required** (trigger 2 + trigger 5: user-visible state wording + claimed feature) |
| **S8** | **Semantic Scholar binding** (§3.5) + `topics → label_id` bridge (D-B7) + the dedicated connector. | S0 untouched | **own review request** (trigger 1: new source contract) |
| **S9** | **BLOCKED / deferred** — Grok-backed tier 1 (`provider_binding != null`), the §9.1 triple-gate routing, and the OQ9 adapter-seam retirement. Requires R-019 remediation + operator go + quota. | n/a | n/a — dependency-gated |

Ordering: **S0 first (hard prerequisite)** → S1 → S2/S3/S4 in parallel → S5 → S6 → S7; S8 after S6; S9 indefinitely deferred.

---

*Design method: read-only over the current tree (public_web_search.py:470/:527-535/:113-122/:157-160, crm_public_web_owner.py:827-861/:1225-1240/:6181-6188, storage.py:12070-12092/:12150-12164/:4010, domain.py:1007/:1068, query_signal_knowledge.py:20-336 (imported and measured), request_normalization.py:71-84, enrichment.py:2389-2400/:9851/:10236/:10444/:4947, model_provider.py:705/:745/:1007/:1277/:1708/:2892-2915, settings.py:29, scripts/live_*.py + scripts/README.md:20-23; `XF/AGENTS.md`, `XF/contracts/x.research_scope.catalog.v1.schema.json:74-86`, `XF/configs/research_orchestration_policy.v1.json:54-67/:77-87`, `XF/src/x_first/research_orchestration.py:175-253/:501-509/:528-592`, `XF/src/x_first/luna_batch_runner.py:132-137/:596-604/:631-637`, `XF/src/x_first/grok_profile_hydration.py:95-106`, `XF/docs/GENERALIZED_RESEARCH_ORCHESTRATION.md`, `XF/docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md`, `XF/docs/LUNA_LIVE_BATCH_RUNNER_DESIGN.md`) + the 2026-07-24 behavior-layer recon + the three proven WS7 议案 as the shape. No src/tests changed; no live env; no PG mutation; no live-schema access. Anchors verified on branch `governance-phase0-ttl-20260611`.*
