# WS7/W7.3 — AI-Native materialize→promote Judgment Design (operator directive #3)

> Status: DESIGN DRAFT 2026-07-24 — **awaiting operator rulings on OQ1–OQ8 (§8) + independent review gate**. No implementation slice lands before the ruling batch is answered and the review verdict for the contract slices is recorded (contract-heavy → §9 protocol). Authority: operator ruling ③ + ④ RATIFIED 2026-07-22 ([REFACTOR_MASTER_PLAN.md](REFACTOR_MASTER_PLAN.md) §6.5, directive #3); factual base: [WS7_STRONG_AGENT_RECON_2026-07-22.md](WS7_STRONG_AGENT_RECON_2026-07-22.md) §2. Design shape mirrors the proven [WS7_AI_BATCH_DIVIDER_DESIGN.md](WS7_AI_BATCH_DIVIDER_DESIGN.md) (divider议案①, OQ1–OQ8 RATIFIED 2026-07-23).

```
status: design-draft    owner: operator (rulings pending)
canonical-path: sourcing-ai-agent/docs/WS7_AI_PROMOTE_DESIGN.md
drafted: 2026-07-24     oracle-pin: NONE YET — S0 characterization oracle is a prerequisite (§6)
```

Unprefixed `file:line` anchors refer to `sourcing-ai-agent/src/sourcing_agent/`; test anchors to `sourcing-ai-agent/tests/`. Anchors verified on this tree (branch `governance-phase0-ttl-20260611`).

---

## 0. Ratified constraints (transcribed; NOT up for redesign)

1. **Ruling ③ — two gates.** The storage lineage guard stays a **fail-closed HARD gate**; replay/lineage safety is NEVER delegated to AI. The AI judges promote/reject ONLY within candidates the guard has already passed. Prerequisite: **shard-recording completeness** (full request params + payload snapshot + `estimated_total` + lineage backfill), joined with the NEXT_TODO backfill残留.
2. **Ruling ④ — fail-closed promotion.** On AI unavailability/failure, promotion **fails closed**: do NOT promote, keep the incumbent authoritative snapshot. Asset correctness is conservative — unlike the divider (ruling ④ there falls back to the rule ladder because division is an *efficiency* concern), promotion is an *asset-correctness* concern and fails toward the incumbent.
3. **The seam is `evaluate_organization_asset_registry_promotion`.** Directive #3's "硬性规则 …历史上出过错误 promote" names the completeness threshold family in that function (§1.2). That family is what the AI judgment replaces; the storage lineage guard (§1.4) is what stays hard.

**Divider-vs-promote contrast (why this is not a copy of 议案①):**

| Axis | Divider (议案①) | Promote (this doc) |
|---|---|---|
| Concern | efficiency (batch shape) | asset correctness (which snapshot serves) |
| AI-unavailable fallback | rule ladder + audit (ruling ④a) | **keep incumbent, no promote** (ruling ④b) |
| Retained hard gate | provider envelope / wave invariants demote to *validators* | storage lineage guard stays a **non-demoted fail-closed gate** |
| Current oracle | whole-dict plan-record characterization existed (ruling-① prereq, `test_fetch_profile_batch_characterization.py`) | **NO characterization oracle exists** — S0 must build it (§6) |
| Record placement risk | additive key breaks the whole-dict plan oracle (D1) | no whole-dict registry-record oracle exists → additive audit key is safe (D1′, §2.4) |

---

## 1. The current hard-rule promote surface (enumerated from code)

Single decision path today: `evaluate_organization_asset_registry_promotion` (asset_reuse_planning.py:1217–1407), wired through `upsert_organization_asset_registry_with_guard` (asset_reuse_planning.py:1515–1548), with a second, independent fail-closed gate inside the store write (storage.py:8633–8695). Each rule below is annotated **[AI-REPLACE]** (the judgment the AI takes over) or **[STAYS HARD]** (a fail-closed gate the AI cannot weaken).

### 1.1 Deterministic pre-branches — **[STAYS RULE]** (not AI-judged; §8 OQ4)

Three branches are trivially correct and need no judgment; they stay deterministic and never invoke the model:
- lifecycle non-promotable → reject `lifecycle_state_not_promotable` (asset_reuse_planning.py:1224, via `organization_asset_lifecycle_promotable`).
- no incumbent → promote `no_existing_authoritative` (:1232).
- same-snapshot refresh → promote `same_snapshot_refresh` (:1241).

### 1.2 The completeness threshold family — **[AI-REPLACE]**

The contested judgment directive #3 targets. Constants (asset_reuse_planning.py:149–152): `MIN_RELATIVE_GAIN=0.05`, `MIN_ABSOLUTE_GAIN_SMALL=20`, `MIN_ABSOLUTE_GAIN_LARGE=100` (≥1000-count triggers LARGE, :1290–1293), `MAX_COMPLETENESS_SCORE_REGRESSION=1.0`.

- **`subsumption_higher`** — six ANDed threshold checks (:1278–1287): candidate_count ≥ existing×0.98; profile_detail ≥ existing×0.98; evidence ≥ existing×0.95; missing_ratio ≤ existing+0.01; profile_gap_ratio ≤ existing+0.02; effective_lane_total ≥ existing×0.98.
- **`completeness_higher`** — candidate score ≥ existing +1.0 (:1288).
- **`materially_higher` family** (:1295–1314): count/profile_detail/evidence/lane_total each ≥ `max(existing+absolute_floor, existing×1.05)`; `coverage_materially_higher` = lane_total alone OR (count+detail+evidence together) (:1311–1314).
- **Four promote branches (OR)** (:1346–1350): `explicit_baseline_inclusion_promotable` (:1331, incl. the sub-50-score "quality recovery" clause :1323–1330), `completeness_higher AND subsumption_higher`, `materially_higher_coverage_despite_snapshot_count_bias` (:1339, score_gap ≤ 3.5), `materially_higher_coverage_with_stable_quality` (:1318, score_gap ≤ 1.0).
- **Candidate selection loop** — `select_organization_asset_registry_promotion_candidate` (:1410–1450): sorts candidates by `_organization_asset_registry_candidate_sort_key` desc, skips the incumbent registry_id/snapshot_id, returns the first `promote=True`. This orchestration (which candidates, in what order) is part of what the AI judgment consumes.

### 1.3 The completeness_score formula — **[STAYS AS FEATURE, not a gate]**

`completeness_score` (asset_reuse_planning.py:740–754): `35 + profile_coverage_ratio×45 − missing_ratio×18 − profile_gap_ratio×10 − manual_ratio×5 + min(8, source_snapshot_count×1.5)`, then clamped to the `[0,100]` range; `explicit_profile_capture` discounted 0.35 into coverage (:734); bands high≥75 / medium≥50 / low (:755–760). This formula is a **deterministic signal the AI reads** (an input feature), not a rule the AI bypasses. It stays computed exactly as today so the AI sees the same coverage number a human operator would; whether any threshold on it is *retained as a hard floor* is OQ2.

### 1.4 The storage lineage / generation guard — **[STAYS HARD, fail-closed]**

Second gate, inside `upsert_organization_asset_registry` (storage.py:8633–8695), runs against `existing_rows` read in the store transaction — structurally it CANNOT move into the planning-layer decision (it needs the in-transaction row read). Two refusal shapes when an authoritative row already exists on a different snapshot:
1. same `materialization_generation_key` + lower sequence → `stale_generation_sequence_replay` (:8670–8675).
2. different lineage whose `selected_snapshot_ids` is a non-empty **strict subset** of the incumbent's → `source_snapshot_coverage_regression` (:8676–8681; the incident `{041551} ⊂ {104157,041551}`).

On refusal the row still upserts **non-authoritative** and returns `authoritative_promotion_refused` (:8683–8695). Legitimate new lineages with equal-or-wider coverage still promote. **This guard is the non-negotiable fail-closed floor of ruling ③ — no AI design may see, bypass, or soften it.**

### 1.5 The escape hatch — `force_upsert`

`registry_refresh_mode="force_upsert"` (asset_registration.py:270–279; default `"guarded_upsert"`, :78) bypasses the guarded path. Its fate is OQ7 (recommend: retained as an audited operator-only escape hatch, never a normal-path).

### 1.6 The three mis-promote incidents the AI judgment must not repeat (recon §2.3)

1. **OpenAI generation rollback** (`bc6b3fd` → `86db42c`): a stale-job recovery reconcile re-materialized an OLD snapshot and flipped OpenAI authoritative from generation sequence 6 (dual-source superset) back to 3. Root fix = the storage guard (§1.4), which took two rounds to calibrate (the first cross-lineage sequence compare mis-fired on serving-repair, 3 tests failed). **Lesson: replay safety belongs to the storage guard, NOT to any judgment layer — this is exactly why ruling ③ keeps the guard hard and outside the AI.**
2. **Google simulate pollution + merge-rebuild** (`d542b3c`): an old authoritative snapshot carried 40 simulate placeholder rows yet stayed authoritative; a cross-snapshot merge rebuild synthesized a 12,837-row / 40-placeholder-leak view (75 min, killed mid-run, artifact quarantined). **Lesson: the promote judgment must treat simulate/placeholder provenance as a hard disqualifier, and coverage must be per-snapshot, not merge-synthesized.**
3. **TML honest rejection** (same commit, the *control* case): the guard correctly rejected a TML promotion on real metrics; the operator re-routed to cache-merge with no override. **Lesson: a correct reject is a success — the AI must be free to (and audited when it does) reject.**

周边债 (recon §2.3.4): `.coord/BOARD.md` + NEXT_TODO shard-lineage-backfill / profile_fetched 对账 are known gaps in the promote evidence chain — the ruling ③ prerequisite batch merges with them.

---

## 2. Contract: the AI promote-decision schema

### 2.1 Where the AI sits relative to the current path

The AI replaces the `evaluate_organization_asset_registry_promotion` call at asset_reuse_planning.py:1541 (inside `upsert_organization_asset_registry_with_guard`). The pre-branches (§1.1) still short-circuit deterministically. For a **contested** decision (incumbent exists, candidate is a different non-refresh snapshot), the flow becomes:

```
inherit/enforce/ensure coverage (:1528–1540, unchanged)
        │
        ▼
guard-predicted PRE-FILTER  ──reject──▶ keep incumbent  (the storage-guard refusal predicate,
 (§5 V-LINEAGE, read-only)                                run early so the AI never judges a
        │ pass                                            candidate the guard will refuse)
        ▼
AI promote judgment  ──reject──▶ keep incumbent + audit
        │ approve
        ▼
retained validators (§5 V-COMP / V-GEN / V-PROV) ──fail──▶ keep incumbent + audit
        │ pass
        ▼
store.upsert_organization_asset_registry(authoritative=True)
        │
        ▼
storage lineage guard (§1.4, storage.py:8633) ── FINAL fail-closed backstop ──▶ non-authoritative + refusal audit
```

**promote ⟺ guard-pass AND AI-approve AND validators-pass.** Reject may originate from any layer. The AI can only be MORE conservative than the rules — it can turn a rule-ladder `promote` into a reject, but it can NEVER turn a guard/validator reject into a promote. The storage guard (§1.4) stays as the final in-transaction backstop even after the pre-filter, because it alone reads the committed rows.

> **Ordering discrepancy D-ORD (recorded honestly):** ruling ③ phrases the guard as a "硬前置" (hard precondition), but in current code `evaluate` (:1541) runs BEFORE the storage guard (storage.py:8633), which demotes post-hoc. The design reconciles this by (a) the conjunction above (order-independent for correctness) and (b) adding the guard-predicted pre-filter so "AI only judges guard-passed candidates" becomes literally true, while keeping the storage guard as the unmovable final backstop. The storage guard is NOT relocated — it depends on the in-transaction `existing_rows` read (storage.py:8615–8622).

### 2.2 Decision schema — `sourcing.organization_asset.ai_promote_decision.v1`

Versioned dotted-contract id (repo convention, mirrors `sourcing.profile_prefetch.ai_batch_division.v1`). One decision record per contested promote evaluation.

```jsonc
{
  "schema_id": "sourcing.organization_asset.ai_promote_decision.v1",
  "decision_id": "<ULID>",                      // audit + idempotency key
  "decision_source": "ai_judge",                // | "deterministic_prebranch" | "guard_refused" | "fallback_keep_incumbent"
  "decision": "reject",                         // "promote" | "reject" — the AI's verdict WITHIN guard-passed candidates
  "reason": "candidate adds 72 current-lane profiles at equal completeness but drops former-lane evidence; net coverage not a superset",  // ≤240 chars free text
  "reason_code": "ai_coverage_not_superset",    // controlled vocabulary; deterministic/guard/fallback records keep the current enum verbatim (§4)
  "candidate_descriptor": {                     // WHAT THE AI SEES — no guard verdict, no store internals
    "incumbent": { "snapshot_id": "…", "metrics": { "...": 0 }, "completeness_score": 0, "completeness_band": "…",
                   "selected_snapshot_ids": ["…"], "source_snapshot_count": 0, "lifecycle_status": "ready" },
    "candidate": { "snapshot_id": "…", "metrics": { "...": 0 }, "completeness_score": 0, "completeness_band": "…",
                   "selected_snapshot_ids": ["…"], "source_snapshot_count": 0, "lifecycle_status": "ready",
                   "explicit_baseline_inclusion": true },
    "coverage_evidence": {                      // per-shard request-population match (the recon §2.4 surface)
      "shards": [ { "shard_id": "…", "lane": "former", "search_query": "…", "locations": ["…"],
                    "function_ids": ["8","9","19","24"], "job_titles": ["…"], "seniority": ["…"],
                    "query_family": "…", "result_count": 304, "estimated_total_count": 0,
                    "provider_cap_hit": false, "payload_snapshot_sha256": "…" } ],
      "request_population_match": { "candidate_covers_incumbent_shards": true, "new_shards": [], "dropped_shards": [] }
    },
    "prior_snapshot_comparison": { "candidate_sort_key": [/* opaque */], "incumbent_sort_key": [/* opaque */],
                                   "simulate_or_placeholder_provenance": false }
  },
  "provenance": {
    "model_provider": "…", "requested_model": "…", "response_model": "…",
    "prompt_sha256": "…", "input_snapshot_sha256": "…",
    "usage": { "input_tokens": 0, "output_tokens": 0 }, "latency_ms": 0
  },
  "validator_results": [ { "validator": "V_LINEAGE_prefilter", "status": "pass" }, … ],
  "fallback": null                              // or the ruling-④ keep-incumbent audit (§4)
}
```

**The AI NEVER sees or emits the guard's verdict.** The `candidate_descriptor` carries coverage/lineage *evidence* (selected_snapshot_ids, per-shard populations) so the AI can reason about superset coverage, but the storage guard's pass/fail is computed independently and is not an AI input or output. Mirroring divider D7: the AI authors ONLY `{"decision", "reason", "reason_code"}` (wire-shape rule, F4 on anything else); the mint-side helper contributes `decision_id`, `provenance.*_sha256`, `input_snapshot_sha256`, assembles the envelope, and runs the validator battery. Hashes over descriptor data the model partially sees are caller-authored fact, not output repair.

### 2.3 The prerequisite recording completeness this judgment consumes (ruling ③ 前置批)

The `coverage_evidence` block requires per-shard request records. Landed (W7.1 first cut, master plan §6.5):
- `build_acquisition_shard_registry_record` (asset_reuse_planning.py:1709) writes the full request filter face into `metadata.request_filters` (:1749–1750) — including the job_titles / seniority / exclude_* axes that `query_signature` does not project and the keywords `normalize` drops.
- `estimated_total_count` now read from the probe summary at the root branch (:1815/:1890/:2028), fixing the hard-coded 0.

**Still missing (blocks the LIVE slice, not the offline slices):**
- **Provider payload snapshot capture** — the acquisition-execution side must persist the actual dispatched provider payload (a `payload_snapshot_sha256` per shard). Recon §2.4: the shard registry does NOT store the complete provider payload; `estimated_total_count` is still 0 across the existing 8-row PG sample; `provider_cap_hit` column exists but all-false. Lands with the 4b/probe execution batch.
- **Existing-row lineage backfill** — `scripts/backfill_acquisition_shard_query_families --dry-run` first, merged with the NEXT_TODO shard-lineage-backfill残留.

Until these land, the AI runs on whatever coverage evidence is recorded; `payload_snapshot_sha256` is nullable and its absence is itself a signal (a shard with no captured payload is weaker evidence). OQ1 decides whether payload-snapshot capture is a hard prerequisite for S5-flip or only for S6-live.

### 2.4 Audit record placement

The decision record lands as an **additive key `ai_promote_decision`** in the `organization_asset_registry` row's `metadata` (the registry row carries a free `metadata`/`metadata_json` payload; storage.py serializes `metadata_json` on upsert). 

> **Discrepancy D1′ (contrast with divider D1):** the divider's plan record has a whole-dict characterization oracle, so any added key breaks it. Here **no whole-dict oracle exists for the `organization_asset_registry` record OR for the `evaluate` decision dict** (§6 confirms current tests are behavioral spot-checks only). The additive audit key is therefore safe to add *before* the flip — but the same absence is why the decision surface is unpinned and S0 is mandatory. The audit key must not be derived from another contract field (Contract Field Ownership rule 4).

---

## 3. Model invocation surface

### 3.1 Provider path — follow the `ModelClient` conventions (as 议案① did)

The repo's single internal-model pattern is the `ModelClient` Protocol (model_provider.py:654) with a deterministic base + provider overrides. The promote judge adds **one Protocol method**, exactly like `divide_profile_prefetch_batches` (model_provider.py:683) did:

- `judge_organization_asset_promotion(self, payload: dict[str, Any]) -> dict[str, Any]: ...` added to the Protocol (:654) alongside `plan_search_strategy` / `judge_profile_membership` / `divide_profile_prefetch_batches`.
- `DeterministicModelClient` (:692) default returns `{}` — the structural "judge unavailable" marker → ruling-④ keep-incumbent (class F1, §4). `OfflineModelClient` inherits it, so simulate/replay are keep-incumbent-by-construction.
- `OpenAICompatibleChatModelClient` (:1510) implements it like its sibling JSON methods: `_safe_text_prompt_with_error` + `_safe_json_object` parse + the shared per-(provider, base_url, model) circuit breaker (`_record_model_provider_failure` / `_model_provider_circuit_error`, 900 s cooldown). Parse-fail/circuit-open → F4/F2. `QwenResponsesModelClient` gets the same method via `_safe_text_prompt` — but has **no circuit machinery** (divider D6): a Qwen failure maps to F3, never F2.
- **Wiring**: the promote path already runs in the registration flow that constructs a model client via `build_model_client` (model_provider.py:2592), including the live fail-closed triple-gate `assert_live_provider_access_allowed` (:2617). A billed promote-judge call clears the same triple-gate as every live provider. The judge is threaded to `upsert_organization_asset_registry_with_guard` as an injected optional client (absent → deterministic pre-branches + keep-incumbent on contested, i.e. today's behavior minus the threshold family).
- **Offline exercise path**: in simulate/replay, `build_model_client` returns `OfflineModelClient`, so the canonical hot path can NEVER exercise the AI branch (divider D2). Mirror the divider's scripted-client precedent (`ScriptedProfileBatchDividerModelClient` behind `SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER`): the judge gets a scripted variant behind a dedicated env (OQ8) returning deterministic schema-valid decisions so tests/simulate drive the AI path without a billed call.

### 3.2 Inputs

The `candidate_descriptor` (§2.2) is the payload: incumbent + candidate metric pairs (the same fields `evaluate` reads today, asset_reuse_planning.py:1254–1269), `completeness_score` as a computed feature (§1.3), selected_snapshot_ids + source_snapshot_count (lineage evidence), per-shard request populations (§2.3), and a simulate/placeholder-provenance flag (incident #2). The full payload is hashed (`input_snapshot_sha256`) into the record for replay. **The payload carries NO store internals and NO guard verdict.**

### 3.3 Output parse + strict validation

Strict manual schema validation (mirrors the divider's `analyze_page_asset` field-allowlist style): the model authors only `{"decision", "reason", "reason_code"}`; required keys, `decision ∈ {promote, reject}`, `reason_code` in the controlled vocabulary, `reason` non-empty ≤240 chars. Any violation → F4 → keep-incumbent. **No partial acceptance, no repair of a bad decision** — a repaired decision would launder an unauditable model error into an authoritative-snapshot flip.

### 3.4 Cost / latency / seat

- **One call per contested promote evaluation.** Deterministic pre-branches (§1.1) and guard-refused candidates never call the model. Promote is a materialize→promote decision point (registration / `live_promote` script / consolidation repair), **NOT a hot serving path** — no per-request latency budget applies the way ruling ②'s <1 s slot-fill does.
- Tokens: small (metric pairs + a bounded shard list) — noise against the actor spend the snapshot represents.
- Timeout: `ModelProviderSettings.timeout_seconds` defaults to 45 s (settings.py:29). The judge gets its own bounded timeout override (recommend 20 s, OQ8) mirroring the divider's `SOURCING_PROFILE_BATCH_DIVIDER_TIMEOUT_SECONDS` (=20, model_provider.py:33–34).

---

## 4. Failure / fallback semantics (ruling ④ — keep incumbent)

Every failure class → **keep the incumbent authoritative snapshot (do NOT promote)** + a recorded audit. The candidate row still lands **non-authoritative** (matching the storage guard's existing behavior, storage.py:8683 — the row persists for salvage/audit lineage, the pointer does not move). This is the asset-correctness-conservative direction: a failure NEVER flips authority.

| Class | Trigger | Detection point | `fallback_reason` |
|---|---|---|---|
| F1 judge unavailable | deterministic/offline client, model disabled, no client injected | structural `{}` return (model_provider.py:692/935 precedent) | `judge_model_unavailable` |
| F2 circuit open | prior failure within cooldown (OpenAI-compatible only, D6) | `_model_provider_circuit_error` | `judge_circuit_open` |
| F3 timeout / transport | call exceeds 20 s, HTTP/URL error, any Qwen failure | `_safe_text_prompt_with_error` error string | `judge_call_failed` |
| F4 invalid output | parse failure, schema violation, decision ∉ {promote,reject}, empty reason | §3.3 strict validation | `judge_invalid_output` |
| F5 validator rejection | any retained validator FAIL (§5) — including the guard pre-filter | §5 battery | `judge_validator_rejected:<validator>` |
| F6 stale input | incumbent changed between input snapshot and apply (another writer flipped authority) | `input_snapshot_sha256` / incumbent snapshot_id mismatch at apply | `judge_input_stale` |

Audit shape (stored in `metadata.ai_promote_decision.fallback`):

```jsonc
{
  "decision_source": "fallback_keep_incumbent",
  "fallback_reason": "judge_validator_rejected:V_COMP_regression_floor",
  "judge_error": "<truncated ≤500 chars>",
  "validator_results": [ ... ],
  "provenance": { ... }                          // present when a call was actually made (F4/F5/F6)
}
```

Deterministic pre-branch decisions (§1.1) keep the current reason strings verbatim (`no_existing_authoritative`, `same_snapshot_refresh`, `lifecycle_state_not_promotable`); the storage guard keeps its refusal reasons verbatim (`stale_generation_sequence_replay`, `source_snapshot_coverage_regression`). Only contested AI decisions use the new `reason_code` vocabulary. F2's 900 s cooldown means a flapping model degrades to "keep-incumbent for the cooldown window" — an explicitly acceptable state under ruling ④ (no promotion is always a safe state).

---

## 5. Validators-from-rules (the AI decision cannot bypass these)

Each retained hard rule becomes a validator that runs regardless of the AI verdict. **The AI can only be MORE conservative:** promote requires guard-pass AND AI-approve AND every validator pass; a reject from any validator is final. Any FAIL → F5 → keep-incumbent.

| # | Validator | Retained rule | Anchor | Direction |
|---|---|---|---|---|
| V_LINEAGE | guard-predicted pre-filter: refuse strict-subset coverage regression + stale-generation replay | storage lineage guard (§1.4) | storage.py:8670–8681 (predicate mirrored read-only pre-decision; storage.py stays the final backstop) | **AI never sees the verdict**; both the pre-filter and the in-transaction backstop fail closed |
| V_COMP | no material completeness/coverage regression: candidate must not drop below incumbent beyond the retained floor | completeness/coverage non-regression (OQ2 sets the floor) | `MAX_COMPLETENESS_SCORE_REGRESSION` :152; `coverage_materially_higher` :1311 as the *shape* of "not a regression" | AI-approve of a regressing candidate still FAILS here |
| V_GEN | generation monotonicity: never make an authoritative generation sequence go backward | generation sequence guard | storage.py:8670–8675 (`stale_generation_sequence_replay`) | fail-closed, independent of AI |
| V_PROV | provenance sanity: reject simulate/placeholder-tainted candidates; require lineage evidence present | incidents #1/#2 lessons (§1.6) | recon §2.3 (simulate 40-placeholder incident) | AI-approve of a placeholder-tainted candidate still FAILS |
| V_LIFECYCLE | candidate lifecycle promotable | §1.1 pre-branch | asset_reuse_planning.py:1224 | deterministic, pre-AI |

V_COMP's exact floor is OQ2: retire the six subsumption thresholds + four promote branches to AI judgment, but keep a **coarse** non-regression floor (e.g. "candidate coverage is not strictly narrower than incumbent, and completeness_score does not regress beyond `MAX_COMPLETENESS_SCORE_REGRESSION`") so the AI's freedom is "within guard-pass and non-regression" rather than "unbounded." This is the crux of ruling ④'s conservatism: the AI gains judgment over *whether a wider/richer candidate is genuinely better*, but never the power to promote a *thinner* one.

---

## 6. Characterization prerequisite (S0 — mandatory before any AI slice)

**Finding: no promote characterization oracle exists.** The current promote tests are behavioral spot-checks, not a whole-dict/shape-exact pin:
- `test_organization_execution_profile.py` (82 asserts): `evaluate_organization_asset_registry_promotion` called at :101/:264/:1123 with `assertTrue(decision["subsumption_higher"])` / `assertEqual(decision["reason"], …)` — asserts *individual keys* on *hand-picked scenarios*, not the whole decision dict across the threshold-boundary grid.
- `test_asset_consolidation_audit.py` (107 asserts): pins the two storage-guard refusal shapes (:977 `stale_generation_sequence_replay`, :996 `source_snapshot_coverage_regression`, :1010–1054 the legitimate-promotion negatives) — good coverage of §1.4, but nothing on the §1.2 threshold family.
- `test_authoritative_source_provenance.py` (14 asserts): one `upsert_organization_asset_registry_with_guard` path (:101).

None of these is the promote-side analogue of the divider's `test_fetch_profile_batch_characterization.py` (whole-dict goldens, the ruling-① prerequisite). **Therefore S0 is a required first slice**, exactly as the divider's oracle was ruling ①'s prerequisite:

**S0 = pin the current promote surface byte/shape-exact BEFORE touching it:**
1. `evaluate_organization_asset_registry_promotion` whole-dict goldens across the threshold-boundary grid: the three pre-branches (§1.1); each of the four promote branches (§1.2) at its boundary (subsumption 0.98/0.95/±0.01/±0.02 edges; completeness +1.0 edge; material floors at 20/100/×1.05; score_gap 1.0/3.5 edges; the sub-50-score quality-recovery clause :1323–1330); the LARGE (≥1000) vs SMALL absolute-floor switch (:1290–1293).
2. `completeness_score` formula golden grid (§1.3): coverage/missing/gap/manual/snapshot-count contributions, the 0.35 explicit-capture discount, clamp[0,100], the three bands.
3. `select_organization_asset_registry_promotion_candidate` goldens (§1.2): sort order, incumbent skip, first-promote selection, the no-candidate/no-incumbent shapes.
4. Storage guard two-refusal-shape pins (extend/keep `test_asset_consolidation_audit.py`) as the **[STAYS HARD]** contract — these goldens are NOT demoted at flip; they are the permanent floor.

Offline, pure-function (no PG for #1–#3; #4 uses the existing PG store fixture). Registered in the GH lane + lane manifest + `regression_matrix` paired mapping (asset_reuse_planning / this oracle / storage-guard), mirroring the divider's paired mapping. **The AI judgment must not weaken this oracle before flip; at flip (S5) the §1.2 threshold goldens demote to the validator battery (§5) exactly as ruling ③'s "AI 只在 guard 放行候选内判" while the §1.4 storage-guard goldens stay hard.**

---

## 7. Simulate-first validation ladder (live deferred to quota + operator)

Every slice keeps S0 green until the single gated flip; the judge is additive throughout.

1. **S0** — characterization oracle green (the ruling-③ prerequisite for touching the threshold family).
2. **Shadow slices (S1–S3)** — contract module, model method + scripted client, shadow recording. The judge computes and records a decision in `metadata.ai_promote_decision`; **authority still comes from `evaluate` (the ladder)**. S0 stays byte-identical green; a new offline suite pins the decision schema, the validator battery, every F1–F6 audit shape, and an "AI decision ≠ ladder decision" divergence counter (free before/after evidence for the flip).
3. **Prerequisite S4 (parallel)** — shard-recording completeness closure (§2.3): provider payload snapshot capture + lineage backfill. Its own review request (registry/shard contract fields).
4. **Flip slice (S5)** — the judge replaces the `evaluate` seat in the conjunction (§2.1); §1.2 threshold goldens demote to validators (§5); the storage guard + its goldens STAY hard. Simulate e2e both paths (scripted judge on/off) reach coherent authoritative + audit states. **Review-gate GO required before landing.**
5. **Live validation (S6)** — explicitly deferred to HarvestAPI/provider quota restoration + explicit operator go; red lines unchanged (triple-gate env, committed `scripts/live_*.py` only, delta-only). The `live_promote_company_snapshot.py` dry-run (recon §2.1) is extended to print the AI decision + guard prediction side by side (OQ7).

---

## 8. Operator questions (the AskUserQuestion batch)

Mapping against the recon's C-section (recon §5): ruling ③ settled **Q9** (two gates), ruling ④ settled **Q12** (fail-closed). Remaining recon promote questions **Q8, Q10, Q11** belong here, plus new questions this design surfaces.

- **OQ1 (= recon Q8) — AI input surface + prerequisite recording completeness.** Does the judge consume the full `candidate_descriptor` (§2.2: metric pairs, completeness_score feature, selected_snapshot_ids/lineage, per-shard request populations, simulate-provenance flag, input-snapshot hash)? And must the remaining shard-recording gaps (provider payload snapshot capture + lineage backfill, §2.3) be a hard prerequisite before which slice?
  **Recommended**: full descriptor + snapshot hash recorded; require payload-snapshot capture + backfill as a prerequisite for **S6-live only** (offline/shadow slices S1–S3 run on whatever is recorded — a missing `payload_snapshot_sha256` is itself a weak-evidence signal). Merge the backfill with the NEXT_TODO shard-lineage-backfill残留.
  *Alternatives*: (b) require full recording completeness before S1 — safest, but blocks all shadow evidence-gathering on a quota-blocked capture batch; (c) minimal inputs (metric pairs only) — rejected: the AI can't beat the ladder on anything the ladder can't already see.

- **OQ2 (= recon Q10) — fate of the `evaluate` threshold family + retained floor.** Retire the six subsumption thresholds (0.98/0.95/0.01/0.02) and the four promote branches (§1.2) to AI judgment; keep `completeness_score` as a computed input **feature** (not a gate); keep a **coarse non-regression floor** as retained validator V_COMP (never promote a strictly narrower / completeness-regressing candidate)?
  **Recommended**: yes — retire the branch thresholds to AI judgment; completeness_score stays a feature; the coverage-subset non-regression is already the storage guard (V_LINEAGE, stays hard) and a coarse V_COMP score floor backstops it. This keeps the AI "more-conservative-only."
  *Alternatives*: (b) also keep `completeness_higher` (+1.0) as a hard promote floor (AI can only add rejects on top) — most conservative, smallest AI win; (c) AI fully owns regression judgment with no V_COMP floor — rejected: re-opens the incident #2 class (a persuasive reason string could flip a thinner snapshot).

- **OQ3 (new) — the seat + conjunction.** Confirm: `promote ⟺ storage-guard-pass AND AI-approve AND validators-pass`; the AI replaces the `evaluate` call (asset_reuse_planning.py:1541); the storage lineage guard (storage.py:8633) stays downstream as the unmovable final backstop; a guard-predicted pre-filter runs early so the AI never judges a guard-refused candidate; the AI never sees or emits the guard verdict; reject may come from any layer (discrepancy D-ORD, §2.1)?
  **Recommended**: yes — the only shape that satisfies ruling ③'s "AI 只在 guard 放行候选内判" without relocating the in-transaction guard.
  *Alternatives*: relocate the guard wholesale before the AI — rejected: it depends on the in-transaction `existing_rows` read (storage.py:8615).

- **OQ4 (new) — engagement scope.** Invoke the AI only on **contested** decisions (incumbent exists AND candidate is a different, non-refresh, promotable snapshot)? Keep the three pre-branches (§1.1: no-incumbent, same-snapshot-refresh, lifecycle-not-promotable) deterministic and model-free?
  **Recommended**: yes — the pre-branches are trivially correct and cheap; judgment is only meaningful when two real snapshots compete.
  *Alternatives*: AI on every decision — rejected: wasteful and adds model risk to trivially-correct paths.

- **OQ5 (new) — failure/fallback direction (ruling ④ confirmation).** Confirm every failure class (F1–F6, §4) → keep the incumbent authoritative, do NOT promote, land the candidate row non-authoritative + audit — mirroring the storage guard's existing refusal behavior (storage.py:8683)?
  **Recommended**: yes — a failure must never flip authority; no-promotion is always a safe state.
  *Alternatives*: fall back to the `evaluate` ladder on judge-unavailable (like the divider's ruling ④a) — **rejected: ruling ④ explicitly separates promote (asset-correctness → keep incumbent) from division (efficiency → ladder).** Recorded to show the divider precedent was consciously NOT followed here.

- **OQ6 (new) — S0 characterization oracle (mandatory).** Accept that no promote characterization oracle exists (§6 — current tests are behavioral spot-checks) and that **S0 = pin the `evaluate` decision goldens + completeness_score formula grid + candidate-selection goldens + storage-guard two-refusal pins BEFORE any AI slice**, mirroring the divider's ruling-① oracle prerequisite?
  **Recommended**: yes — S0 is a hard prerequisite; the §1.2 threshold goldens demote to validators at flip while the §1.4 storage-guard goldens stay permanent.
  *Alternatives*: proceed on the existing spot-checks — rejected: the flip would be unpinned; a threshold-boundary regression would land silently.

- **OQ7 (new) — audit product + escape hatch + dry-run.** Land the decision record as an additive `metadata.ai_promote_decision` key on the registry row (D1′-safe, §2.4)? Keep `force_upsert` (asset_registration.py:270) as an audited operator-only escape hatch? Extend `scripts/live_promote_company_snapshot.py` dry-run to print the AI decision beside the guard prediction?
  **Recommended**: yes to all three — additive metadata key (no whole-dict oracle to break), force_upsert retained but audit-visible + blocked in normal-path signoff (Contract Field Ownership rule 5), dry-run shows both gates.
  *Alternatives*: new dedicated `organization_promote_decision` table — cleaner queryability, but a migration + reader/writer surface for a record that is 1:1 with a registry row; defer unless audit volume demands it.

- **OQ8 (new) — model routing + timeout + scripted client.** Use the configured `ModelProviderSettings` model as-is (no per-call pin, consistent with every ModelClient method) with a promote-judge-specific 20 s timeout override; offline AI-path testing via a scripted client behind a dedicated env (mirroring `SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER`) — name `SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE`?
  **Recommended**: yes — model choice stays operator-owned in settings; only the timeout is judge-specific; dedicated scripted flag (different blast radius from the divider/planning flags).
  *Alternatives*: reuse the divider or planning scripted flag — rejected: couples unrelated opt-ins.

**Cross-reference to remaining recon questions:** recon Q11 (audit product location + force_upsert + dry-run) is fully absorbed by OQ7. Recon Q8/Q10 are OQ1/OQ2. The recon's directive-#4 compensation questions (Q17 补 promote as a typed durable command) and directive-#5 behavior-layer questions (Q13–Q15) are OUT of this proposal's scope — they belong to the compensation doc and the behavior-layer doc respectively. Recon Q18's promote-oracle half is answered by §6 (S0).

---

## 9. Implementation slices + review-gate routing

Each slice is independently green; §7-protocol applies (implement + targeted tests + pin commit + record review request, then continue; NO-GO freezes only the affected scope's promotion).

| Slice | Content | Gate/oracle state | Review gate |
|---|---|---|---|
| **S0** | **Characterization oracle** (§6): `evaluate` whole-dict goldens across the threshold-boundary grid + completeness_score formula grid + candidate-selection goldens + storage-guard two-refusal pins. New `tests/test_organization_promote_characterization.py` (pure-function + PG-store for the guard); GH lane + manifest + regression_matrix paired mapping. **Prerequisite — must land before S1.** | establishes the oracle | request recorded (trigger 1: pins a contract surface); blocks nothing before S5 |
| **S1** | Decision contract module: schema `sourcing.organization_asset.ai_promote_decision.v1`, strict parser, validator battery (V_LINEAGE pre-filter / V_COMP / V_GEN / V_PROV / V_LIFECYCLE as pure functions) + offline suite. | S0 untouched | request recorded (trigger 1: new contract schema) — verdict blocks nothing before S5 |
| **S2** | `ModelClient.judge_organization_asset_promotion` Protocol method + deterministic default `{}` (F1) + OpenAICompatible/Qwen impls (20 s timeout, shared circuit / D6 Qwen-no-circuit) + scripted judge client behind `SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE` + orchestration helper (`propose_and_validate_promotion`: OQ4 contested-only gate, one call per decision, envelope assembly, F1–F6 audit mapping via S1). | S0 untouched | rides S1 (trigger 3: model behavior) |
| **S3** | Shadow mode: record `metadata.ai_promote_decision` at the `upsert_organization_asset_registry_with_guard` seam (asset_reuse_planning.py:1541), decision computed but **authority still from `evaluate`**; ladder-divergence counter. Structural non-invocation: no judge client (D2), pre-branch decisions (§1.1), guard-refused candidates. | S0 byte-identical green | none beyond S1/S2 (additive, authority unchanged) |
| **S4** | **Prerequisite (parallel after S0)** — shard-recording completeness closure (§2.3): provider payload snapshot capture (acquisition-execution side) + `estimated_total` / `provider_cap_hit` population + existing-row lineage backfill (`--dry-run` first, merged with NEXT_TODO). | S0 untouched | **own review request** (trigger 1: shard registry contract fields) |
| **S5** | **Flip**: judge replaces the `evaluate` seat in the conjunction (§2.1); §1.2 threshold goldens demoted to validators (§5); storage guard + its goldens STAY hard; simulate e2e both paths. | S0 §1.2 goldens rewritten in-slice per ruling ③; §1.4 storage goldens unchanged | **GO verdict required before landing** (trigger 5: claimed feature) + operator confirmation of OQ1–OQ8 |
| **S6** | Live promote validation (extended `live_promote` dry-run showing both gates). | n/a | operator explicit go + quota + live red lines |

Ordering: **S0 first (hard prerequisite)** → S1→S2→S3 after the OQ ruling batch; S4 in parallel after S0; S5 strictly after S4's GO and the pinned review of the flip diff; S6 indefinitely deferred to quota + operator.

---

*Design method: read-only over the current tree (asset_reuse_planning.py:1217/731/149, storage.py:8633, asset_registration.py:270, model_provider.py:654/683/2592, settings.py:29) + the recon §2 forensics + the divider design as the proven议案 shape. No src/tests changed; no live env; no PG mutation. Anchors verified on branch `governance-phase0-ttl-20260611`.*
