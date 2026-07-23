# WS7/W7.2 — AI Batch Divider Design (fetch-profile `dispatch_item_specs` plan stage)

> Status: DESIGN DRAFT 2026-07-23, awaiting operator rulings (§6 question batch) + independent review gate (contract-heavy → §7 protocol; no implementation slice lands before the ruling batch answers OQ1–OQ8 and the review verdict for the contract slices). Authority: operator ruling ① + ④ RATIFIED 2026-07-22 ([REFACTOR_MASTER_PLAN.md](REFACTOR_MASTER_PLAN.md) §6.5); factual base: [WS7_STRONG_AGENT_RECON_2026-07-22.md](WS7_STRONG_AGENT_RECON_2026-07-22.md) + the ruling-① characterization oracle `tests/test_fetch_profile_batch_characterization.py` (28 tests + 36 subtests, landed 2026-07-23).

```
status: design-draft    owner: operator (rulings pending)
canonical-path: sourcing-ai-agent/docs/WS7_AI_BATCH_DIVIDER_DESIGN.md
drafted: 2026-07-23     oracle-pin: tests/test_fetch_profile_batch_characterization.py
```

Unprefixed `file:line` anchors refer to `sourcing-ai-agent/src/sourcing_agent/`; test anchors to `sourcing-ai-agent/tests/`. Anchors verified on this tree (branch `governance-phase0-ttl-20260611`).

## 0. Ratified constraints (transcribed; NOT up for redesign)

1. **Ruling ①** — the AI divider is an **independent plan stage** that directly produces `dispatch_item_specs` (4–8 batches + per-batch membership + explainable reason). The current constant rule ladder (recon §1.2) demotes to **acceptance validators**: per-batch ≤300 members, ≤8 batches per wave, R7 reason audit, R5/R6 wave semantics preserved. Prerequisite (DONE 2026-07-23): the scheduler plan-record characterization oracle.
2. **Ruling ④** — on divider failure/unavailability, division **falls back to the current rule ladder with a recorded fallback audit**. (Promotion fail-closed is ruling ③'s domain; deliberately out of scope here.)
3. **Operator directive 2** — target completion within 1–2 rounds of HarvestAPI actors budget; **KEEP** url-level failure recording + the single unified final retry pass.
4. **Budget model** (recon §4.3) — 3 actors / $4 per 1k (no-email) / DB limiter default 4 (`resolved_harvest_profile_actor_global_inflight`, runtime_tuning.py:354–361; fast_smoke=2, runtime_tuning.py:30). "1–2 rounds" = `ceil(batch_count / actor_global_inflight) ≤ 2`; 4–8 AI batches ≈ 1–2 full actor waves (8 batches @ inflight 4 → 2 rounds; provider ceiling ~8 is unverified until the probe round — see discrepancy D4).

## 1. Contract: the `dispatch_item_specs` divider output

### 1.1 The surface being replaced (oracle-pinned)

Today `dispatch_item_specs` is minted exclusively by `_build_profile_prefetch_batch_plan` (enrichment.py:1327–1556) as field `ProfilePrefetchBatchPlan.dispatch_item_specs: list[tuple[int, list[ProfilePrefetchQueueItem]]]` (enrichment.py:1225). The membership semantic is: chunks are **contiguous, order-preserving slices of the input URL list** cut by `_actor_slot_chunk_strings` at the window `batch_size` (:1491–1494), then tiny-merged by `_coalesce_tiny_profile_dispatch_chunks` (:1496–1500). The oracle pins that ordering semantic explicitly (test_fetch_profile_batch_characterization.py:493–495 "chunks are contiguous, order-preserving slices … that ordering semantic is itself part of the pin", `_assert_specs` :502–507). **The AI divider's whole point is to break exactly this semantic** — free regrouping of members into 4–8 batches — which is why the flip is a gated, single-slice event (§5).

The plan record surface the divider extends:

- `ProfilePrefetchBatchPlan.to_record()` (enrichment.py:1253–1324): `kind="profile_prefetch_batch_plan"`, `schema_version=1`, plus the 40-key record whose FULL dict shape is pinned by `_base_record` whole-dict `assertEqual` (test :142–192 — "any added/removed/renamed record key fails the oracle").
- Persistence: per-item registry writes via `_record_profile_prefetch_batch_plan_items` (enrichment.py:1559, called :5642) and the record embedded as `"batch_plan"` in refill outcome payloads (:5722/:5753/:5810/:6429).
- Mint site: the refill/replan path builds window → durable-wave inheritance → worker budget → queue items → plan (enrichment.py:5598–5641).

### 1.2 Divider output schema — `sourcing.profile_prefetch.ai_batch_division.v1`

The divider produces one **division record** per wave mint. Versioned schema id string follows the repo's dotted-versioned-contract convention (`sourcing.x_first.subject_selection.v1`, `x.research_scope.catalog.v1` — recon §3):

```jsonc
{
  "schema_id": "sourcing.profile_prefetch.ai_batch_division.v1",
  "division_id": "<ULID>",                    // wave-scoped identity; idempotency + R6 key (§4.3)
  "division_source": "ai_divider",            // | "rule_ladder_fallback"
  "batch_count": 6,                           // len(batches)
  "batches": [
    {
      "batch_index": 1,                       // 1-based, mirrors dispatch_item_specs ordinals
      "member_index_ranges": [[0, 249], [1800, 1849]],  // inclusive ranges over the canonical
                                              // input inventory ordering (§2.2) — the model
                                              // never echoes raw URLs
      "member_count": 300,
      "reason": "roster-heavy cohort, same source shard, fills one provider envelope",  // ≤240 chars, free text
      "reason_code": "ai_division"            // R7 vocabulary extension; fallback records keep
                                              // the ladder's original enum values verbatim
    }
  ],
  "membership_sha256": "…",                   // hash over the ordered normalized url-key partition
  "provenance": {
    "model_provider": "…", "requested_model": "…", "response_model": "…",
    "prompt_sha256": "…", "input_snapshot_sha256": "…",
    "usage": {"input_tokens": 0, "output_tokens": 0}, "latency_ms": 0
  },
  "validator_results": [ {"validator": "V1_provider_envelope", "status": "pass"}, … ],
  "fallback": null                            // or the ruling-④ audit object (§3)
}
```

Membership representation: **index ranges over a canonically ordered inventory** (the deduped, normalized ready-set ordering already produced by `_build_profile_prefetch_queue_items`, keys via `normalize_linkedin_profile_url_key` — the same normalization the oracle and R6 registry entries use, test :91/:399–408). The applier expands ranges to `ProfilePrefetchQueueItem` chunks; the expansion must be an **exact partition** (validator V3). Rationale: a 3,000-url set must not be round-tripped through the model verbatim (token cost, echo-corruption risk); ranges keep membership exact and cheaply verifiable.

Placement relative to the current record: **additive key `ai_batch_division`** inside the plan record, landing **only at the flip slice** together with `schema_version` 1→2. This is forced, not optional: the oracle's `_base_record` whole-dict compare (test :142–192) fails on ANY added key, so shadow-mode slices (§5) must store the division record **outside** the plan record (in the `refill_plan_items` activity surface next to `_record_profile_prefetch_batch_plan_items`), and the plan record gains the key exactly once, in the same slice that demotes the oracle per ruling ①. (Discrepancy D1, §4.6.)

### 1.3 Validator battery (each = one current-ladder rule, with its oracle pin)

Validators run on every AI division before it is applied; any FAIL → ruling-④ fallback (class F5, §3). The ladder itself survives as the fallback implementation, so its unit goldens survive with it.

| # | Validator | Demoted ladder rule | Current-code anchor | Oracle pin |
|---|---|---|---|---|
| V1 | every batch ≤ 300 members | provider envelope cap | `PROVIDER_ENVELOPE_MAX_URLS`, constant band enrichment.py:465–503 | ConstantLadderBaselineTest :202–211; R1.d golden grid rows :255–258 |
| V2 | ≤ 8 batches per wave | `MAX_BATCH_COUNT_FOR_LARGE_READY_SET` | same band; sizer R1.d :677–702 | `test_large_3000_hits_the_8_cap_and_defers_the_surplus_wave` :665–713 |
| V3 | exact partition: batches cover the ready set, no duplicate/omitted/invented member | (implicit in contiguous slicing today) | `_actor_slot_chunk_strings` :1491 | `_assert_specs` membership pins :502–507 (every plan golden) |
| V4 | batch < 50 only with an R7-legal tiny reason (`retry_isolation`, `queue_quiescent_final_tail`, `low_volume_company`, …) | R4 sub-50 tail guard + tiny legality set | tail guard :1467–1490; legal-reason set :1955–1962 | sub-50 tail goldens :532–575; retry tiny golden :803–835 |
| V5 | dispatched specs ≤ `available_new_worker_count`; surplus defers (never overruns worker budget) | R5 wave bounding | `_split_profile_prefetch_dispatch_specs` :1062 | DispatchSpecSplitCharacterizationTest :470–489; 3000-case deferred 600 :713 |
| V6 | an in-flight durable wave is never re-divided; division happens at wave mint only | R6 durable-wave inheritance | `_apply_durable_refill_wave_dispatch_window` :1162 | DurableWaveInheritanceCharacterizationTest :387–467 (incl. append/retry/empty never-inherit :462–467) |
| V7 | every batch carries an explainable reason; record keeps an R7-auditable `reason_code` | R7 `batch_size_reason` audit enum | window/record fields :1306–1317 | sizer golden grid reasons :231–259; record `batch_size_reason` pins throughout |
| V8 | retry_wait items are never mixed into a normal-wave division; the retry wave is not AI-divided at all (§4.2) | retry isolation | `_profile_refill_retry_gate` :3900–4020; plan retry branch :1348–1350/:1536–1537 | `test_retry_wait_items_dispatch_as_isolated_wave_even_when_tiny` :803–835 |
| V9 | `ceil(batch_count / actor_global_inflight) ≤ 2` (the "1–2 rounds" budget as a machine check) | (new — formalizes directive 2; recon §4.3) | runtime_tuning.py:354–361 | none yet — added with the validator (OQ3) |
| V10 | `batch_count ∈ [4, 8]` when the divider is engaged (engagement threshold = OQ6) | (new — ruling ① batch-count band) | — | none yet — added with the validator |

## 2. Model invocation surface

### 2.1 Provider path — follow the existing `ModelClient` conventions

The repo's single pattern for internal model use is the `ModelClient` Protocol (model_provider.py:606–639) with a deterministic base implementation and provider-backed overrides. The divider adds **one Protocol method**:

- `divide_profile_prefetch_batches(self, payload: dict[str, Any]) -> dict[str, Any]: ...` added to the Protocol (:606) alongside `plan_search_strategy`/`judge_profile_membership` etc.
- `DeterministicModelClient` (:642) default returns `{}` — the structural "divider unavailable" marker, which the caller maps to ruling-④ fallback class F1. `OfflineModelClient` (:886) inherits it, so simulate/replay modes are fallback-by-construction.
- `OpenAICompatibleChatModelClient` (:1274) implements it exactly like its sibling JSON methods: `_safe_text_prompt_with_error` (:1239, exception → truncated error string via `_model_call_error_message` :175) + `_safe_json_object` parse (:1972) + the shared per-(provider, base_url, model) circuit breaker — `_record_model_provider_failure` (:223, 900 s default cooldown) / `_model_provider_circuit_error` (:208). A parse failure or circuit-open maps to F4/F2. `QwenResponsesModelClient` (:960) gets the same method via `_safe_text_prompt` (:1231/:1245).
- **Wiring already exists**: the plan stage lives in `MultiSourceEnricher`, which already receives an injected `model_client` (enrichment.py:2421–2428). No new construction path; `build_model_client` (model_provider.py:2263–2291) stays the single factory, including its live fail-closed gate (`assert_live_provider_access_allowed`, :2280–2285 — a billed divider call clears the same triple-gate as every live provider).
- **Offline exercise path**: in simulate/replay/scripted, `build_model_client` returns `OfflineModelClient` (:2268–2274), so the canonical simulate hot path can NEVER exercise the AI branch (discrepancy D2). Mirror the existing scripted-planning precedent: `ScriptedLivePlanningModelClient` (:909) behind `_scripted_live_model_planning_enabled` (:168–172, env `SOURCING_SCRIPTED_LIVE_MODEL_PLANNING`, model_provider.py:23) — the divider gets a scripted client variant (env name = OQ7) that returns deterministic, schema-valid divisions so tests and simulate e2e can drive the AI path without a billed call.

### 2.2 Inputs (candidate inventory shape)

One call per wave mint, at the point where today's window+plan are computed (enrichment.py:5598–5631), **before** `_build_profile_prefetch_batch_plan`. Payload:

- **Canonical inventory**: ordered list index → `{url_key_suffix (short display token), source_shards, queue_state, attempt_count, last_failure_class, priority}` — per-URL state from the registry entries already fetched at :5598 (`_current_prefetch_registry_entries`) and shard provenance from `source_shards_by_url` (:5582–5584). For sets >~600, per-URL rows collapse to per-(shard × state) aggregate groups with index ranges — membership exactness is preserved because the model answers in index ranges either way (§1.2).
- **Sizes**: ready count, `requested_url_count`, `candidate_count`, deferred/tail counts from the prior plan.
- **Budget context**: `worker_budget` dict (the exact shape the oracle pins, test :100–108), `actor_global_inflight`, the validator constants (300/8/50), and the cost line: $4/1k no-email, per-batch cap `max(est+0.25, est×1.25)` (harvest_connectors.py:3782–3803; recon §4.1).
- **Prior-round context**: durable-wave fields for any items already claimed by a wave (R6 fields, enrichment.py registry keys `refill_plan_batch_size/count/window_url_count` — oracle :399–408), retry_wait counts, url-level failure history (the retained mechanism, §4.1).
- The full payload is hashed (`input_snapshot_sha256`) and stored in the division record for replay/audit; the payload itself is recorded on the activity surface (size-bounded), not inside the plan record.

### 2.3 Output parsing + strict validation

Strict manual schema validation (repo has no jsonschema runtime dependency for this path; validation mirrors the field-allowlist style of `analyze_page_asset` :1446–1467): required keys, types, index ranges within bounds, ranges non-overlapping, reasons non-empty. Then the V1–V10 battery (§1.3). Any violation → F4/F5 fallback; **no partial acceptance, no "repair" of a bad division** — repaired output would launder an unauditable model error into a dispatch shape.

### 2.4 Cost/latency envelope

- **One call per wave mint** (idempotent via `division_id`; re-ticks and R6-inherited replans reuse the recorded division — §4.3/§4.5). A 2-round Google-scale wave (recon §2.4 sample) is 1 call total, not 1 per tick.
- Tokens: ~2–6k in (aggregated inventory) / ≤800 out (ranges + reasons). At any plausible internal-model pricing this is noise against the actor spend it optimizes ($8 for a 2,000-url wave, recon §4.3).
- Latency: `ModelProviderSettings.timeout_seconds` defaults to 45 s (settings.py:29) — too long to sit inside a refill tick with a per-phase budget (recon §1.3, `phase_budget_exhausted`). The divider call gets its own bounded timeout (recommend 20 s, OQ8) and runs only on the wave-mint path, never on the completion-refill fast path (§4.5), so ruling ②'s <1 s slot-fill target is untouched.

### 2.5 Seat

Plan stage only. `_dispatch_prefetch_chunk` (enrichment.py:5945 on this tree — recon's :5882 anchor has drifted) and the whole submit/ingest path consume `dispatch_item_specs` exactly as today; the divider changes who mints the specs, never how they dispatch.

## 3. Failure/fallback semantics (ruling ④)

Every failure class → **rule-ladder fallback** (the current `_build_profile_prefetch_batch_plan` path runs unchanged) + a recorded audit. Dispatch is never blocked by divider failure — division is an efficiency concern, not an asset-correctness concern (ruling ④'s stated rationale; contrast ruling ③).

| Class | Trigger | Detection point | `fallback_reason` |
|---|---|---|---|
| F1 model unavailable | deterministic/offline client, model disabled, no client injected | structural `{}` return | `divider_model_unavailable` |
| F2 circuit open | prior failure within cooldown | `_model_provider_circuit_error` (model_provider.py:208) | `divider_circuit_open` |
| F3 timeout / transport | call exceeds divider timeout, HTTP/URL error | `_safe_text_prompt_with_error` error string (:1239) | `divider_call_failed` |
| F4 invalid output | JSON parse failure, schema violation, out-of-bounds/overlapping ranges | §2.3 strict validation | `divider_invalid_output` |
| F5 validator rejection | any V1–V10 FAIL | §1.3 battery | `divider_validator_rejected:<validator>` |
| F6 stale input | ready set changed between input snapshot and apply (lock revalidation drops/adds URLs, enrichment.py:5560–5575) | membership_sha256 mismatch at apply | `divider_input_stale` |

Audit record shape (stored where the division record would go; on the plan record post-flip it appears as `ai_batch_division.fallback`):

```jsonc
{
  "division_source": "rule_ladder_fallback",
  "fallback_reason": "divider_validator_rejected:V2_batch_ceiling",
  "divider_error": "<truncated ≤500 chars>",           // mirrors _model_call_error_message
  "validator_results": [...],                            // whatever ran before the failure
  "provenance": {...}                                    // present when a call was actually made (F4/F5/F6)
}
```

Fallback plans keep today's `batch_size_reason` / `plan_reason` vocabularies **verbatim** (oracle-pinned), so downstream consumers of R7 audit fields see no drift; the only new information is the fallback audit object beside them. F2's cooldown (900 s default) means a flapping model degrades to pure ladder behavior for the cooldown window — an explicitly acceptable state under ruling ④.

## 4. Interplay with the retained mechanisms

### 4.1 url-level failure recording — unchanged

The retained per-URL failure spine is untouched: `failed_urls` extraction from worker summaries (enrichment.py:3396–3416), `worker_status ∈ {backpressure, failed} → retry_wait` activity (:3440–3452), the `linkedin.profile_url.terminal_record` command family (:2930–2995), and failed-url writeback with `flush_reason="retry_isolation"` (:6046–6083). The divider only consumes this history as input (§2.2); it never writes it.

### 4.2 Unified final retry pass — out of divider scope

`_profile_refill_retry_gate` (:3900–4020, "Retry is a separate wave. It may start only after the normal wave is closed") is untouched, and the retry wave is **never AI-divided**: retry sets are small, `retry_isolation` is already a legal tiny reason (:1955–1962), and the plan's retry branch (`retry_wait_isolated_dispatch`, :1536–1537) is pinned by the oracle (test :803–835). Validator V8 enforces the exclusion. The retry pass naturally lands as the second round (or the tail of round 2) of the 1–2-round budget (recon §4.3).

### 4.3 R6 wave inheritance — needs a bounded contract extension (honest gap)

R6 today claims a recomputed window when a **scalar** recorded wave (`refill_plan_batch_size/batch_count/window_url_count` per registry item) is at least as large (enrichment.py:1162; oracle :387–467). An AI division is **heterogeneous** — per-batch sizes differ — so the scalar identity cannot represent it. Design: each divided item records `division_id` + its `batch_index`; the inheritance check gains a branch — *if the dispatch set's items carry a live `division_id`, the recorded division claims the window wholesale (no re-split, no re-call), preserving R6's invariant-1 ("same wave is never re-cut") by identity rather than by size comparison*. Append-replan/retry/empty-set still never inherit (oracle :462–467). This is a real contract-field change (registry item schema) — it is its own slice with its own review request (§7, S4), and until it lands the divider cannot flip. Recorded here honestly rather than papered over: **ruling ① says "R5/R6 wave semantics preserved", and the scalar R6 mechanism as-is cannot host AI output; preserving the semantic requires this extension.**

### 4.4 retry_wait isolation pins

Oracle pins that a pure-retry_wait queue flips the whole record to `refill_policy="retry_wait_isolated_refill"` / `plan_reason="retry_wait_isolated_dispatch"` (test :803–835). The divider path is bypassed entirely for such waves (V8); fallback plans reproduce the pinned record byte-for-byte because they ARE the current code path.

### 4.5 Slot-refill immediate tick (ruling ②)

Ruling ② makes completion events wake the refill daemon for an immediate bounded tick (target <1 s), on top of today's signal-only + 5 s poll (recon §1.3; `_run_profile_completion_next_submit_opportunity` orchestrator.py:71622, "Completion callbacks are not profile-refill executors"). Interplay rule: **completion-driven refills of an in-flight wave never call the divider** — they hit R6 inheritance (`division_id` claim, §4.3) and submit the next recorded batch immediately; a model call on that path would blow the <1 s target by an order of magnitude. The divider runs only when a *new* wave is minted (fresh ready set with no live division), which is poll-tick/enqueue territory where a bounded 20 s call is affordable. Idempotency: `division_id` is claimed under the same scheduler lock that guards plan minting today (enrichment.py:5560 lock-revalidation block), so ruling ②'s higher tick frequency cannot double-call the model.

### 4.6 Recorded discrepancies (assumptions the code contradicts)

- **D1** — the oracle's whole-dict record compare (test :142–192) makes ANY additive plan-record key a break; therefore shadow-mode divider output cannot live in the plan record, and the record extension + oracle demotion are one atomic flip slice (§5).
- **D2** — in simulate/replay `build_model_client` returns `OfflineModelClient` (model_provider.py:2268–2274): the canonical hot path exercises ruling-④ fallback by construction, never the AI branch. The scripted divider client (§2.1) is the only way to e2e the AI path offline; live is the only place the real model runs.
- **D3** — the recon's dispatch anchor `_dispatch_prefetch_chunk`=:5882 has drifted to :5945 on this tree (post-`bcdb035` extraction churn).
- **D4** — the ~8-actor provider ceiling is folklore-grade (SERVICE_GRADE_ARCHITECTURE_PLAN.md:23) and the probe round that would verify it is quota-blocked (HARVESTAPI_PLAYBOOK.md:430). V9 therefore binds to the *configured* inflight (default 4), not the unverified ceiling (OQ3).
- **D5** — the "canonical hot path" today never uses R1.b–d at all: under simulate the window ladder emits 4×75 for a 300-url set (oracle pinned quirk, test :44–49/:599–625). The divider replaces the *composed* window+sizer surface, not just the R1 sizer; the oracle's twelve full plan goldens are the true baseline, not the R1 grid alone.

## 5. Simulate-first validation ladder

Every slice keeps the characterization oracle green until the single gated flip; the divider is additive throughout.

1. **S0 (landed)** — oracle green: 28 tests + 36 subtests pin the full current surface (this is the ruling-① prerequisite, DONE 2026-07-23).
2. **Shadow slices** (S1–S4, §7) — divider contract, model method, scripted client, shadow recording. The divider computes and records a division **on the activity surface only**; dispatched shapes still come from the ladder. Oracle stays byte-identical green; a new offline suite pins the division schema, validator battery, and every F1–F6 fallback audit shape (including "shadow division ≠ ladder division" divergence counters — free before/after evidence for the flip decision).
3. **Flip slice** (S5) — division applied to `dispatch_item_specs`; plan record gains `ai_batch_division` + `schema_version=2`. Oracle demotion per ruling ①, in the same commit: the twelve whole-dict plan goldens are rewritten into the validator battery (V1–V10 as invariant checks over both AI and fallback plans); the sizer/roster-split/coalesce/R5/R6 **unit** goldens survive unchanged because the ladder survives as the ruling-④ fallback. The oracle docstring's demotion note (test :25–32) is the contract for this rewrite. Review gate verdict required before this slice lands.
4. **Simulate e2e** — full-chain smoke under simulate with the scripted divider client (AI path) and without it (fallback path): both must reach dispatched/terminal states with coherent audit records; `refill_saturation`/`unfilled_available_slot_count` (enrichment.py:1253–1277) before/after comparison per ruling ②'s acceptance metrics.
5. **Live validation — explicitly deferred** to HarvestAPI quota restoration + explicit operator go (red lines unchanged: triple-gate env, committed `scripts/live_*.py` only, delta-only paid dispatch). First live wave doubles as the D4 ceiling probe if the operator approves that scope.

## 6. Open questions for the operator (the AskUserQuestion batch)

Mapping against the recon's 18-question list (recon §5): the four RATIFIED rulings settle **Q1** (ruling ① seat), **Q4** (ruling ① "rules demote to validators" shape), **Q6** (ruling ② wake semantics), **Q9** (ruling ③ two gates), **Q3+Q12** (ruling ④ both fallback directions). Of the remaining questions, **Q2, Q5, Q16, and the divider half of Q18 belong to this proposal** (Q7 is ruling ②'s metrics question but its answer is consumed here in §5.4; Q8/Q10/Q11 → ruling ③ doc; Q13–Q15 → behavior-layer doc; Q17 → compensation doc). Each restated as a decidable question:

- **OQ1 (= recon Q2) — divider input surface.** Does the divider consume, beyond ready URLs + source mix + worker budget: per-URL attempt/failure history, registry queue_state, actor pricing/charge-cap constants, priority? And is the input snapshot recorded for replay?
  **Recommended**: yes to all (as §2.2 — failure history and shard mix are exactly what a non-trivial regrouping needs; snapshot hash + bounded payload on the activity surface).
  *Alternatives*: (b) minimal inputs (urls+counts) — cheaper but the AI can't beat the ladder on anything the ladder can't already see; (c) full inputs but no snapshot recording — rejected: unauditable.
- **OQ2 (= recon Q5) — fate of contract ruling D1 (50/55 floor).** The 50-url actor-slot floor on tiny/legacy windows (enrichment.py:1371–1376; oracle :715–758) — retire at flip, subsumed by V4 tiny-legality?
  **Recommended**: retire at flip; V4 already forbids illegitimate sub-50 batches and the AI is free to pack ≥50 shapes.
  *Alternatives*: keep the 50-floor as an extra validator (V11) — safe but re-encodes the ladder inside the "AI-free zone", shrinking the win.
- **OQ3 (= recon Q16) — formal "1–2 rounds" definition + inflight basis.** Adopt `ceil(batch_count/actor_global_inflight) ≤ 2` as validator V9, bound to configured inflight (default 4), revisited only after a probe validates the 8 ceiling? Budget owner stays the plan stage (validator), not M2 provider runtime?
  **Recommended**: yes / configured-4 / plan-stage validator.
  *Alternatives*: (b) bind to the folklore 8 ceiling now — rejected: unverified (D4); (c) budget owner in M2 runtime — rejected: the runtime enforces inflight already; the *shape* constraint is a planning contract.
- **OQ4 (= recon Q18, divider residue) — oracle demotion mechanics at flip.** Accept §5.3: unit goldens survive (fallback ladder), whole-dict plan goldens → validator battery, new scripted-divider characterization for the AI path?
  **Recommended**: yes — keeps anti-regression teeth on both paths without freezing AI freedom.
  *Alternatives*: keep whole-dict goldens for the fallback path only (fallback stays byte-pinned) — stronger, costs golden maintenance on every additive record change; acceptable variant if the operator wants maximum fallback rigidity.
- **OQ5 (new) — divider engagement threshold.** Engage the AI divider only when the normal-wave ready set exceeds the single-envelope band (>300 urls, where the ladder starts multi-batch splitting and the 4–8 band is meaningful)?
  **Recommended**: yes, >300; ≤300 sets keep the ladder (a 4-batch minimum on a 60-url set would mint 4 illegal tiny batches).
  *Alternatives*: (b) engage ≥50 with `batch_count ∈ [1,8]` reinterpretation of ruling ① — needs an explicit operator re-reading of "4–8"; (c) always engage — rejected outright for retry/tiny waves (V8).
- **OQ6 (new) — invocation cadence + R6 extension.** Confirm: one model call per wave mint, `division_id` recorded per item, R6 inheritance extended to claim by division identity (§4.3), completion-driven refills never call the model (§4.5)?
  **Recommended**: yes — the only shape that preserves R6's invariant AND ruling ②'s <1 s target.
  *Alternatives*: re-divide on every tick — rejected: model cost per tick, churn against in-flight batches, direct R6 violation.
- **OQ7 (new) — scripted divider opt-in.** Offline AI-path testing via a scripted client behind a dedicated env (mirroring `SOURCING_SCRIPTED_LIVE_MODEL_PLANNING`, model_provider.py:23) — name `SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER`?
  **Recommended**: yes, dedicated flag (does not piggyback the planning flag — different blast radius).
  *Alternatives*: reuse the existing scripted-planning flag — fewer knobs, but couples two unrelated opt-ins.
- **OQ8 (new) — divider model routing + timeout.** Use the configured `ModelProviderSettings` model as-is (no per-call model pin, consistent with every other ModelClient method) with a divider-specific 20 s timeout override?
  **Recommended**: yes — model choice stays operator-owned in settings; only the timeout is divider-specific.
  *Alternatives*: dedicated divider model env — premature until live evidence shows the product model underperforms on this task.

## 7. Implementation slices + review-gate routing

Each slice is independently green; §7 protocol applies (implement + targeted tests + pin commit + record review request, then continue; NO-GO freezes only the affected scope's promotion).

| Slice | Content | Gate/oracle state | Review gate |
|---|---|---|---|
| S1 | Division contract module: schema `sourcing.profile_prefetch.ai_batch_division.v1`, strict parser, validator battery V1–V10 (pure functions + offline suite) | oracle untouched | request recorded (trigger 1: new contract schema) — verdict blocks nothing before S5 |
| S2 | `ModelClient.divide_profile_prefetch_batches`: Protocol + deterministic default + OpenAICompatible/Qwen impl + circuit wiring + scripted divider client (OQ7) | oracle untouched | rides S1's request (trigger 3: model behavior) |
| S3 | Shadow mode: divider called on wave mint, division/fallback audit recorded on the activity surface, divergence counters; dispatched shapes unchanged | oracle byte-identical green (hard check in CI) | none beyond S1/S2 (additive, no dispatched-shape change) |
| S4 | R6 `division_id` wave-identity extension (registry item fields + inheritance branch, §4.3) | oracle green (R6 unit goldens extended additively — new-variant tests, existing goldens unchanged) | **own review request** (trigger 1: registry contract field) |
| S5 | **Flip**: division applied to `dispatch_item_specs`; plan record + `ai_batch_division`, `schema_version=2`; oracle demoted per §5.3; simulate e2e both paths | oracle rewritten in-slice per ruling ① | **GO verdict required before landing** (trigger 5: claimed feature completion) + operator confirmation of OQ batch answers |
| S6 | Live validation wave (+ optional D4 ceiling probe) | n/a | operator explicit go + quota + live red lines |

Ordering: S1→S2→S3 may proceed immediately after the OQ ruling batch; S4 in parallel after S1; S5 strictly after S4's GO and the pinned review of the flip diff; S6 indefinitely deferred to quota + operator.
