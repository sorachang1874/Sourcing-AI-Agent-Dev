# Luna live batch runner + LinkedIn seed facts — design

> Status: design proposal. No contract, schema, prompt, fixture, or runtime change is
> authorized by this document. Any implementation of this lane is contract-heavy and
> requires the pinned independent review gate before the first live/provider-costing call.

## 1. Grounded current state

This design was written after mapping the existing lanes; it deliberately reuses them
instead of inventing parallel ones.

- **Population boundary** (README, "Population boundary"): two independent dimensions per
  person — target-lab affiliation `current|historical` and pretraining experience
  `current|historical`. All four combinations stay in the recall pool.
- **LinkedIn content already has a native input contract.**
  `contracts/x.portable.research_campaign.request.v1.schema.json` `$defs.seed_input`:
  `source_kind` enum includes `linkedin_profile`; `professional_facts[]` carries typed
  `fact_type` (`affiliation|role|education|project|location|other`) × `temporal_state`
  (`current|historical|ambiguous|not_applicable`) + `evidence_ref`, with
  `source_status` (`source_bound|model_mediated_unverified|human_supplied_unverified|fixture_synthetic`).
  No new LinkedIn evidence source needs to be invented.
- **The direction-judgment artifact already exists.**
  `x.portable.research_campaign.result.v1` `$defs.dimension_result`:
  `relevance_state` (`target_core|target_adjacent|out_of_scope|ambiguous`) +
  `target_activity_temporal_state` (`current|historical|...`) + `evidence_refs`.
  `src/x_first/research_orchestration.py:1195` admits `professional_facts` evidence refs
  into the allowed evidence set, and lines 1417–1468 validate caller-supplied
  dimension/affiliation results (evidence-ref closure, temporal-filter consistency,
  source-status compatibility). The module is a validator/replayer — it does not call
  Grok/Luna/X/LinkedIn (module docstring, line 5).
- **The per-post Luna state-review lane already exists.**
  `src/x_first/source_neutral_mapping.py`:
  - `build_luna_input_queue` (line 1552) yields the hydrated-post review queue;
  - `build_luna_state_review_projection` (line 1287) schema-validates a caller-supplied
    `x.source_neutral.mapping.luna_state_review.v1` result and binds it to the exact
    hydrated Post bytes (`candidate_ref`, `stable_post_id`, `source_text_sha256`);
  - `_build_luna_axis_reduction` (line 1368) deterministically folds per-post proposals
    into per-candidate-per-axis (`lab_affiliation|pretraining_experience`) rows with
    coverage accounting; `authorized_transition_count` is const `0` — Luna output is
    `diagnostic_only_unattested` and cannot authorize state transitions.
  - Reviewed states per axis: `current|historical|ambiguous|unsupported`.
- **The only live Luna transport is a single-input canary.**
  `src/x_first/luna_live_canary_v2.py` + `src/x_first/profile_bio_semantic_v2.py`:
  chshapi relay `https://api.chshapi.org/v1/responses`, model `gpt-5.6-luna`, strict
  JSON-schema output, `Authorization: Bearer $CHSHAPI_API_KEY`. It reviews one verified
  X Bio snapshot (bio_semantic v2.2; China/Asia proxy + affiliation vocabulary).
- **`reported_profile_text_semantic.py`** is an offline deterministic adjudicator over
  model-mediated unverified texts (China/Asia lane only, evidence basis
  `model_mediated_unverified_text_only`); its contract doc states a live batch runner
  "is not implemented".

## 2. The real gap

Two lanes can *validate and bind* Luna judgments, but **nothing executes Luna over a
queue**: every lane consumes caller-supplied results. Separately, the judged input of
`luna_state_review.v1` binds exactly one hydrated Post's text; the operator directive
(2026-07-19) requires the judge to see the **full per-candidate context** — LinkedIn
profile facts + Grok-returned Bio + all collected Posts/Replies — in one judgment.

## 3. Design

### 3.0 Stage I/O contracts (operator-pinned 2026-07-19)

The two AI stages have explicit, operator-pinned input/output shapes. Rich context is
a correctness requirement, not a tuning knob: account resolution without profile
context mis-resolves people, and direction judgment without the full bundle loses
recall on exactly the evidence that decides the axes.

**Grok stage (per candidate, one or more CLI calls):**

- *Input*: the candidate's LinkedIn profile context from `seed_inputs` —
  `name_text`, headline/role facts, current and past affiliations with their
  `temporal_state`, location, education. The prompt MUST anchor identity on these
  facts (name + current lab + past labs) so the resolved account is the same person,
  not a namesake.
- *Output*: a per-candidate bundle —
  `account_resolution` (handle, resolution confidence, evidence receipts),
  `x_bio` (exact reported text), and `items[]`: candidate-authored Posts/Replies
  relevant to the target direction (pre-training), each with id/URL/timestamp/text
  and a retrieval receipt. Bundle items keep their own trust labels.

**Luna stage (per candidate, exactly one call):**

- *Input*: the complete bundle — LinkedIn `professional_facts` + Grok-returned
  `x_bio` + every collected Post/Reply text. Nothing is pre-filtered by the caller;
  the judge sees all of it and decides what is evidence.
- *Output*: `{proposed_lab_affiliation_state, proposed_pretraining_experience_state}`
  over `current|historical|ambiguous|unsupported`, plus per-axis `evidence_citations[]`
  naming the judged bundle items (`seed_fact:<ref>`, `x_bio`, `post:<stable_post_id>`)
  that carry the judgment. The judge abstains (`ambiguous`) when the bundle is thin;
  `unsupported` when the bundle is rich and silent.

### 3.1 Live Grok collection runner (per-candidate bundle producer)

- Driver: one worker per candidate over the campaign `seed_inputs`; each worker
  builds the identity-anchored prompt (§3.0), runs `grok -p ... --output-format json`
  headless (the only CLI mode that works in agent shells; interactive TUI fails with
  `os error 6` outside a WindowServer session), and validates the structured output
  against the bundle shape.
- Concurrency: aggressive. No provider-side worker cap is assumed for the Grok CLI
  or the Luna relay (unlike HarvestAPI's conservative 4-8 slot policy); default
  pool 16, tunable upward; per-item receipts isolate failures and make retries
  cheap. Ordering is restored by seed ordinal so downstream digests stay
  deterministic.
- Receipts: per-call operator receipt (prompt hash, session id, elapsed, cost),
  folded into the campaign's handle-resolution/observation evidence contracts.

### 3.2 Live Luna batch runner (new module `src/x_first/luna_batch_runner.py`)

- Review unit: **one candidate, one call** (not one Post). The judged input is the
  §3.0 bundle. This replaces the earlier per-Post v2 sketch: the operator directive
  requires the judge to see profile + bio + posts together, and one call per person
  is strictly cheaper and simpler at campaign scale.
- New contract `x.source_neutral.mapping.luna_candidate_review.v1`:
  `candidate_ref`, `judged_bundle_manifest` (ordered item refs + per-item
  `source_kind`/`source_status`/`sha256`), `judged_bundle_sha256`, the two axis
  states, per-axis evidence citations, `authority_status:
  diagnostic_only_unattested`, `model_claim_scope: state_proposal_only`.
- A thin deterministic adapter folds each candidate review into the unchanged
  `luna_axis_reduction.v1` rows: per axis, `reviewed_evidence_count` = bundle item
  count, `reviewed_evidence_manifest_sha256` = `judged_bundle_sha256`,
  `diagnostic_proposed_state` = the axis state, coverage `complete` for judged
  candidates. The reducer itself is not modified.
- Execution: `gpt-5.6-luna` Responses call via the existing canary transport shape
  (chshapi relay, strict JSON schema, exact returned-model check). Aggressive
  concurrency as in §3.1 (default 16 workers, no provider-side cap assumed).
- Every live call is receipt-bound, mirroring the canary v2 receipt family:
  approval receipt before the first provider-costing call, per-call execution receipt
  (route/model/payload/timing/HTTP-or-failure), retention + deletion journal/receipt.
  No receipt, no adjudication.

### 3.3 Prompt (new versioned prompt, closed output)

- Output schema per candidate: the two axis states over
  `current|historical|ambiguous|unsupported` plus per-axis `evidence_citations[]`
  bound to judged bundle item refs — the axis enum is identical to
  `luna_state_review.v1` so the reducer stays stable.
- Identity anchoring: the prompt presents the seed facts as *identity context* and
  instructs the judge to first confirm the X account/bio/posts plausibly belong to
  the same person (name + current lab + past labs); a mismatch yields
  `ambiguous` on both axes with a citation, never a silently wrong-person judgment.
- Same boundaries as the bio-semantic v2.2 prompt: judge only the supplied texts, no
  external facts, no protected-identity inference, no eligibility/ranking/outreach,
  abstain on ambiguity. Prompt version + canonical-JSON SHA-256 pinned in module
  constants, validated exactly like `validate_prompt` (bio_semantic v2 line 891).

### 3.4 Trust classes

- LinkedIn `professional_facts` produced by sourcing-ai-agent's Harvest fetch are
  `source_bound` (provider-API receipt in the company-asset snapshot).
- Grok-mediated reported text stays `model_mediated_unverified`; hydrated posts with
  raw-session replay are source-bound at the projection boundary.
- The runner never upgrades trust: Luna output remains `diagnostic_only_unattested`;
  `authorized_transition_count` stays `0`. A later authority gate is out of scope here.

### 3.5 Explicit non-goals

- No v3 of `profile_bio_semantic_v2` (verified-Bio lane is unchanged).
- No change to the China/Asia proxy vocabulary, proxy policy, or
  `reported_profile_text_semantic` (that lane keeps its own trust basis).
- No ranking, eligibility, outreach, or canonical-write authority.

## 4. TML product-path mapping (integration with sourcing-ai-agent)

1. sourcing-ai-agent exports Layer 1-3 candidates as `seed_inputs`:
   `source_kind=linkedin_profile`, `source_profile_url`, `name_text`,
   `professional_facts` = current TML affiliation + role + education + location facts,
   each with `evidence_ref` = company-asset snapshot artifact hash,
   `source_status=source_bound`.
2. Portable campaign request: `analysis_questions` = pretraining dimension,
   `temporal_scope` = affiliation + target activity `current`+`historical`.
3. Grok waves resolve X accounts and collect candidate-authored posts/replies with
   the LinkedIn profile as identity anchor (§3.0/§3.1; 16-way worker concurrency).
4. Hydration binds exact post bytes; the Luna batch runner (this design) judges each
   **candidate bundle** (profile facts + bio + all posts/replies) in one call; the
   deterministic adapter + reducer fold per-candidate axis states with coverage
   accounting.
5. Results flow back through the existing portable adapter pins
   (`src/sourcing_agent/x_first_portable_adapter.py`); the CRM export CSV's X-First
   columns become `dimension_result.relevance_state`,
   `target_activity_temporal_state`, and evidence refs — replacing the ad-hoc boolean
   used in the first TML pass.

## 5. Impact surface and gates

- New: `luna_batch_runner.py` + Grok collection runner + prompt config +
  `luna_candidate_review.v1` contract + receipt contracts + fixtures + tests;
  a deterministic candidate-review → axis-row adapter feeding the unchanged
  `luna_axis_reduction.v1` reducer.
- Unchanged pins: bio_semantic v2.2 prompt/schema/proxy-policy SHAs; reported-text lane;
  portable request/result schema SHAs already pinned in sourcing-ai-agent
  (`x_first_portable_package.py` / `x_first_portable_adapter.py`) — the seed/result
  contracts are sufficient as-is, so no cross-repo re-pin is needed for this lane.
- Gates: pinned independent review (GO) before the first provider-costing runner
  call is **not** used to block implementation of the lane itself (operator
  directive 2026-07-19: review must not stall development); the GO edge remains
  required before any *promotion* of runner output into product paths. The TML
  Layer 1-3 live test below is an explicitly exploratory run whose outputs stay
  diagnostic-only.

## 6. Live test plan (TML Layer 1-3, 67 candidates)

1. Seeds: the 67 Layer 1-3 TML candidates from sourcing-ai-agent
   (`layered_outreach_232.json` joined to snapshot `20260719T183049`
   `candidate_documents.json`), mapped to `seed_inputs` with
   `source_kind=linkedin_profile`, current-TML affiliation + role/education/location
   facts, `evidence_ref` = snapshot artifact hash, `source_status=source_bound`.
2. Grok stage: 16-way worker pool, identity-anchored prompts (§3.0/§3.1); per-person
   bundle JSON + operator receipts.
3. Luna stage: 16-way pool, one `gpt-5.6-luna` call per candidate with the full
   bundle; per-axis states + citations + execution receipts.
4. Comparison baseline: the earlier ad-hoc 67-person pass (53 accounts, 16
   pre-train) is replayed as a smoke oracle — the structured lane must reproduce
   account resolutions it got right, and its per-axis states are diffed against the
   ad-hoc booleans with every divergence traceable to a cited bundle item.
5. Success measures: per-candidate wall time, account-resolution rate, citation
   coverage (every non-`unsupported` axis has ≥1 citation), wrong-person detections
   (identity-anchor mismatch count), and cost per candidate; all reported honestly,
   no target quota.
