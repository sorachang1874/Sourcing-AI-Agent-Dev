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
`luna_state_review.v1` binds exactly one hydrated Post's text; the candidate's
LinkedIn-sourced `professional_facts` are admissible as *evidence refs* in campaign
results but are **not part of the judged input** of any Luna review.

## 3. Design

### 3.1 Live Luna batch runner (new module `src/x_first/luna_batch_runner.py`)

- Input: the output of `build_luna_input_queue` (hydrated posts, exact bytes) plus the
  campaign manifest's per-candidate `professional_facts` (from `seed_inputs`).
- Execution: one `gpt-5.6-luna` Responses call per queue item via the existing canary
  transport shape (chshapi relay, strict JSON schema, exact returned-model check).
  Worker pool with no provider-side cap (default 16); result ordering restored by queue
  ordinal so downstream digests are deterministic.
- Output per item: `x.source_neutral.mapping.luna_state_review.v2` result, bound by an
  extended `build_luna_state_review_projection` and folded by the unchanged
  deterministic axis reducer (`luna_axis_reduction.v1` consumes v2 reviews
  identically — it reads only the two axis states + binding hashes).
- Every live call is receipt-bound, mirroring the canary v2 receipt family:
  approval receipt before the first provider-costing call, per-call execution receipt
  (route/model/payload/timing/HTTP-or-failure), retention + deletion journal/receipt.
  No receipt, no adjudication.

### 3.2 Judged-input extension (`luna_state_review.v2`, additive)

- New optional field on the review result: `judged_seed_facts_sha256` — canonical-JSON
  SHA-256 of the candidate's `professional_facts` array as judged; the runner's prompt
  presents the hydrated Post text **and** the seed facts (LinkedIn-sourced headline /
  affiliation / role / education / project facts with their temporal states).
- The projection builder recomputes and binds this hash exactly like
  `source_text_sha256`. Reviews without seed facts (v1 shape) remain valid;
  `judged_seed_facts_sha256` is then absent and the reducer treats the row unchanged.
- Rationale: direction experience is often stated in the LinkedIn profile itself
  (e.g. "Pretraining @ Thinking Machines Lab") while posts/replies provide
  corroborating current/historical evidence. The axis states already model
  `current|historical|ambiguous|unsupported`; the extension only widens what the judge
  may cite, without changing the state vocabulary or the reducer.

### 3.3 Prompt (new versioned prompt, closed output)

- Output schema per item: `{proposed_lab_affiliation_state, proposed_pretraining_experience_state}`
  over the closed enum `current|historical|ambiguous|unsupported` — identical to
  `luna_state_review.v1` so the reducer and contracts stay stable.
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
3. Grok waves resolve X accounts and collect candidate-authored posts/replies
   (replaces the ad-hoc per-person CLI loop; 16-way worker concurrency).
4. Hydration binds exact post bytes; the Luna batch runner (this design) judges each
   queue item with seed facts in scope; the deterministic reducer folds per-candidate
   axis states with coverage accounting.
5. Results flow back through the existing portable adapter pins
   (`src/sourcing_agent/x_first_portable_adapter.py`); the CRM export CSV's X-First
   columns become `dimension_result.relevance_state`,
   `target_activity_temporal_state`, and evidence refs — replacing the ad-hoc boolean
   used in the first TML pass.

## 5. Impact surface and gates

- New: `luna_batch_runner.py` + prompt config + output schema + receipt contracts +
  fixtures + tests; additive `luna_state_review.v2` schema; reducer accepts v2.
- Unchanged pins: bio_semantic v2.2 prompt/schema/proxy-policy SHAs; reported-text lane;
  portable request/result schema SHAs already pinned in sourcing-ai-agent
  (`x_first_portable_package.py` / `x_first_portable_adapter.py`) — the seed/result
  contracts are sufficient as-is, so no cross-repo re-pin is needed for this lane.
- Gates: pinned independent review (GO) before the first live/provider-costing runner
  call; canary-style approval receipt with explicit cost ceiling per batch.
