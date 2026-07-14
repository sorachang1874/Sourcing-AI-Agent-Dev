# Luna-native profile Bio semantic review v2.1

> Status: offline, fixture-only, advisory proposal contract. No OpenAI-compatible endpoint, Luna model, X profile,
> credential, live provider, fallback, product writer, discovery/ranking path, or outreach path was called in this
> slice. A future model canary requires a separate owner decision and independent review.

## Outcome and boundary

V2 makes semantic review model-native without making model prose authoritative. `gpt-5.6-luna` receives one exact,
synthetic `.invalid` profile Bio through an OpenAI-compatible Responses request with strict `json_schema` output. A
deterministic validator then recomputes every source span and hash and binds the result to the exact request, profile
snapshot, stable external platform user ID, Bio, model, reasoning effort, prompt, output schema, response, and usage.

The result is only an `unverified_professional_context_proposal`. It cannot:

- add someone to discovery, rank or filter a candidate, decide eligibility, or authorize outreach;
- infer real name, ethnicity, nationality, race, citizenship, religion, gender, ancestry, or protected identity;
- confirm employment, merge identities, establish external facts, or write canonical/product state;
- call another model, tool, web search, X search, or fallback transport.

The deterministic v1.4 rule lane remains a champion/guardrail baseline for synthetic comparison. It is not the v2
primary recall classifier and must not grow into an alias/marker registry that duplicates semantic review.

## Source trust and model execution are separate

The request deliberately has two fields:

| Field | Closed v2 values | Owner and meaning |
| --- | --- | --- |
| `profile_source_mode` | only `offline_fixture` | Profile-capability owner; accepts only a handle-bound `.invalid` URL and exact UTF-8 Bio snapshot/hash |
| `model_execution_mode` | `offline_fake`, `live_canary` | Model-transport owner; selects the injected fake or a future explicitly approved live transport |

`live_canary` does not mean the Bio came from X. V2 cannot accept `x.com` or claim native profile provenance. A future
profile-capability contract must establish stable user ID, Bio value/hash, observation, call and source receipt before
native data enters this review lane.

Any transport declaring `is_live=true` is blocked before invocation unless `execute_live=true`. Even with that flag,
the request must say `model_execution_mode=live_canary`; the profile remains `offline_fixture`. This slice provides no
HTTP relay or credential loader, and its tests never exercise a provider call.

Invocation is closed to two exact tuples: `offline_fake + offline_fake_responses + execute_live=false +
model_execution_mode=offline_fake`, or `live + openai_compatible_responses + execute_live=true +
model_execution_mode=live_canary`. A valid live tuple without the explicit execution flag is the sole blocked form.
Every other flag, transport, or mode combination fails before the injected transport is called.

## Contract stack and ownership

| Artifact | Source of truth | What it owns | What it cannot own |
| --- | --- | --- | --- |
| `x.profile.bio_semantic.request.v2.1` | Caller plus source snapshot | Exact subject/account/snapshot/Bio binding, model policy, authority and budgets | Provider result or semantic verdict |
| `x.profile.bio_semantic.prompt.v2.1` | Versioned prompt config | Closed task instruction and safety boundary | Runtime override or fallback |
| `x.profile.bio_semantic.model_output.v2.1` | Strict Responses `text.format` schema | Untrusted verdict, proposals, display-only narrative and exact spans | External facts or downstream authority |
| `x.profile.bio_semantic.review.v2.1` | Deterministic validator | Recomputed spans/hashes/IDs, response and usage binding, every terminal result | Canonical employment, ranking, eligibility or writes |
| v1.4 rule lane | Champion/guardrail fixture baseline | Regression comparison and obvious safety guards | Primary semantic recall classification |

The prompt and strict output schema are canonical-SHA-pinned. The request binds both digests, `gpt-5.6-luna`,
`reasoning_effort=low`, `strict_structured_output=true`, and `fallback_model=null`. The Responses payload repeats those
bindings, sends `tools=[]`, uses `store=false`, and sets `truncation=disabled`.

## Proposal semantics

Four proposal types remain separate:

1. `professional_region_experience` — explicit work, study, research or residence context in a physical region.
2. `professional_china_digital_ecosystem` — explicit professional publishing/account/activity context in a China
   digital ecosystem. It is not physical-region evidence.
3. `professional_affiliation` — a subject-authored organization relation with reversible
   `current_claimed|previous_claimed|future_or_aspirational|unspecified` state. It is not confirmed employment.
4. `observed_chinese_professional_content` — weak, directly observed Chinese-language professional content. It is not
   region experience, fluency, nationality, ethnicity, identity, or a discovery/ranking signal.

`verdict=proposals_available` mechanically requires at least one proposal. `no_supported_professional_context` and
`abstained` require none. Non-affiliation types use `relation_state=not_applicable`; affiliation cannot use it.

Every proposal includes:

- Unicode code-point `span_start`/`span_end` and an exact Bio `excerpt`;
- exactly one closed `reason_code` owned by the `(proposal_type, relation_state)` pair;
- a bounded human-readable `reason` and `reason_source=model_proposed_untrusted_narrative`;
- `evidence_basis=profile_bio_only`, a confidence label, and mandatory independent verification.

The validator re-slices the Bio, recomputes excerpt SHA-256 and deterministic proposal ID, and rejects duplicate or
misbound proposals. Semantic identity and duplicate detection use only type, relation, exact span/excerpt and the one
reason code; confidence and narrative cannot create another proposal. Reasons reject URLs, protected-identity claims,
external-fact markers, and `@handles` or Arabic/Chinese numeric claims absent from the cited excerpt. Natural wording
is deliberately not treated as deterministically fact-complete: the closed reason code and exact source span own the
machine-readable judgment. `reason` remains untrusted display-only prose and cannot add facts, evidence or authority.

## Responses and terminal behavior

The builder follows the Responses structured-output shape:

```text
model = gpt-5.6-luna
reasoning.effort = low
text.format.type = json_schema
text.format.strict = true
text.format.schema = closed model-output schema
tools = []
store = false
```

The injected transport must return exactly one completed assistant message with one `output_text`, the exact model ID,
one response ID, and integer input/output/total usage that reconciles. Because a reasoning model may also emit a
provider `reasoning` item, the parser permits at most one bounded, ID-bearing completed reasoning item alongside that
one message; it never adopts or exposes the reasoning content. Every tool/call/unknown item, a second message or
reasoning item, refusal, reroute, missing usage, duplicate JSON key, non-finite number, invalid JSON, schema drift,
budget overrun or source mismatch fails closed. There is no retry or fallback.

Closed terminal review states are `completed`, `blocked`, and `failed`. The review schema has status-conditional
receipt, usage, proposal, error and execution shapes, and the validator recomputes all three states rather than merely
accepting a plausible envelope. A valid request always preserves its exact
numeric platform user ID in every terminal artifact. An object without enough valid request/profile identity to bind
an artifact raises the typed `request_binding_invalid` boundary and produces no review artifact; it never writes an
`unknown` or invented identity.

Hard budgets are versioned as one call, 12 proposals, 12,000 model-output characters, 4,000 input tokens, 1,600 output
tokens, 5,600 total tokens, 30 seconds, 64 validation levels, 4,096 JSON nodes, 12,000 characters/48,000 UTF-8 bytes
per JSON string, 262,144 canonical response bytes, and a reasoning summary capped at four items, 2,000 characters and
8,000 UTF-8 bytes. Full-tree key/value Unicode and leaf budgets run before canonical hashing; this includes ignored
provider metadata and intermediate request fields. Unpaired surrogates, 1,500-level objects, 5,000-node values and
oversized reasoning/diagnostic leaves return bounded terminal failures without traceback or content echo.

## Fixture evaluation

The offline fake transport covers eight synthetic Bios:

| Synthetic pattern | Expected semantic output |
| --- | --- |
| explicit Singapore model-training work | strong physical-region professional proposal |
| named Xiaohongshu professional account/activity | China digital-ecosystem proposal |
| named WeChat public account | China digital-ecosystem proposal |
| `Head of ... @org` | current claimed affiliation |
| `Prev @org` | previous claimed affiliation |
| `Future Researcher at @org` | future/aspirational affiliation, never current |
| Chinese AI-engineering content | weak observed-language professional context only |
| generic AI systems text | no supported proposal |

Future Luna canary evaluation compares semantic v2 against the frozen v1.4 baseline and human labels. Report per-type
precision/recall, abstention, source-span validity, reason support, review minutes, latency and usage. The weak-language
type is reported separately and cannot contribute to discovery/ranking/eligibility. A model challenger may improve
recall only while identity/source/authority/write/fallback guardrails remain zero.

## Validation

From `x-first-researcher-sourcing/`:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m unittest tests.test_profile_bio_semantic_v2 -v
../sourcing-ai-agent/.venv/bin/ruff check \
  src/x_first/profile_bio_semantic_v2.py tests/test_profile_bio_semantic_v2.py
```

The request shape follows the official [Responses API](https://platform.openai.com/docs/api-reference/responses) and
[Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs) contracts; the official
[GPT-5.6 Luna model page](https://developers.openai.com/api/docs/models/gpt-5.6-luna) currently lists Responses,
structured outputs and `low` reasoning support. The explicit model choice remains owner-pinned; the offline fixture
does not prove account entitlement, endpoint compatibility, latency, quality or cost for a future live canary.
