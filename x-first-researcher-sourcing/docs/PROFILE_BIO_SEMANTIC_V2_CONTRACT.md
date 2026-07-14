# Luna-native profile Bio semantic review v2.2

> Status: offline, fixture-only, advisory proposal contract. No OpenAI-compatible endpoint, Luna model, X profile,
> credential, live provider, fallback, product writer, discovery/ranking path, or outreach path was called in this
> slice. A future model canary requires a separate owner decision and independent review.

## Outcome and boundary

V2.2 makes semantic review model-native without preserving model-authored explanatory prose. `gpt-5.6-luna` receives one exact,
synthetic `.invalid` profile Bio through an OpenAI-compatible Responses request with strict `json_schema` output. A
deterministic validator then recomputes every source span and hash and binds the result to the exact request, profile
snapshot, stable external platform user ID, Bio, model, reasoning effort, prompt, output schema, response, and usage.

The result is only an `unverified_professional_context_proposal`. It cannot:

- add someone to discovery, rank or filter a candidate, decide eligibility, or authorize outreach;
- infer real name, ethnicity, nationality, race, citizenship, religion, gender, ancestry, or protected identity;
- confirm employment, merge identities, establish external facts, or write canonical/product state;
- call another model, tool, web search, X search, or fallback transport.

The semantic request is source-neutral and does not bind any rule-lane champion. A future comparative evaluation must
use a separate independently accepted evaluation manifest that pins its champion version, implementation hash and
fixture digest. The current v1.4 rule lane is not named or implied as an accepted/frozen baseline by this contract.

## Source trust and model execution are separate

The request deliberately has two fields:

| Field | Closed v2 values | Owner and meaning |
| --- | --- | --- |
| `profile_source_mode` | only `offline_fixture` | Profile-capability owner; accepts only a handle-bound `.invalid` URL and exact UTF-8 Bio snapshot/hash |
| `model_execution_mode` | `offline_fake`, `live_canary` | Model-transport owner; selects the injected fake or a future explicitly approved live transport |

`live_canary` does not mean the Bio came from X. V2 cannot accept `x.com` or claim native profile provenance. A future
profile-capability contract must establish stable user ID, Bio value/hash, observation, call and source receipt before
native data enters this review lane.

The fixture URL has one canonical form: `https://profiles.invalid/x/{lowercase_handle}`. The schema rejects root
`invalid`, subdomains, uppercase host/handle, query, fragment and trailing slash; runtime additionally requires the
path handle to equal `current_handle` byte-for-byte. This is fixture provenance syntax, not evidence of an X account.

Any transport declaring `is_live=true` is blocked before invocation unless `execute_live` is the exact boolean `true`.
Truthy strings, integers, nulls, invalid `is_live` values, invalid transport IDs and invalid transport pairs terminate
as `execution_contract_invalid` with zero calls. Even with an exact flag,
the request must say `model_execution_mode=live_canary`; the profile remains `offline_fixture`. This slice provides no
HTTP relay or credential loader, and its tests never exercise a provider call.

Invocation is closed to two exact tuples: `offline_fake + offline_fake_responses + execute_live=false +
model_execution_mode=offline_fake`, or `live + openai_compatible_responses + execute_live=true +
model_execution_mode=live_canary`. A valid live tuple without the explicit execution flag is the sole blocked form.
Every other flag, transport, or mode combination fails before the injected transport is called.

Before any `is_live` or `transport_id` getter runs, runtime canonical-snapshots the request, prompt, output schema and
resolved proxy policy into plain JSON and performs asset preflight only from those snapshots. Asset-preflight failures
therefore use `mode=not_inspected`, `transport_id=not_inspected`, and `contract_error=not_evaluated`; a changed policy
remains `proxy_policy_invalid` even if the transport has invalid or hostile metadata getters. After
`create_response` returns, its value is immediately canonical-snapshotted into plain JSON before response parsing.
The validator follows the same rule for review, request, prompt, schema, policy and raw response before it may inspect
transport metadata, so a getter cannot rewrite validator source objects.

Receipt-owning callers use `adjudicate_observed_response(...)`, never private result builders. Its version is
`x.profile.bio_semantic.pure_adjudication.v1`; its manifest digest covers all semantic schema/prompt/policy versions plus
the transitive implementation reachable from that public function. The implementation bundle records a stable digest
of each controlled function/class code object and a type-explicit manifest of every referenced data or external
callable dependency. This is a **trusted-runtime drift signal**, not tamper-proof integrity and not an independent trust
root: the digest checker and expected value live in the same Python module and can be monkey-patched together. Live
execution authority instead belongs to the Luna bundle validator, which validates durable approval and observed HTTP
receipts outside the semantic review payload. The pure API accepts an execution projection and raw response only to
replay the deterministic review shape; that projection does not prove that a call occurred or that usage was billable.
The API has no transport object, `is_live` inference, network call, retry or fallback capability.

## Contract stack and ownership

| Artifact | Source of truth | What it owns | What it cannot own |
| --- | --- | --- | --- |
| `x.profile.bio_semantic.request.v2.2` | Caller plus source snapshot | Exact subject/account/snapshot/Bio binding, model policy, authority and budgets | Provider result, semantic verdict or comparative champion |
| `x.profile.bio_semantic.prompt.v2.2` | Versioned prompt config | Closed task instruction and safety boundary | Runtime override or fallback |
| `x.profile.bio_semantic.model_output.v2.2` | Strict Responses `text.format` schema | Untrusted verdict, proposals, closed explanation template and exact spans | External facts or downstream authority |
| `x.profile.bio.professional_experience_proxy_policy.v1` | Versioned policy config | Proposal-type-to-region proxy mapping, strength order and downstream usage boundary | Semantic truth, physical region, identity, eligibility or ranking |
| `x.profile.bio_semantic.pure_adjudication.v1` | Semantic runtime | Pure deterministic projection from caller-supplied source objects and raw response | Proof of provider execution, billing, receipt independence or network execution |
| `x.profile.bio_semantic.review.v2.2` | Deterministic validator | Recomputed spans/hashes/IDs and offline non-billable semantic replay | Live call/billing authority, canonical employment, ranking, eligibility or writes |
| `x.profile.bio_semantic.live_canary.*.v2` | Luna runner plus durable approval/execution ledgers | Observed route, call count, response, billing projection, approval and artifact provenance | Semantic correctness, product writes or downstream authority |
| future evaluation manifest | Evaluation owner plus independent review | Accepted champion version/hash/fixture digest and comparison policy | Semantic request or provider execution |

The prompt, strict output schema and professional-experience proxy policy are canonical-SHA-pinned. The request binds
all three version/digest pairs, `gpt-5.6-luna`,
`reasoning_effort=low`, `strict_structured_output=true`, and `fallback_model=null`. The Responses payload repeats those
bindings, sends `tools=[]`, uses `store=false`, and sets `truncation=disabled`. Every terminal review repeats the exact
expected proxy-policy version/SHA plus the actually observed version/SHA and
`validation_status=validated_canonical|invalid`. An absent, malformed or changed policy fails in preflight as
`proxy_policy_invalid`; its review artifact never presents the expected canonical hash as if it were observed. The
review schema binds `validated_canonical` to the exact expected observation and requires every `invalid` observation to
differ, so provenance cannot be relabeled by a schema-valid artifact.

## Proposal semantics

Four proposal types remain separate:

1. `professional_region_experience` — explicit work, study, research or residence context in a physical region.
2. `professional_china_digital_ecosystem` — explicit professional publishing/account/activity context in a China
   digital ecosystem. It is not physical-region evidence.
3. `professional_affiliation` — a subject-authored organization relation with reversible
   `current_claimed|previous_claimed|future_or_aspirational|unspecified` state. It is not confirmed employment.
4. `observed_chinese_professional_content` — weak, directly observed Chinese-language professional content. It is not
   physical-region experience, fluency, nationality, ethnicity or identity; it only feeds the deterministic weak
   professional-experience proxy described below.

The external versioned proxy policy owns the mapping used to build a separate
`professional_experience_proxy_rollup` for high-recall verification input:

| Source-bound proposal | China proxy | Asia proxy | Boundary |
| --- | --- | --- | --- |
| subject-owned China digital-ecosystem professional publishing/account/activity | `strong_proxy` | `strong_proxy` | professional-experience proxy only |
| Chinese-language professional or technical content | `weak_proxy` | `weak_proxy` | professional-experience proxy only |
| nonprofessional Chinese-language interest/consumption | `none` | `none` | no proposal |
| explicit physical-region experience | `none` from this roll-up | `none` from this roll-up | remains explicit source-span evidence, never a proxy |

The roll-up has `status=unverified_model_derived|none`. Each China/Asia result carries its strength and a deterministic
list of contributing proposal IDs, types, confidence values and mapped strengths. Strength is the maximum declared by
the pinned policy; unmapped and terminal/empty results remain `none` with empty provenance. Runtime reads the mapping
from that policy only—there is no second proposal-type mapping in code or prompt.

The deterministic validator proves structural closure, exact source spans, hashes, policy application and provenance;
it does **not** prove that the model's semantic proposal is correct. A non-`none` roll-up may feed only a separately
governed high-recall **verification queue**. It cannot be final eligibility, ranking, filtering, outreach, physical
region evidence or canonical employment. Physical-region proposals remain a separate explicit-span type. Protected
identity inference remains prohibited and has no field or mapping in this contract.

All exact JSON contracts use type-strict equality. In particular, Python-equivalent values such as `false` and `0`,
`true` and `1`, or integer `1` and number `1.0` are different. Budget/token/span fields require non-boolean integers;
authority, verification, billable, execution and fallback fields require exact booleans. The executable schema helper
uses the same type-strict rule for `const` and `enum`.

`verdict=proposals_available` mechanically requires at least one proposal. `no_supported_professional_context` and
`abstained` require none. Non-affiliation types use `relation_state=not_applicable`; affiliation cannot use it.

Every proposal includes:

- Unicode code-point `span_start`/`span_end` and an exact Bio `excerpt`;
- exactly one closed `reason_code` owned by the `(proposal_type, relation_state)` pair;
- the exact deterministic explanation owned by `(proposal_type, relation_state)` and
  `reason_source=closed_deterministic_explanation_template_v2.2`;
- `evidence_basis=profile_bio_only`, a confidence label, and mandatory independent verification.

The validator re-slices the Bio, recomputes excerpt SHA-256 and deterministic proposal ID, and rejects duplicate or
misbound proposals. Semantic identity and duplicate detection use only type, relation, exact span/excerpt and the one
reason code; confidence and the deterministic explanation cannot create another proposal. Open narrative is rejected:
the only valid `reason` is the closed template for the proposal type/relation pair. Therefore invented organizations,
locations, protected-identity claims and natural paraphrases cannot survive in the review artifact. The closed reason
code and exact source span remain the machine-readable judgment.

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
provider `reasoning` item, the parser permits at most one bounded, ID-bearing reasoning item alongside that one
message; `status` and `summary` may be absent, but when present must be closed and bounded. `output_text.logprobs` may
be absent, null or empty, and message `phase` may be absent or `final_answer`; none is adopted or exposed. Every
tool/call/refusal/unknown item, unknown nested field, a second message or
reasoning item, refusal, reroute, missing usage, duplicate JSON key, non-finite number, invalid JSON, schema drift,
budget overrun or source mismatch fails closed. There is no retry or fallback.

Closed terminal review states are `completed`, `blocked`, and `failed`. The review's execution projection records a closed
`preflight|authorization|transport|response|semantic|completed` phase, transport invocation count, external call
count and response presence. Those fields make a replay total; they are not independent evidence. The review schema
has status-conditional receipt, usage, proposal, error and execution shapes. Asset-preflight artifacts are recomputed
without consulting a transport; later offline blocked/failed artifacts additionally require the original transport
declaration and exact execute flag. The validator never accepts an `execution_attempt` argument as evidence, including
`review["execution"]` or a deep copy of it: JSON placement cannot establish independence. A completed offline review is
recomputed from one fixed non-billable fake-transport projection. Any completed `live_canary` review fails closed at
this semantic API and must be validated by the outer Luna artifact validator, which binds the durable approval ledger,
observed HTTP execution receipt, retained response and result hashes before deterministic semantic replay. Preflight
and blocked states require zero calls and no raw response; the outer live contract requires one external model call;
offline external calls are always zero. A valid request always preserves its exact
numeric platform user ID in every terminal artifact. An object without enough valid request/profile identity to bind
an artifact raises the typed `request_binding_invalid` boundary and produces no review artifact; it never writes an
`unknown` or invented identity.

Hard budgets are versioned as one call, 12 proposals, 12,000 model-output characters, 4,000 input tokens, 1,600 output
tokens, 5,600 total tokens, 30 seconds, 64 validation levels, 4,096 JSON nodes, 12,000 characters/48,000 UTF-8 bytes
per JSON string, 65,536 canonical request bytes, 262,144 canonical response bytes, and a reasoning summary capped at four items, 2,000 characters and
8,000 UTF-8 bytes. Full-tree key/value Unicode and leaf budgets run before canonical hashing; this includes ignored
provider metadata and intermediate request fields. Unpaired surrogates, 1,500-level objects, 5,000-node values and
oversized reasoning/diagnostic leaves return bounded terminal failures without traceback or content echo. Container
breadth is rejected from `len` plus pending-node accounting before children are enqueued, and over-character strings
are rejected before UTF-8 encoding.

## Fixture evaluation

The offline fake transport covers nine synthetic Bios:

| Synthetic pattern | Expected semantic output |
| --- | --- |
| explicit Singapore model-training work | strong physical-region professional proposal |
| named Xiaohongshu professional account/activity | China digital-ecosystem proposal plus strong China/Asia proxy |
| named WeChat public account | China digital-ecosystem proposal plus strong China/Asia proxy |
| `Head of ... @org` | current claimed affiliation |
| `Prev @org` | previous claimed affiliation |
| `Future Researcher at @org` | future/aspirational affiliation, never current |
| Chinese AI-engineering content | weak observed-language professional context plus weak China/Asia proxy |
| nonprofessional Chinese-language interest | no supported proposal or proxy |
| generic AI systems text | no supported proposal |

Future Luna canary evaluation may compare semantic v2.2 against human labels and a separately reviewed, manifest-pinned
champion. Report per-type
precision/recall, abstention, source-span validity, reason support, review minutes, latency and usage. Strong and weak
proxy metrics are reported separately. This semantic review never performs selection itself, but a separately governed
high-recall sourcing policy may consume either proxy at its declared strength. Neither proxy may establish physical
geography, protected identity or canonical employment. A model challenger may improve recall only while
identity/source/authority/write/fallback guardrails remain zero.

The separate receipt-first Luna v2 lane owns live wire/transport receipt reconciliation. Semantic fixture tests do not
authorize that lane; its production validation and promotion gates remain governed by
`docs/LUNA_RELAY_LIVE_CANARY_CONTRACT.md`.

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
