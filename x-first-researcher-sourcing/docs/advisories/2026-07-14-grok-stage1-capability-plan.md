# ADVISORY_ONLY — Grok offline Stage 1 capability-plan review

> Not proof of X access, not implementation approval, not an independent-review artifact, and not a formal GO.

## Consultation record

- Date: 2026-07-14
- CLI: Grok `0.2.99` (`b1b49ccb71a7`)
- Result: process exit code `0`
- Working directory: `x-first-researcher-sourcing/`
- Permissions: `plan`
- Web search/fetch: disabled
- Cross-session memory: disabled
- Subagents: disabled
- Maximum turns: 12
- Repository writes requested from Grok: none
- External-data activity: no X search, web retrieval, real-person lookup, credential read, OAuth inspection, or
  downstream provider call. The Grok model consultation itself was the only model-backend call.

Effective invocation (the full prompt is preserved in
`docs/advisories/2026-07-14-grok-stage1-capability-request.md`):

```text
grok --cwd <x-first-researcher-sourcing> \
  --disable-web-search \
  --no-memory \
  --no-subagents \
  --permission-mode plan \
  --max-turns 12 \
  --output-format plain \
  --single <offline Stage 0 audit and Stage 1 capability-probe planning prompt>
```

The prompt constrained the review to the files listed under **Evidence reviewed**, kept the live verdict at `NO-GO`,
and explicitly prohibited live X access, real people, credentials, canonical writes, outreach, and protected-trait
inference or proxies.

## Verdict

- Live X or researcher mapping: `NO-GO`.
- X-native capability: `unproven`.
- Stage 0: retain as an immutable, synthetic, fail-closed fixture gate.
- Stage 1: design as a separate, versioned capability probe. It must prove an access contract, not collect a
  researcher population.
- Stage 2 or broader mapping: deferred behind a successful Stage 1 artifact and a new owner decision.

## Findings accepted from the Grok review

### Contract separation

`x.grok.collection.v1` is intentionally fixture-shaped. It fixes fixture mode, provider identity, `.invalid` URLs,
exact `8/8/96/24` budgets, empty errors, and empty canonical writers. A Stage 1 payload cannot conform without either
lying about its provenance or weakening the Stage 0 gate.

If owner decisions later permit implementation, Stage 1 should therefore introduce sibling contracts such as:

- `x.grok.capability_probe.request.v1`
- `x.grok.capability_probe.result.v1`

It must not add a live mode to `x.grok.collection.v1` in place.

### Capability-only population boundary

The smallest probe targets one owner-approved official lab account and requests at most five recent public technical
posts. It must not:

- identify or rank researchers;
- mint provisional people or candidate packets;
- expand mentions, followers, lists, or graph neighbors;
- infer affiliation or pretraining relevance;
- create identity-link proposals or assertions;
- write PersonAsset, CRM, projection, export, or outreach state.

Protected identity remains wholly outside the request, response, scoring, and promotion contracts. No name, language,
region, school, community, bio, post, mention, or graph signal may be used as a protected-trait proxy.

### Missing Stage 1 owners and controls

The current Stage 0 files intentionally do not own the following live-probe concerns:

- an owner-pinned official account stable platform ID and handle history;
- an executable X-native handshake;
- terminal failure/error envelopes;
- model, tool, request, response, and access-mode provenance;
- maximum cost, rate, deadline, and kill-switch behavior;
- retention TTL, minimization, deletion, and correction handling;
- a Stage 1 independent-review artifact and promotion binding.

These are new contracts, not values to infer from the fixture.

## Proposed Stage 1 contracts

### Owner decisions required before implementation or execution

| Decision | Fail-closed default |
| --- | --- |
| Approve one user-triggered external probe request | Do not implement or run |
| Approve legal, terms, and privacy basis for exact returned fields | Do not run |
| Pin model/access mode, cost ceiling, rate budget, deadline, and kill-switch owner | Do not run |
| Confirm the request contains no protected population or proxy | Reject request |
| Confirm output remains private raw probe evidence with no canonical/product writes | Reject integration |
| Pin one official lab account by stable platform ID, with handle secondary | Do not run |
| Approve request/result schema versions and ephemeral storage location | Do not write artifact |

Credentials must never appear in prompts or artifacts.

### Request envelope

The future request should contain only:

- schema version, opaque probe ID, user-trigger/approval reference;
- one lab ID and one official-account stable ID plus handle;
- one fixed capability query: recent public technical posts from that account only;
- hard budgets: one execution, one external call, one page, and at most five observations;
- an owner-set cost ceiling and wall-clock deadline;
- an armed kill switch with explicit trip conditions;
- an ephemeral retention class and delete-after time;
- allowed professional signals and a closed forbidden-signal registry;
- explicit prohibitions on population mapping, graph expansion, web/provider fallback, and protected-trait inference.

The request is invalid if a required owner pin is absent. There is no automatic default model, cost, deadline, account,
or retention period.

### Capability handshake

`x_native_proven` is allowed only when every applicable condition holds:

1. Tool provenance explicitly identifies an X-native access path; generic web search is insufficient.
2. At least one and at most five observations are returned from the single pinned official account.
3. Every item contains a stable platform post ID.
4. Every item has a canonical `https://x.com/{handle}/status/{post_id}` URL whose ID matches the object ID.
5. The author stable platform user ID is present whenever the interface contract says it is exposed.
6. Authored and observed timestamps are timezone-aware and ordered.
7. Provider, access mode, model, tool, request ID, prompt version, and request/response hashes are recorded without
   credentials.
8. Only a bounded excerpt is retained; full body retention is prohibited.
9. No web-search, scraper, Apify, alternate-provider, or retry fallback occurs.

Any failed condition terminalizes the probe as unavailable or failed. Partial valid items do not create partial
capability credit in v1.

### Result envelope

The future result should contain:

- immutable request binding and probe/run/task IDs;
- terminal-total run and task status;
- a separate capability verdict;
- exact provider/tool/access provenance;
- actual executions, calls, pages, observations, cost, and elapsed time;
- zero-to-five raw public-post observations;
- structured terminal errors with non-sensitive messages;
- empty `candidate_packets`, `identity_link_proposals`, and `assertions`;
- false claims for exhaustiveness, employment guarantee, outreach permission, protected inference, and researcher
  mapping authorization;
- retention class, delete-after timestamp, and `full_body_stored=false`.

Pretraining relevance remains `UNKNOWN` or absent in Stage 1. A capability probe cannot become a selection artifact.

## Owner/source-of-truth matrix

| Concern | Proposed owner/source of truth | Stage 1 rule |
| --- | --- | --- |
| Official external account | Owner-reviewed lab registry entry | Stable platform ID owns identity; handle is mutable |
| Probe authorization | Owner approval record | Required and bound to request hash |
| Query | Capability-probe request contract | One fixed official-account technical-post query |
| Budgets | Owner-approved request | `1 execution / 1 call / 1 page / <=5 observations` |
| Provider/access provenance | Probe runner raw metadata | Required; missing or generic-web provenance fails closed |
| Capability verdict | Executable probe validator | Derived only from the complete handshake |
| Observation | Capability result contract | Public-post evidence only; bounded excerpt |
| Cost/deadline/kill switch | Operator/owner contract | No inferred defaults; any overrun terminalizes |
| Retention/deletion | Privacy/legal owner | Ephemeral TTL and deletion evidence required |
| Candidate/person/identity state | Not owned by Stage 1 | Must remain absent or empty |
| Canonical/product writes | Existing product owners | Forbidden |
| Stage 0 fixture | Existing schema, validator, generator, tests | Must remain unchanged by Stage 1 |

## Terminal-total state and fail-closed rules

A future state registry should be explicit and shared by schema, validator, runner, tests, and operator summary.

Recommended run transitions:

```text
queued -> running -> completed
                  -> failed
                  -> cancelled
                  -> killed
```

Recommended task transitions:

```text
pending -> queued -> running -> succeeded
                            -> failed
                            -> cancelled
                            -> expired
```

The capability verdict is orthogonal to runtime status:

- `x_native_proven`
- `capability_unavailable`
- `probe_error`
- `killed`

Unknown status or verdict values are invalid. A terminal run cannot contain a non-terminal task. For every terminal
artifact, exact totals must reconcile with the observation list and provider metadata.

Immediate fail-closed stops include:

- a second execution or external call;
- a second page or sixth observation;
- generic web results or provider fallback;
- missing stable IDs, mismatched/non-canonical URLs, or unverifiable provenance;
- deadline, rate, or cost overrun;
- protected-field/value or credential material;
- full-body retention;
- any candidate, identity, assertion, CRM, export, or outreach write.

No retry ladder is allowed in the first probe.

## Future probe runbook

This is a design only. It becomes executable only after all owner decisions and a separate implementation/review pass.

1. Re-run and record the deterministic Stage 0 generator check, executable validator, tests, scope, and hashes.
2. Validate the owner-approved Stage 1 request offline and bind its hash.
3. Confirm the account pin, access mode, budgets, deadline, retention, and kill switch.
4. Perform exactly one user-triggered call through the approved Grok access path.
5. Retain only required provenance and bounded excerpts; do not persist full post bodies.
6. Normalize into the sibling result contract and validate offline.
7. Terminalize the task and run, reconcile exact totals, schedule deletion, and store deletion evidence.
8. Stop. Do not automatically start Stage 2 or generate researcher candidates.

## Future validation matrix

Before any live call, deterministic synthetic Stage 1 fixtures should prove:

- one-to-five valid X-native observations can pass the handshake;
- generic web-shaped provenance becomes `capability_unavailable`;
- missing IDs, non-canonical hosts, URL/ID mismatch, or missing timestamps fail closed;
- a second execution/page/call or sixth observation fails closed;
- unknown and inconsistent run/task states fail closed;
- totals must equal actual observations and provider metadata;
- candidate packets, identity links, assertions, and canonical writers remain empty;
- protected fields, proxy terms, credentials, and full-body retention fail closed;
- deadline/cost/rate/kill-switch failures are terminal and observable;
- fixture mode still rejects real X URLs and any capability-probe mode;
- both declarative schema and executable validator accept/reject a common adversarial corpus.

Stage 0 gold precision and recall do not apply to Stage 1; Stage 1 measures capability-contract integrity only.

## Promotion gates

| Gate | Required evidence | What it does not authorize |
| --- | --- | --- |
| Stage 0 fixture gate | Generator check, executable validator, tests, independent review, scope hashes | Any live call |
| Stage 1 design approval | Owner decisions and reviewed request/result contracts | Probe execution |
| Stage 1 execution approval | Explicit user trigger, armed controls, approved access mode | Researcher mapping |
| Stage 1 capability result | Valid terminal artifact and independent review of provenance/privacy/budgets | Stage 2 or multi-lab scale |
| Stage 2 eligibility | New owner approval and separate bounded evidence contracts | Product/canonical writes |

Model chat output, generic web results, partial IDs, or a structurally valid artifact without X-native provenance are
never capability proof.

## Local disposition

Accepted now:

- preserve Stage 0 as fixture-only;
- use a sibling Stage 1 capability contract if owner approval is later granted;
- keep the first probe account-level and capability-only;
- require terminal totals, explicit provenance, hard operational budgets, and retention/deletion;
- prohibit automatic promotion to population discovery.

Deferred:

- exact official account pin;
- concrete model/access mode, cost ceiling, TTL, and deadline;
- precise provider tool name and proof of X-native access;
- Stage 1 schema/validator/runner implementation;
- adapter consumption;
- Stage 2 collection and multi-lab expansion;
- Meta scope;
- live researcher mapping.

The live verdict remains `NO-GO` until those owner decisions and gates are satisfied.

## Evidence reviewed

Grok was constrained to read only:

- `AGENTS.md`
- `README.md`
- `docs/GROK_CAPABILITY_GATE.md`
- `docs/X_FIRST_FIXTURE_CONTRACT.md`
- `configs/labs.v1.json`
- `configs/query_families.v1.json`
- `contracts/x.grok.collection.v1.schema.json`
- `src/x_first/contracts.py`
- `scripts/generate_openai_fixture.py`
- `tests/test_x_first_fixture_contract.py`

This file is a normalized, locally reviewed record of Grok's advisory. It intentionally corrects one wording ambiguity
from the raw response: the Grok model consultation was a model-backend call, while no X/web/data-provider retrieval was
performed.
