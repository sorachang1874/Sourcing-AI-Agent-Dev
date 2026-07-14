# Stage 2A field-capability contract

> Status: offline fixture implementation, not live X capability, promotion, formal review, or product-write `GO`.

## Outcome and boundary

This slice answers one narrow question before a costing live hydration canary: what evidence must exist before the
system may call a profile or Post field exact, bounded, absent, or unverified?

It does not search X, call Grok/xAI/Luna, use credentials, discover or rank people, confirm employment, merge person
identity, write product state, or contact anyone. The deterministic fixture contains only synthetic handles and
reserved `.invalid` URLs.

The source of truth is the combination of:

- `configs/stage2_field_registry.v1.json` for the exact ordered 14-field descriptors and closed state vocabulary;
- a separately supplied `x.stage2.external_selection.fixture.v1` manifest for the unique fixture-selection
  denominator; the request compares and binds the complete ordered lead tuple: opaque ref, candidate hash, lookup
  handle, reported numeric-id value, and reported-id status;
- the request-owned `stage2-field-capability-fixture-scenarios-v1` manifest for each task's closed `scenario_id`, exact
  expected semantics, scenario-manifest SHA-256, and expectation-semantics SHA-256;
- the four closed outer-envelope JSON schemas under `contracts/`: experiment request, collection, capability
  expectation, and evaluation;
- `src/x_first/stage2_field_capability.py` for deep identity, digest, replay, denominator, retention, and authority
  validation;
- `fixtures/stage2_field_capability_fixture_v1.json` plus its generator for deterministic adversarial replay.

The selection manifest is an external fixture input, not evidence that the request selected itself. Its opaque lead
references, candidate-row hashes, and case-insensitive handles must each be unique. The request must cover exactly
that set once, including the reported-id diagnostic value/status; duplicate account/candidate denominators or a
single-field swap reject.

The scenario manifest is frozen inside the request before collection construction. Its ids are a closed enum and its
semantics are mechanically reproduced from the id; callers cannot attach a different terminal/status/field meaning
to a known id. The capability expectation copies those exact rows, adds row identities, binds the full request SHA,
and repeats both request-owned digests. This avoids an impossible circular full-artifact hash while making the binding
bidirectional: request -> expectation semantics, expectation -> exact request.

## Why model-mediated text is not a fixture hydration result

`model_mediated_unverified` means a model reported a handle, numeric id, Bio, Post, or other field, but the retained
transport evidence cannot replay the provider payload that contained that field. It can remain a diagnostic lead for
a later lookup; it cannot be relabelled as source-bound merely because the prose looks plausible or repeats an id.

A local Grok CLI `0.2.99` trace checkpoint exposed only completed tool-event metadata keys equivalent to `call_id`,
`id`, `input`, and `name`. It did not expose the X profile/Post result payload. The fixture therefore includes that
shape as `tool_metadata_only`: the task terminates `failed/source_payload_unavailable` and all 14 fields remain
`unverified`. Metadata-only sources are always `replayable=false`. Conversely, a `full_fixture_payload` profile/Post
must be `replayable=true` and must be consumed by normalization or a mechanically derived quarantine/incident. No raw
local trace ids or private values are retained here.

The documented xAI SDK path can explicitly request `x_search_call_output`; CLI-like/default event streams expose
invocations without proving result-payload retention. A future live profile canary should therefore use a supported
xAI SDK/API-key transport with explicit call-output inclusion and a new reviewed live envelope. There is currently no
approved `XAI_API_KEY` path in this slice, and no live call was made.

## Field state machine

Every requested field receives exactly one terminal state in registry order:

| State | Meaning |
| --- | --- |
| `present_exact` | One replayable raw record contains the exact identifier/value; derived Bio SHA-256 matches the exact text. |
| `present_bounded` | A declared bounded projection exists; v1 uses this for `bounded_excerpt`. |
| `absent` | A replayable payload explicitly lacks the requested field. |
| `unverified` | The retained transport cannot prove either presence or absence. |

Every task ends in exactly one of `completed`, `quarantined`, or `failed`. The synthetic fixture exercises:

- one exact profile plus same-account bounded Post: `completed`;
- conflicting numeric platform user ids: `quarantined`;
- a handle rename without an already-bound stable account: `quarantined`;
- metadata-only tool trace with no result payload: `failed`.

The exact fixture total is `4 terminal = 1 completed + 2 quarantined + 1 failed`.

The terminal state machine is closed. A completed row has no error, quarantine, or incident ids. A quarantined row
has one typed quarantine and one matching incident. A failed row has no normalized profile/Post or quarantine and
has exactly one `source_payload_unavailable` incident. Zero call receipts or zero source records can never establish
`absent`; that shape is terminal `failed` and all 14 fields remain `unverified`.

Task-row states are never caller-owned summaries. The validator recomputes every profile and Post state from the
consumed full raw records, validates any normalized profile/Post against those records, and requires exact registry
order. A present Post source can therefore never become `absent`. When no Post source exists, Post fields may be
`absent` only when a replayable profile response explicitly carries
`post_fields_explicitly_absent=true`; otherwise they remain `unverified`.

## Profile and Post source binding

A source-bound profile requires one replayable raw profile record that co-locates all of:

```text
platform_user_id
current_handle
profile_url
bio_text
bio_observed_at
bio_content_version
```

`bio_sha256` is derived from the exact UTF-8 Bio text when a Bio is present. Numeric id, handle, URL, and the time at
which the Bio state was observed must be `present_exact`. `bio_text`, `bio_sha256`, and `bio_content_version` are
either atomically `present_exact` or, for an explicitly absent Bio in that same replayable profile record, atomically
`absent` with all three normalized values `null`. A missing payload or
metadata-only trace is not explicit absence and remains `unverified`. This preserves a valid source-bound external
account even when the account has no Bio. A model-reported platform id remains a separate diagnostic value even when
it matches; when it differs, it cannot replace the source id. Multiple ids, handle drift, or an unbound Bio fail
closed.

A Post must replay one raw Post record with numeric Post and author ids, a canonical reserved fixture URL, handle,
authored timestamp, bounded excerpt, and one closed thread relation. Its author numeric id and handle must match the
source-bound profile in the same task. Cross-account Post evidence is rejected. `bounded_excerpt` is deliberately
`present_bounded`; the fixture does not claim a full Post body.

Source observation time must fall inside its call-receipt window. Profile `bio_observed_at` equals the enclosing
source observation exactly, and a Post authored time cannot be later than its source observation. Post source, Post,
task and receipt task ids close in both directions. Numeric `canonical_post_id` is unique collection-wide across raw
Post sources; canonical URL and author bindings are checked from the same source record.

Source-record identity binds task, record kind, provider path, and raw-record SHA-256. Call-receipt identity then
binds the ordered source-record ids. Every full replayable source must be consumed by a normalized profile/Post or a
typed quarantine/incident; selected sources cannot be silently dropped or cherry-picked. Profile/Post, quarantine,
and incident ids are unique and bidirectionally closed through their task rows. This avoids a circular
receipt/source id while preserving closure.

The same numeric platform user id observed under multiple case-insensitive handles is a collection-wide identity
conflict. Every affected task must quarantine with `platform_user_id_handle_conflict`; two such rows cannot both
complete.

Quarantine and incident codes are derived from retained source facts under one precedence: multiple replayable
profile payloads, malformed/multiple profile ids, collection-wide id/handle conflict, lookup-handle rename, then
cross-account Post evidence. Zero profile payloads are `source_payload_unavailable`; two or more full payloads are
`multiple_profile_sources` and cannot erase present evidence by claiming it was unavailable. A multi-id profile keeps
its higher-priority `conflicting_platform_user_ids` result even when one id also participates in a cross-handle
conflict, so every combined conflict has one legal terminal state. The row, quarantine, incident, field ids and exact
task source set must all reproduce that result; swapping a valid closed code and rehashing ids still rejects.

## Owner matrix

| Field or decision | Owner / source of truth | Consumers | Fallback and deletion rule |
| --- | --- | --- | --- |
| Selected fixture denominator | Separately supplied external selection manifest | experiment request and validator | Duplicate lead/candidate/account rejects; request digest/count must match the supplied manifest |
| Requested field set and order | `x-stage2-field-registry-v1` | request, collection rows, evaluator | Unknown/duplicate field rejects; new fields require a registry version |
| Field state | Collection task row, validated against raw record | evaluator only | Missing terminal state rejects; metadata-only always `unverified` |
| External account id | Single replayable profile record | fixture-local profile/Post binding | Model-reported id is diagnostic only; conflict quarantines |
| Handle | Replayable record plus lookup comparison | fixture-local profile/Post binding | Rename quarantines; handle is not canonical person identity |
| Bio | Exact raw record + SHA-256 + observed/version tuple | future source-bound semantic lane only | No prose repair; raw deletion cannot silently retain an exact claim |
| Post author/object | Exact raw Post record and same-account profile | evaluator | Cross-account evidence rejects |
| Terminal task counts | Collection rows | deterministic evaluator | Missing/nonterminal row rejects |
| Fixture capability expectation | Request-bound closed scenario manifest built before collection | deterministic conformance evaluator | Collection output cannot create or relabel expectations; not accuracy gold or human adjudication |
| Retention simulation | Request policy plus collection retention record | fixture validator only | `live_reuse_allowed=false`, `promotion_eligible=false`, expiry never promotes fixture evidence; a future live version needs a real purge owner and receipt |
| Product authority | Closed false/empty fields | all consumers | No fallback; identity merge, discovery/ranking, writes, and outreach remain forbidden |

An iterative byte/depth/node scan runs before JSON Schema traversal, so hostile nested values cannot make schema
validation the first unbounded walk. The JSON schemas then own the closed outer envelopes and array ceilings. The
Python validator owns deep semantics and recomputes the evaluation byte-for-byte from its bound
request, collection, pre-collection capability expectation, and registry. The evaluation reports terminal counts,
per-field states, fixture-expectation matches, and guardrails only; it makes no precision, recall, interval, segment,
or search-quality claim. Its decision is mechanically `offline_fixture_expectation_conformant` only when mismatch
count is zero; otherwise it is `offline_fixture_expectation_mismatch`.

`collection_id` is the SHA-derived identity of every material collection field except itself: request/registry
bindings, terminal rows, receipts, raw sources, normalized profiles/Posts, quarantine, incidents, retention, empty
authority-write arrays, and authority. Rehashing only task rows and raw sources is insufficient.

## Scale boundary

The request has technical parser/execution ceilings (`10,000` tasks, `100,000` source records, byte/depth/node and
24-hour retention limits). These are emergency engineering limits, not business success caps, desired candidate
counts, or convergence rules. The former five-account diagnostic canary remains a separate live safety decision; it
does not constrain a future generalized hydration queue. Search recall, candidate yield, and adaptive expansion are
outside this field-capability slice.

## Validation

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_stage2_field_capability_fixture.py --check
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_stage2_field_capability -v
```

The focused suite contains 37 tests: the 12 specified second-round mutation regressions plus two post-audit combined
state-machine regressions. It verifies deterministic
generation, complete external selection binding, closed request/expectation scenario semantics, both evaluation
decisions, all four envelope owners,
bounded-first/schema ceilings, exact registry order/descriptors, `.invalid`-only fixture content, terminal-total arithmetic,
single-record profile binding, diagnostic-only model ids, metadata-only/zero-source fail-closed behavior, complete
source consumption, conflicting ids across and within handles, handle rename, unbound Bio, deep same-account Post
semantics, unique/closed normalized and incident ids, simulation-only retention, zero authority, and absence of
provider/network imports. Malformed nested values return a closed validation error list rather than escaping
`TypeError` or `AttributeError`.

## Remaining gate

This implementation does not establish that a current live xAI transport returns the required fields. Promotion
still requires a non-author review of this pinned offline slice, owner-approved credential/model/cost/deadline/TTL
decisions, a new live schema and runner, explicit call-output inclusion, private raw evidence with deletion evidence,
and a bounded live field canary. Review delay does not block unrelated offline work.
