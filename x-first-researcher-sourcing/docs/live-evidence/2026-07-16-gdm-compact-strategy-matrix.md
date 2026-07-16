# Google DeepMind compact strategy matrix

Date: 2026-07-16

Status: diagnostic-only live evidence. This is not a formal live-gate artifact, a candidate handoff, or an exhaustion
claim.

## Question

Can Grok CLI discover a substantially larger Google DeepMind/DeepMind pretraining-relevant population when discovery
is separated from evidence/profile hydration, and which strategy decomposition improves recall and native-call
efficiency?

All candidate text, handles, profile fields, Posts, Replies, locations, and URLs remain in owner-only private
artifacts. This document contains aggregate metrics only.

Primary private directory:

`/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm-compact-matrix-20260716T111822Z/`

The processes used local Grok CLI `0.2.101`, model `grok-4.5`, high reasoning, native X tools, no generic web search,
no filesystem/shell tools, read-only sandbox, no subagents, a 15-minute external deadline, and no business candidate,
observation, or native-call cap.

## Experiment sequence

| Arm | Unique leads | Actual native calls | Elapsed | Main finding |
|---|---:|---:|---:|---|
| Full evidence v5.1 baseline | 23 | 45 | 199.513s | Useful evidence, but full serialization constrained recall |
| Compact v1 run 1 | 45 | 110 | 290.294s | More leads, but all model refs collapsed to the X root URL |
| Compact v1 run 2 | 40 | 63 | 186.591s | Root-URL evidence failure reproduced |
| Strict-ref mixed phase | 47 | 90 | 207.963s | Valid-looking refs, but 48/90 person-hydration calls violated discovery phase |
| Pure discovery base | 46 | 53 | 161.050s | All calls policy-valid; 0.868 leads/call |
| Pure discovery topology | 39 | 44 | 191.115s | Added 21 over base; 0.886 arm leads/call |
| Pure discovery historical/project | 48 | 45 | 200.791s | Added 26 over prior union; 1.067 arm leads/call |
| Function/role negative-space v1 | 25 | 32 | 142.139s | Added 11 diagnostically, but one person-scoped call excluded the whole arm |
| Function/role negative-space v2 | 16 | 23 | 140.888s | Policy-clean retry added 3 after receipt-owned limitation projection |

The root-URL arms prove that schema success is not evidence success. The original URL pattern admitted
`https://x.com/`; the reviewed compact contract now requires either an exact candidate profile URL or an exact stable
status URL and validates handle/author/subject binding at runtime.

The strict-ref arm proves that prompt wording is not a phase boundary. Requiring stronger refs caused the model to mix
person-scoped `x_user_search` into discovery. Disabling `x_user_search` at the process tool layer removed that failure:
all 142 calls in the three pure-discovery shards passed the discovery subject and phase policy.

## Pure-discovery result

| Shard | Leads | Calls | Keyword | Semantic | Thread | Positive Reply queries | Explicit time shards |
|---|---:|---:|---:|---:|---:|---:|---:|
| Base | 46 | 53 | 42 | 7 | 4 | 2 | 2 |
| Topology | 39 | 44 | 28 | 7 | 9 | 8 | 1 |
| Historical/project | 48 | 45 | 35 | 7 | 3 | 1 | 17 |

Mechanical union metrics:

- 93 case-insensitive unique leads from 133 shard rows;
- 14 leads appeared in all three shards;
- pairwise Jaccard values were 0.269, 0.253, and 0.243;
- base-only 23, topology-only 18, historical-only 26;
- topology added 21 at 0.477 marginal lead/call;
- historical/project added 26 at 0.578 marginal lead/call;
- 142 total calls, 139 unique exact call signatures, and zero exact duplicate call inside any shard;
- 552.956 aggregate sequential seconds and 0.655 union lead/call.

The historical/project shard still added 26 new leads in 45 calls, so the three-arm campaign was not near a
defensible recall plateau. A fourth materially different function/role-first negative-space shard was therefore run;
its results and gate are below.

## Post, Reply, mention, and Bio use

The three terminal objects contained 209 candidate reference rows. Deduplication by candidate, URL, surface, and
support dimensions retained 181 distinct references:

| Surface | Distinct candidate refs |
|---|---:|
| Self-authored Post | 75 |
| Mention | 51 |
| Bio/profile | 42 |
| Reply | 6 |
| Official Post | 5 |
| Quote | 2 |

Status surfaces supplied 139/181 references (76.8%). Every lead had at least one status-class discovery reference;
40/93 also had a profile reference. The method is therefore not Bio-first or Bio-dependent. Bio remains valuable for
profile and affiliation enrichment, while Posts, Replies, mentions, quotes, and threads remain first-class discovery
and technical-experience surfaces.

The old diagnostic shape can mechanically prove self-binding for 42 Bio refs and 75 self-Post refs. It cannot prove
the subject relationship of the remaining 64 mention/Reply/official/quote refs because it lacks separate
`subject_handle` and `author_handle`. The new compact contract adds those fields rather than treating enclosing model
placement as strong binding.

Sixteen thread fetches produced graph expansion but no explicit `thread` surface row. This is a remaining path-
retention weakness: the thread may contribute leads without preserving the exact topology that exposed them.

## Independent temporal dimensions

| Lab / pretraining state | Base | Topology | Historical/project |
|---|---:|---:|---:|
| current / current | 16 | 14 | 19 |
| current / historical | 0 | 0 | 0 |
| current / ambiguous | 19 | 13 | 10 |
| historical / current | 1 | 2 | 1 |
| historical / historical | 9 | 5 | 12 |
| historical / ambiguous | 0 | 1 | 6 |
| ambiguous / current | 0 | 4 | 0 |
| ambiguous / historical | 1 | 0 | 0 |

Across overlapping leads, one lab axis and two pretraining axes had concrete `current` versus `historical` conflicts;
five pretraining axes differed when concrete-versus-ambiguous changes were included. A merge must not use last-writer
wins. The compact operator merge retains one concrete value when all other observations are merely ambiguous and
resolves concrete current/historical conflict to ambiguous for later evidence adjudication.

## Metric ownership and serialization

The model's tool totals drifted from the session ledger in two of three shards. The base result over-reported one
semantic call; topology over-reported two keyword calls. The session ledger remains the only owner of native-call
counts.

All three wrapper `structuredOutput` values were null even though each process exited zero. Their `text` fields
contained 7, 12, and 8 JSON objects respectively. A formal runner cannot scan an arbitrary wrapper string and accept
whichever object looks useful. It must bind the selected terminal assistant object to transcript order after the last
native X completion.

## Profile hydration and tool-compliance gate

A first 49-account profile batch returned one schema-valid structured object with 49/49 matched rows in 117.132
seconds, but the session ledger contained zero native X tool completions. The model answered from internal knowledge
instead of calling `x_user_search`. Every apparent field-coverage result from that batch is excluded.

A separately partitioned 44-account batch completed in 242.086 seconds with exactly 44 distinct bare-handle
`x_user_search` calls and no other native-X tool. It returned 44 terminal rows: 42 exact handle bindings and two
not-found rows. It had no missing/extra/duplicate call, missing/extra/duplicate row, wrong binding, duplicate platform
id, or duplicate matched handle.

The wrapper still left `structuredOutput` null and contained two JSON documents. The session transcript, however,
contained exactly one terminal JSON document after the final native-X completion, and that object passed the supplied
schema. Acceptance therefore depends on the ledger and transcript terminal, not the wrapper field.

Among the 42 exact X-native matches, field coverage was:

| Profile field | Exact matches with field | Coverage |
|---|---:|---:|
| Platform user id | 42 | 100% |
| Display name | 42 | 100% |
| Bio | 40 | 95.2% |
| Bio-explicit affiliation | 37 | 88.1% |
| Bio-explicit organization signal | 37 | 88.1% |
| Verification | 24 | 57.1% |
| Location | 0 | 0% |
| External URL | 0 | 0% |
| Professional category | 0 | 0% |
| X organization-affiliation badge | 0 | 0% |

Every retained affiliation signal came from explicit Bio text. This reproduces the earlier single-profile result:
Grok `x_user_search` is useful for stable id, handle/display name, Bio, Bio-explicit current/previous organizations,
and some verification state. It does not expose the location, homepage link, professional category, or X affiliation
badge visible in the consumer X profile UI. Those fields require another enrichment source rather than repeated
equivalent user searches.

This failure establishes a mandatory quality gate:

`schema valid + exact input rows + exact handle binding + ledger native calls == input rows`

If the last equality fails, the batch is model-without-tool output, not X-native hydration.

Tool-compliance A/B was non-monotonic:

| Requested handles | Exact `x_user_search` calls | Result |
|---:|---:|---|
| 1 | 1 | accepted |
| 4 | 0 | rejected despite valid schema |
| 8 | 8 | accepted |
| 44 | 44 | accepted |
| 49 | 0 | rejected despite valid schema |

Three additional single-handle leaves all produced exactly one user-search call and one valid row in 31.156, 28.371,
and 28.731 seconds. This supports a deterministic adaptive-split recovery: try a response-byte-sized batch; if ledger,
terminal, row, or binding reconciliation fails, discard the whole batch and bisect it; continue until compliant
children are obtained or a single-handle leaf fails. A guessed static maximum batch size would not explain these
observations.

The remaining 49 inputs were subsequently replayed through deterministic adaptive splitting. Across the original
accepted 44 and the replayed 49, all 93 inputs now have compliant completed-tool evidence: 91 exact matches and two
not-found rows. The replay did not borrow any field from the rejected model-only answer. Across the 91 exact matches,
platform id and display-name coverage were 100%, Bio 96.7%, explicit affiliation 89.0%, verification 60.4%, external
URLs 5.5%, and location/professional category remained zero. These are diagnostic field-availability observations,
not source-bound profile truth or a population-exhaustion claim.

The tracked candidate-free machine receipt
`2026-07-16-gdm-compact-strategy-matrix.aggregate-receipt.v1.json` binds these aggregate metrics to SHA-256 hashes of
the owner-private discovery execution receipts, initial hydration aggregate, and adaptive campaign summary. It
contains no candidate handle or Bio, Post, Reply, location, URL, or organization value. Reviewers can now verify the
tracked aggregate-to-private-source binding without receiving candidate data; availability and interpretation of the
private sources remain owner-controlled. The tracked receipt SHA-256 is
`0eb5e2cd3db4ed1c5d7e1e35351d3734ee09536d019647105ac97cd6d68b07f0`.

## Function/role negative-space shard

The fourth discovery shard is function/role-first rather than organization-, project-, or era-first. It prioritizes:

- training data and tokenization;
- optimization and training stability;
- distributed/accelerator systems and training infrastructure;
- multimodal/domain training;
- training-time evaluation and safety;
- first-person technical work, Replies, quotes, contributor acknowledgements, and technical threads.

It disabled `x_user_search`, kept independent lab/pretraining dimensions, permitted ambiguous discoveries, had no
business result or call ceiling, and used only a 240-second wall-clock emergency deadline. New leads were measured
against the frozen 93-lead union.

The first attempt returned 25 leads and 57 references from 32 native-X calls. It overlapped 14 of the frozen 93 and
would have added 11, but only 31/32 calls satisfied the discovery-only query policy. One person-scoped keyword query
crossed the phase boundary. Because the old result did not retain query-to-lead attribution, the whole arm was
excluded instead of guessing which rows were contaminated.

The stricter retry made 23 calls: 13 keyword, seven semantic, and three thread calls. All 23 were policy-allowed,
unique, completed, and present in the raw-output ledger. Its single terminal object after the last tool call passed
the compact runtime validator and contained 16 leads / 33 references. It overlapped 13 of the frozen 93 and added
three, producing a 96-lead validated diagnostic union. The incremental yield was 0.130 lead/call and 1.278 leads/minute.
Its source mix was 14 Bio, 17 self-Post, one mention, and one thread reference; it did not add Reply or quote coverage.

The terminal also asserted `result_truncated` and `execution_deadline_reached`. Those are operator-owned facts, and
the receipt contradicted both: exit zero, 140.888 seconds against a 240-second deadline, no timeout, no TERM, and no
KILL. The operator projection removed those two model-authored claims and added no technical limitation. It preserved
the model's three domain limitations—native-X search, profile hydration, and thread hydration incomplete—so the
normalized shard remains `X_DISCOVERY_PARTIAL`, not a false `OK`. The projection itself passed the runtime contract
with zero errors. This makes the three marginal leads usable as model-mediated diagnostic proposals while preserving
the explicit conclusion that the GDM population is not exhausted.

Private evidence for these attempts is under:

- `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm-function-negative-space-20260716T124532Z/`
- `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm-function-negative-space-v2-20260716T125048Z/`
