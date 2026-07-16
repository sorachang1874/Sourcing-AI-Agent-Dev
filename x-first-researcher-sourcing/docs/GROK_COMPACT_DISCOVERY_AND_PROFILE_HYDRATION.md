# Grok compact discovery and profile hydration

Status: proposed reusable workflow. Live diagnostics inform this design, but neither the workflow nor its outputs are
a formal product handoff or a population-exhaustion claim.

## Goal

Use Grok's native X search as the high-recall discovery engine without asking one model response to discover a large
population, hydrate every profile, reproduce every Post, and serialize a large evidence bundle at the same time.
The workflow separates those jobs while preserving the AI-native advantage:

1. Grok chooses and executes native-X searches inside a configured strategy shard.
2. A compact result retains candidate identities, independent temporal proposals, and evidence-bearing X references.
3. The operator merges strategy shards mechanically and measures their marginal contribution.
4. A separate Grok user-search phase hydrates profile fields for the union.
5. Post, Reply, quote, mention, and thread evidence is hydrated and semantically reviewed independently from profile
   text. Bio is one evidence surface, not the population definition.

There is no business limit on candidates, observations, source references, or native-X calls. Runtime deadlines,
response-byte limits, turn limits, cost controls, and process-group termination are technical emergency controls, not
recall targets or proof of convergence.

## Configured target, not hard-coded prompts

Every campaign supplies a versioned target descriptor. The descriptor, rather than Python branches, owns:

- target-lab aliases and current/historical organization names;
- official X handles allowed for broad `from:` discovery;
- project, paper, model, report, dataset, and infrastructure families;
- pretraining-adjacent function families such as data, tokenization, architecture, scaling, optimization,
  distributed training, training infrastructure, multimodal training, and training-time evaluation;
- historical time shards;
- role/function scope;
- enabled discovery strategy shards;
- desired profile fields and downstream semantic-review policy.

Changing Google DeepMind to OpenAI, Anthropic, xAI, Meta, Thinking Machines Lab, or another lab changes the descriptor
and reviewed prompt binding. It does not change merge or scoring code.

## Phase contract

### Phase 1: compact discovery

Discovery uses native X keyword, semantic, and thread tools. `x_user_search` is disabled at the tool layer so the model
cannot spend the discovery budget re-hydrating people it has already found. No exact-name, bare-handle, or
person-scoped query is permitted in this phase.

Each strategy shard explores a materially different search topology:

- broad organization/function coverage;
- Reply, quote, mention, acknowledgement, and thread topology;
- historical-era and attributable project/report coverage;
- optional challenger shards selected from measured uncovered cells or low-overlap source classes.

The compact output contract is `x.grok.compact_discovery.result.v1`. It contains no model-authored query totals or
tool-call totals. Those metrics come from the Grok session ledger. Each retained lead has:

- X handle and canonical profile URL;
- nullable platform user id;
- independent `current|historical|ambiguous` lab-affiliation and pretraining-relevance proposals;
- one or more candidate-bound profile or stable status references;
- explicit source surface and supported dimension;
- `model_mediated_unverified` source status.

A Post, Reply, quote, mention, official Post, or thread can establish a discovery path even when the Bio is silent.
An unresolved axis remains `ambiguous`; it does not cause the lead to disappear.

Before a valid terminal object can enter the union, the operator projects execution limitations from the process
receipt. Grok may report the domain limitations `native_x_search_incomplete`, `profile_hydration_incomplete`, and
`thread_hydration_incomplete`. Only the supervising operator owns `result_truncated`,
`execution_deadline_reached`, `transport_failure`, and `model_output_repaired`. The operator removes every model-
authored value in that technical set, re-adds only receipt-backed facts, and deterministically recomputes status. The
unmodified terminal and receipt remain private audit evidence. A model statement cannot turn a normal exit into a
deadline, and a normal exit cannot erase a model's explicit domain-coverage limitation.

### Phase 2: operator union

The operator validates every shard, case-folds handles, unions all leads, and preserves every distinct evidence
reference. It never trusts model-authored aggregate counts.

State reconciliation is deterministic:

- one concrete state plus any number of `ambiguous` observations retains the concrete state;
- conflicting `current` and `historical` observations resolve to `ambiguous` until evidence review;
- conflicting non-null platform user ids are not selected by first-writer order;
- no merge may remove the only source supporting a non-ambiguous axis;
- input order cannot change the merged result.

Per-shard membership remains inspectable so marginal unique leads, overlap, and strategy-specific failure can be
recomputed after the merge.

### Phase 3: profile hydration

Profile hydration runs only after the discovery union. It uses exactly one canonical `x_user_search` lookup per input
handle in a normal attempt. Repeating equivalent handle, URL, and display-name variants is not a default strategy;
additional calls require a typed lookup failure or a separate challenger experiment.

The generic output contract is `x.grok.profile_hydration.result.v1`. Its operator evaluator combines the model object,
the exact expected-handle set, and ledger-derived native tool calls; a model object alone cannot claim X-native
hydration.

The desired open profile field set is:

- matched handle and stable platform user id;
- display name;
- Bio;
- declared location;
- external profile links and expanded destinations when exposed;
- X professional category;
- current/historical organization mentions parsed from Bio;
- X verified-organization affiliation signals;
- verification state;
- field-level missing/unsupported markers.

The model must not invent a field that the user-search result did not expose. Until the transport returns replayable
raw profile records, these fields remain model-mediated observations. They are still useful input for enrichment and
semantic review, but they are not silently relabelled as source-bound truth.

Batch boundaries are chosen from response-byte and turn estimates. Every input must produce exactly one output row;
the operator checks missing rows, duplicates, case-insensitive misbinding, and unexpected extra rows before merging
batches.

Tool compliance is not inferred from a valid model object. The session ledger must contain exactly one completed
`x_user_search` for every expected bare handle and no other native-X call. A schema-valid batch with zero, missing,
duplicate, or extra calls is discarded in full. Because observed compliance is not monotonic in batch size, the
operator does not configure one guessed "maximum batch size": it attempts a response-byte-sized batch and recursively
splits a failed batch until compliant children are obtained or a single-handle leaf returns a typed tool-compliance
failure. This adaptive split is a recovery strategy, not a business cap on the campaign population.

Profile hydration uses the same execution-ownership rule as discovery. The model may retain
`native_x_lookup_incomplete`; the operator replaces deadline, transport, truncation, and repair claims with process-
receipt facts and recomputes `OK|PARTIAL|BLOCKED` before ledger reconciliation. This prevents a schema-valid model
answer from inventing a timeout or concealing a real one.

### Phase 4: Post and conversation hydration

Profile hydration does not decide pretraining experience by itself. Stable status references from discovery seed a
separate evidence lane that can inspect:

- self-authored technical Posts;
- Replies and quoted Posts;
- official lab announcements and acknowledgements;
- attributable threads;
- mentions linking a person to a project, paper, report, dataset, or training system.

The semantic reviewer evaluates lab affiliation and pretraining relevance as separate axes and records whether the
evidence is current, historical, or temporally ambiguous. Tokenization, training data, scaling, architecture,
optimization, distributed systems, and training-time evaluation may be relevant even when the literal word
"pretraining" is absent.

### Phase 5: semantic enrichment

Fast models such as Luna may review open Bio/Post text under a strict response schema. Deterministic code owns schema,
identity binding, source references, allowed states, merge behavior, and aggregate metrics; the model owns open-text
semantic interpretation. Region-professional proxy review, role/function review, lab affiliation, and pretraining
relevance remain separate outputs so one signal cannot overwrite another.

## Owner and source-of-truth matrix

| Contract | Owner | Source of truth | Failure behavior |
|---|---|---|---|
| Native tool invocation count and arguments | Operator | Grok session ledger | Missing or malformed ledger is unmeasured |
| Technical execution limitations | Operator | Process receipt and transcript capture | Model-authored technical claims are replaced, not trusted |
| Domain coverage limitations | Grok, then operator validation | Valid terminal object | Preserved as partial until a later shard or hydration closes them |
| Discovery lead proposal | Grok | Valid compact terminal object | Invalid object is excluded |
| Source URL shape and handle binding | Operator | Compact validator | Bad reference or lead fails closed |
| Cross-shard membership and marginal yield | Operator | Validated handle sets | Recomputed; model totals ignored |
| Temporal proposal | Grok, then semantic reviewer | Evidence-linked proposal history | Concrete conflict becomes ambiguous |
| Profile field observation | Grok user-search hydration | Per-handle hydration row | Missing, duplicate, or misbound row is quarantined |
| Source-bound profile truth | Future payload-returning transport | Replayable raw provider record | Model-mediated fields cannot upgrade it |
| Business precision tranche | Configured policy | Independent lab and pretraining axes | Default tranche requires supported current/current |
| Population convergence | Operator evaluation | Multi-shard marginal and benchmark evidence | A single low-yield shard never proves exhaustion |

## Performance evaluation

The workflow is compared by rates and by strategy segment, not by headline lead count alone.

### Discovery metrics

- case-insensitive unique leads;
- actual native-X calls from the session ledger;
- unique leads per actual call and seconds per unique lead;
- source references per lead and share by profile/Post/Reply/quote/mention/thread surface;
- percentage of leads with support for each temporal axis;
- pairwise overlap, all-shard overlap, and marginal leads added by each shard;
- duplicate native calls, policy-blocked calls, and tool-family mix;
- terminal-object validity, transcript binding, and result-byte size.

### Hydration metrics

- exact input-row coverage and matched-handle rate;
- platform-id, Bio, location, external-link, professional-category, organization-affiliation, and verification-field
  coverage;
- missing-row, duplicate-row, unexpected-row, and handle-misbinding rate;
- actual user-search calls per successfully bound profile;
- elapsed time and output bytes per profile;
- disagreement between profile claims and discovery evidence.

### Quality metrics

- lab-affiliation precision and conditional recall, evaluated independently from pretraining relevance;
- pretraining precision and conditional recall by evidence surface and temporal state;
- precision of the default `current/current` tranche;
- recall contribution of `current/historical`, `historical/current`, `historical/historical`, and ambiguous queues;
- incremental verified leads per strategy shard and per semantic-review minute;
- false identity binding and unsupported-source-claim rate.

## Expansion and stopping

Within a shard, repeated materially different expansions with no new evidence-bearing handle, source class, or
temporal correction may end that shard. Across the campaign, stopping is an operator decision based on the union,
uncovered strategy cells, benchmark misses, marginal yield, precision, latency, and cost. A shard-local zero, answer
length, candidate count, call count, or model statement that search is complete is never population-exhaustion proof.

## Current limitation

Grok CLI is a practical AI-native exploration transport, but its headless wrapper may concatenate intermediate and
terminal JSON while leaving `structuredOutput` null, and current session evidence does not persist native X result
bodies. Formal promotion therefore requires transcript-bound terminal selection and, for source-bound field truth, a
transport that exposes replayable provider payloads. These limits do not erase the measured discovery utility; they
define which claims the output can support.
