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

Every discovery shard serializes the immutable `campaign_id`, `target_descriptor_id`, descriptor SHA-256, shared
discovery prompt-policy SHA-256, contract version, and a unique `shard_id`. Hydration repeats the campaign and target
binding, adds its own phase-policy digest, and binds both the discovery-union digest and the closed identity-input-set
digest. That set hashes `(lead identity, operator-resolved lookup handle, expected stable platform id or explicit
provisional state)`, not handles alone. A missing or unequal binding is a contract error; a shard from another lab or descriptor revision cannot be
unioned merely because its JSON shape is valid.

## Phase contract

### Phase 1: compact discovery

Discovery uses native X keyword, semantic, and thread tools. `x_user_search` is disabled at the tool layer so the model
cannot spend the discovery budget re-hydrating people it has already found. Exact `@handle` keyword/semantic queries
are mechanically rejected. Broader exact-name, bare-handle, or person-scoped exclusions remain versioned prompt-policy
requirements until a descriptor-aware query classifier can enforce them without rejecting legitimate topic terms.

Each strategy shard explores a materially different search topology:

- broad organization/function coverage;
- Reply, quote, mention, acknowledgement, and thread topology;
- historical-era and attributable project/report coverage;
- optional challenger shards selected from measured uncovered cells or low-overlap source classes.

The compact output contract is `x.grok.compact_discovery.result.v1`. It contains no model-authored query totals or
tool-call totals. Those metrics come from the Grok session ledger. Each retained lead has:

- X handle and canonical profile URL;
- nullable platform user id plus an explicit `stable_platform_id|provisional_handle|quarantined_handle_reuse`
  identity status;
- an operator-only `lookup_handle` (null in model shards) and closed identity-conflict codes in the union;
- reversible handle-history proposals and per-lead/per-reference origin-shard membership;
- independent `current|historical|ambiguous` lab-affiliation and pretraining-relevance proposals;
- one or more candidate-bound profile or stable status references;
- explicit source surface and supported dimension;
- `model_mediated_unverified` source status.

A Post, Reply, quote, mention, official Post, or thread can establish a discovery path even when the Bio is silent.
An unresolved axis remains `ambiguous`; it does not cause the lead to disappear. `ambiguous` still means an observed
weak, conflicting, or temporally unresolved signal, so it must have a source reference supporting that dimension.
No evidence is an unassessed input, not an `*_ambiguous_signal`, and is excluded before this result contract.

Before a valid terminal object can enter the union, the operator fixes an immutable session precommit containing the
expected session/request/model/reasoning-effort values, exact user-prompt bytes and digest, expected
system/prompt-context digests, and the exact five-row user-visible `chat_history.jsonl` prefix bytes and digest. The
prefix is the system row, ordinary user row, project-instructions synthetic row, system-reminder synthetic row, and
the `prompt_index=0` prompt row. Closed `verbatim_prompt_row_v1` binds that row to the exact prompt-file bytes after
removing one terminal LF; named legacy `legacy_user_query_envelope_v1` binds the earlier `<user_query>` envelope.
Arbitrary open text is not a prompt-binding mode. The final chat file is deliberately not described as precommitted
because its reasoning/tool/assistant suffix is generated during execution.
One operator builder then replays the immutable six-file Grok session
(`summary.json`, `updates.jsonl`, `events.jsonl`, `chat_history.jsonl`, `system_prompt.txt`, and
`prompt_context.json`) and derives the typed process receipt. It hashes every exact source byte stream and each
tool-event record, validates each tool family's closed argument shape, exhaustively pairs starts/completions, rejects
unsupported tools or omitted calls, and selects the unique contract-valid terminal whose raw byte range begins after
the final tool completion. Registry `x.grok.raw_session_shape.v1` accepts only
`user_message_chunk|agent_thought_chunk|agent_message_chunk|tool_call|tool_call_update` and exact per-kind envelopes.
A single `user_message_chunk` must be the first registered update; every thought, native-X start/completion, and
assistant chunk is rejected until that turn-opening event is present. Before recursive decoding, a deterministic
lexical preflight bounds the complete assistant stream to depth 64 and 50,000 JSON-like nodes; decoder recursion is
also translated into a stable replay error.
A separate closed event registry requires exact `turn_started`, `loop_started`, initial
`phase_changed(waiting_for_model)`, `first_token`, subsequent reasoning/text phases, and final completed
`turn_ended` order with sorted timestamps; unregistered intermediate events fail closed. Summary update and chat
message counts must equal the two replayed JSONL ledgers.
A native-X start proves only `kind=search` with exact `{backend:true,variant:"XSearch"}` raw input; the concrete
`x_*` family and arguments arrive at completion and must reconcile by call id, equal non-empty title, exact event
metadata, and start/completion hashes. The post-prefix chat registry permits only exact completed reasoning rows,
native-X backend-tool rows, and one final assistant row. Backend call identity/arguments must equal the completion
ledger, while final model/reasoning effort and assistant content must equal the precommit/session and selected raw
terminal. Any later system/user row or unknown update/chat kind fails closed. The exact terminal byte slice is
strict-parsed again, so duplicate keys and non-strict JSON
cannot become a valid terminal. Earlier schema-invalid progress JSON is allowed; multiple contract-valid terminals
after the final completion fail closed, and any contract-shaped candidate before the final completion is rejected as
ambiguous provenance rather than reinterpreted as harmless progress. Wrapper `structuredOutput` is not evidence. A fresh but borrowed internally
consistent session fails when any precommitted identity, prompt, system, or context binding differs. The v3 receipt
binds the precommit digest, campaign, descriptor, policy, shard, session, raw-source manifest/transcript SHA-256,
terminal SHA-256 and byte/update range, exhaustive ordered calls, and a separate operator-execution-facts digest. Grok may report the domain limitations
`native_x_search_incomplete`, `profile_hydration_incomplete`, and
`thread_hydration_incomplete`. Only the supervising operator owns `result_truncated`,
`execution_deadline_reached`, `transport_failure`, and `model_output_repaired`. Those four values live in an immutable
operator-owned facts object retained separately from the model/result receipt. The operator removes every model-
authored value in that technical set, re-adds only those facts, and deterministically recomputes status. Revalidation
compares the receipt against the retained facts rather than copying booleans out of the receipt. The unmodified
terminal, precommit, operator facts, immutable raw sources, and receipt remain inside the typed projection envelope as private audit
evidence. Union accepts that envelope only, replays the retained source bytes, recomputes every digest and normalized
result at consumption, and rejects a raw mapping, transcript-free dataclass, or forged wrapper. A model statement cannot turn a normal exit into a deadline, and a normal exit cannot erase a model's
explicit domain-coverage limitation. Structural/domain validation runs before projection; full status coherence runs
after it, so a false model-authored timeout cannot make an otherwise usable terminal un-normalizable.

The shape registry was calibrated against `grok 0.2.101 (5bc4b5dfadcf)` with binary SHA-256
`8431538dbd99379240f558b48b779c651d668b06d793c87311ad532c4395a4e2`, but the current six-file artifact set does not
contain CLI version or binary identity. Therefore the replay proves exact `x.grok.raw_session_shape.v1` conformance,
not that the bytes were produced by that binary. Binding executable/version evidence requires a future
operator-owned execution receipt or expanded source set; an unknown future shape fails closed in the meantime.

### Phase 2: operator union

The operator validates every receipt-projected shard, reconciles stable platform identity, unions compatible leads,
and preserves every distinct evidence reference. It never trusts model-authored aggregate counts.

Identity reconciliation is platform-id first:

- one non-null stable platform id observed under multiple handles becomes one external-account lead with reversible
  handle-history proposals; hydration remains closed until the operator selects one observed current lookup alias;
- one handle observed under multiple non-null platform ids becomes separate `quarantined_handle_reuse` leads, and
  their states or evidence are never combined;
- a missing/model-mediated platform-id observation under the same case-folded handle as exactly one stable identity is
  retained in a candidate-free unresolved sidecar whose candidate-id set contains that one stable id. Its temporal
  axes, source references, handle history, and origin membership never rewrite the stable lead; it adds neither a
  second lead nor a second hydration lookup;
- a provisional observation colliding with multiple stable ids uses the same generalized no-lookup sidecar. For both
  one-id and multiple-id collisions, the sidecar binds exact provisional lead/reference digests, candidate stable ids,
  and origin shards back to retained projected inputs until a separate identity-resolution contract exists;
- this is external-account identity only and never a canonical-person merge.

State reconciliation is deterministic:

- one concrete state plus any number of `ambiguous` observations retains the concrete state;
- conflicting `current` and `historical` observations resolve to `ambiguous` until evidence review;
- conflicting non-null platform user ids for one handle quarantine rather than null-and-merge;
- no merge may remove the only source supporting a non-ambiguous axis;
- input order cannot change the merged result.

The union stores each input shard id and projected-result SHA-256, while every handle-history proposal, lead, and
source reference retains sorted origin-shard ids. Per-shard membership therefore remains inspectable and bound to the
exact projected inputs, so marginal unique leads, overlap, cross-shard evidence, and strategy-specific failure can be
recomputed after the merge.

### Phase 3: profile hydration

Profile hydration runs only after the discovery union. Its owner-built expectation carries one closed identity tuple
per resolved, non-quarantined union lead and uses exactly one canonical `x_user_search` lookup per resolved lookup
handle in a normal attempt. A renamed stable account cannot use lexical alias order as evidence: an operator must
select an observed alias. Repeating equivalent handle, URL, and display-name variants is not a default strategy;
additional calls require a typed lookup failure or a separate challenger experiment.

The generic output contract is `x.grok.profile_hydration.result.v1`. Its operator evaluator accepts only a typed
projection envelope, not a model object plus caller-supplied call dictionaries. The envelope binds the operator
session precommit and execution facts, campaign/target,
run, batch, discovery union, closed identity input set, session, raw-source manifest/transcript, raw terminal byte
range, and paired tool lifecycles; a model
object alone cannot claim X-native hydration. Evaluation additionally requires an owner-supplied typed expectation
for campaign, target, policy, discovery union, batch, and exact identity tuples; a self-consistent batch borrowed from a
different campaign therefore still fails closed.

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

For every matched stable expectation, a null returned platform id is invalid and a different non-null platform id is
fail-closed into the mismatch quarantine count. A provisional expectation may surface a new model-mediated id, but it
does not retroactively rewrite discovery identity. Evaluator error output is candidate-free; malformed nested
projection, receipt, result, completion, or expectation members return stable invalid codes rather than exceptions;
internal schema details and unexpected key names are collapsed to a closed candidate-free projection error.

Tool compliance is not inferred from a valid model object. The typed ledger receipt must contain a unique call id,
unique start/completion event digests and sequences, `started < completed < selected terminal`, and
`completion_status=completed` for
exactly one `x_user_search` per expected bare handle, with no other native-X call. The receipt batch/session/input-set
and terminal digest must equal the result envelope. A schema-valid batch with zero, missing,
duplicate, or extra calls is discarded in full. Because observed compliance is not monotonic in batch size, the
operator does not configure one guessed "maximum batch size": it attempts a response-byte-sized batch and recursively
splits a failed batch until compliant children are obtained or a single-handle leaf returns a typed tool-compliance
failure. This adaptive split is a recovery strategy, not a business cap on the campaign population.

Profile hydration uses the same execution-ownership rule as discovery. The model may retain
`native_x_lookup_incomplete`; the operator replaces deadline, transport, truncation, and repair claims with process-
receipt facts and recomputes `OK|PARTIAL|BLOCKED` before ledger reconciliation. Record-level
`model_output_repaired` is also removed from model ownership and projected consistently across every row from the one
receipt fact. Non-matched rows must contain exactly the lookup-failure limitation implied by
`not_found|blocked|error`; contradictory failure codes are invalid. This prevents a schema-valid model answer from
inventing a timeout, repair, or recovery route, or concealing a real one.

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
| Native tool lifecycle and arguments | Operator | Replay of immutable six-file session bytes; exact start/completion record digests | Missing, unpaired, borrowed, unsupported, or malformed evidence invalidates the batch |
| Session identity and execution context | Operator | Immutable expected ids, prompt bytes/hash, and system/context hashes | Borrowed or freshly rebuilt mismatched sessions fail before projection |
| Technical execution limitations | Operator | Immutable process facts retained separately from the result receipt | Model-authored or receipt-forged technical claims are replaced/rejected |
| Domain coverage limitations | Grok, then operator validation | Valid terminal object | Preserved as partial until a later shard or hydration closes them |
| Discovery lead proposal | Grok | Valid compact terminal object | Invalid object is excluded |
| Source URL shape and handle binding | Operator | Compact validator | Bad reference or lead fails closed |
| Cross-shard membership and marginal yield | Operator | Input projected-result digests plus lead/ref origin ids | Recomputed; model totals ignored |
| Hydration lookup alias and expected platform id | Operator | Exact compact union plus explicit alias resolution for renamed identities | Missing alias, duplicate lookup, null stable id, or id mismatch fails closed/quarantines |
| Temporal proposal | Grok, then semantic reviewer | Evidence-linked proposal history | Concrete conflict becomes ambiguous |
| Profile field observation | Grok user-search hydration | Per-handle row inside a receipt-projected batch | Missing, duplicate, or misbound row invalidates the batch |
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

Grok CLI is a practical AI-native exploration transport, but its headless wrapper may concatenate intermediate
progress JSON and the final terminal while leaving `structuredOutput` null. The operator replay now selects only the
unique contract-valid terminal after the final native-X completion; it does not treat wrapper structured output as
required evidence. An owner-run aggregate replay of the retained v2 diagnostic session crossed the prompt-prefix,
event, chat, update, and 61-call lifecycle registries, then deliberately failed
`raw_session_terminal_assistant_causality_invalid`: three pre-tool objects were contract-shaped before the one
post-tool final object. That result is a compatibility canary and remains diagnostic-only; it produced no projection
and is not relabelled as a passing real session. Current session evidence still does not persist native X result bodies. Formal promotion therefore
requires, for source-bound field truth, a
transport that exposes replayable provider payloads. These limits do not erase the measured discovery utility; they
define which claims the output can support.

The receipt intentionally retains the exhaustive ordered call ledger as the extension point for a future
operator-derived `call_id -> lead/reference` attribution sidecar. The current compact terminal does not carry enough
mechanical provenance to derive that mapping, so acknowledgement/artifact/thread marginal yield must not be attributed
from model prose.
