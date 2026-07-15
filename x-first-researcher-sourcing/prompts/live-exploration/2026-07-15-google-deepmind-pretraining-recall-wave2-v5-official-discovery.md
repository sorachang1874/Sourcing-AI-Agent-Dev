You are executing a discovery-only, high-recall challenger wave using Grok's read-only native X tools.

Target: a broad public-X lead pool for people with current or historical Google DeepMind or DeepMind affiliation and
current, historical, adjacent, or still-ambiguous relevance to pre-training, base-model training, foundation-model
training, or the systems and data work that directly enables it. Keep lab affiliation and technical temporality as
independent dimensions. Retain a plausible professional lead when one dimension has public-X-referenced evidence and
the other remains ambiguous; this wave discovers leads for later hydration and does not require final verification.

## Hard phase boundary

This wave is Phase D only. Do not perform person-scoped hydration:

- do not issue any candidate or person `from:<handle>` query;
- do not issue a bare-handle, exact-name, or known-person `x_user_search`;
- do not run per-person Post/Reply corroboration;
- do not spend calls trying to force an ambiguous lead into a confident state.

One narrow organization exception is allowed: keyword search may contain exactly one positive `from:GoogleDeepMind`
or `from:DeepMind` operator because those handles are entry-bound official lab accounts. Do not negate it, combine two
`from:` operators, use it in semantic/user search, or substitute any other handle. These official-account searches
must remain broad organization, project, report, launch, contributor, acknowledgement, hiring, or technical-topic
discovery; they are not permission to hydrate a known person. The operator will mechanically enforce the exact
entry-bound official-handle allowlist and continue to reject every candidate-scoped `from:` query.

Broad role/organization user searches are allowed only in the closed form "Google DeepMind" plus one or more
professional role/function terms such as researcher, scientist, engineer, research, pretraining, training, model,
scaling, tokenization, infrastructure, safety, multimodal, or robotics. Do not add person-name tokens to a user search.
A discovery query must contain multiple professional-context terms; do not use any handle-like single-token query in
keyword, semantic, user, or thread search. A profile ID or Bio may be retained only when it is incidentally returned
by such a broad native-X discovery result. Otherwise set optional profile fields to null. Constructing the canonical
`https://x.com/<handle>` profile URL from an observed handle is allowed. Hydration is a separate operator-generated
stage after this discovery result is mechanically reconciled.

## Coverage matrix

Exercise every family below before evaluating convergence. This is a strategy-coverage obligation, not a fixed call,
candidate, observation, or answer-length quota. Use materially different query wording when a literal family is
sparse, and keep expanding while new evidence-bearing handles or material temporal corrections continue to appear.

1. Organization and time:
   - `Google DeepMind`, `DeepMind`, `@GoogleDeepMind`, historical `@DeepMind`, and the entry-bound official-account
     `from:` surfaces;
   - current, recent-historical, and older-historical shards using explicit `since:`/`until:` ranges;
   - both `mode=Top` and `mode=Latest` for each major organization/era family.
2. Official and professional graph:
   - official Posts, Replies, quotes, welcomes, farewells, acknowledgements, team announcements, hiring, conference
     conversations, and contributor lists;
   - named or mentioned contributors, paper/project authors, launch participants, and one-hop professional edges;
   - positive `filter:replies` discovery across four distinct cells: official organization/team conversations, model
     launch/report conversations, pretraining-function conversations, and a historical-era shard. A query without a
     positive `filter:replies` does not satisfy Reply coverage. Expand additional Reply cells when they yield leads.
3. Models and projects:
   - Gemini, Gemma, Gopher, Chinchilla, RETRO, Flamingo, Sparrow, AlphaCode, Veo, Genie, SIMA, AlphaFold, and other
     attributable DeepMind or Google DeepMind base, generative, multimodal, vision, speech, robotics, or world models;
   - technical reports, model cards, launch threads, author acknowledgements, conference threads, and contributor
     conversations;
   - Google Brain or Google Research projects remain ambiguous predecessor seeds unless a separate public X source
     binds the person to DeepMind or post-merger Google DeepMind.
4. Training functions:
   - pretraining data, mixtures, quality, filtering, deduplication, synthetic data, and data systems;
   - tokenization, architecture, mixture-of-experts, scaling laws, compute allocation, and training-time evaluation;
   - optimization, training stability, distributed training, JAX/TPU, accelerators, MaxText, checkpointing,
     reliability, and large-scale research engineering;
   - multimodal or domain-specific pretraining and training-time safety when it directly affects base-model training.
5. Retrieval diversity:
   - keyword searches across literal and synonym vocabulary;
   - semantic searches for professional descriptions that omit `pretrain` or `foundation model` wording;
   - broad user searches for organization plus role/function clusters;
   - `x_thread_fetch` for official or attributable launch, report, acknowledgement, or contributor threads that expose
     multiple handles.

Harvest handles from Posts, Replies, mentions, quotes, acknowledgements, and threads. Do not over-focus on Bio: a
person's authored or mentioned Post/Reply evidence can establish the discovery path even when the Bio omits technical
work. Tokenization, data, architecture, optimization, distributed systems, training infrastructure, multimodal
training, and evaluation-for-training can all be pretraining-relevant; generic product, post-training, inference,
application, or evaluation work alone is not.

## Evidence and terminal-result preflight

Retain every novel, evidence-bearing professional lead; do not add famous or weak filler. For each candidate, retain
the smallest useful evidence set that explains the discovery path, including stable Post URL, author, timestamp,
bounded excerpt, topology, and typed temporal support claim when available. Third-party and official evidence are
valid discovery seeds but must use their lower-authority relationship. Use `relationship=self` only when the evidence
author handle equals the candidate handle case-insensitively. Never invent an ID, Bio, Post, URL, timestamp, excerpt,
relationship, topology, or support claim.

Before emitting the terminal object, perform all of these checks:

1. Emit exactly one candidate row per case-insensitive handle. Reconcile duplicate rows before output. Never attach a
   Post about a named person to a guessed handle unless that same handle is explicitly bound by the returned X data.
   If same-handle evidence genuinely conflicts, retain only correctly bound evidence and use `ambiguous`; do not
   silently select a preferred row or combine evidence about different people.
2. A `kind=bio` evidence row is exclusively a profile Bio: its URL is the candidate profile URL and `post_id`,
   `published_at`, and `thread_relation` are null. Any X status URL is `post`, `mention`, or `thread`, has the matching
   `post_id`, a non-null topology in `self_post|reply|quote|thread_root|thread_reply`, and a timestamp.
3. Convert native-X RFC dates such as `Mon, 18 May 2026 14:35:17 GMT` to UTC ISO-8601 exactly like
   `2026-05-18T14:35:17Z`. Never copy an RFC timestamp into `published_at`.
4. Every `current` or `historical` candidate dimension has at least one valid typed support claim for that exact value.
   Keep unsupported or conflicting temporality `ambiguous`; do not upgrade it.
5. Recompute the diagnostic candidate/evidence/tool counts from the final object and the calls you observed. Counts do
   not override the operator's mechanical session ledger.

Complete an explicit gap audit after the first matrix pass. As a model-side stopping heuristic, exercise all configured
families, Top and Latest, historical shards, the four Reply cells, and attributable thread expansion, then try three
materially different discovery expansions from different strategy cells after you observe zero novel evidence-bearing
handles or material temporal corrections. The operator cannot verify per-query yield from retained arguments and will
never treat that heuristic as proven population convergence. If the external deadline arrives first, return a
schema-valid partial result and list uncovered cells. Exact profile lookup and person hydration are intentionally out
of scope and must not be counted as missing discovery work.

Because this phase intentionally omits per-handle Post/Reply hydration and does not mechanically own the complete
strategy matrix, the operator will keep the overall result at `X_SEARCH_PARTIAL`. That readiness status is expected
and does not by itself mean discovery failed or converged. The operator will independently audit raw session arguments
for attempted coverage cells; per-query yield and population convergence remain unproven in this transport version.
List every materially uncovered or deadline-truncated strategy cell in `limitations`.

Use only Grok native X keyword, semantic, user, and thread tools. No generic web search/fetch, browser, filesystem,
shell, connectors, memory, subagents, mutations, or contact. Use public professional evidence only and never query or
infer protected identity. Do not impose a candidate, observation, query, or native-X-call business cap; the external
operator owns the deadline, token/cost ceiling, and emergency kill switch.

Stay silent while using tools: do not emit progress, checkpoint, interim JSON, commentary, prefix, or suffix. At the
end, return exactly one JSON object matching the authoritative result schema appended by the operator. Model-reported
provenance and reconciliation are diagnostic; the operator derives mechanical call counts from the session trace.
