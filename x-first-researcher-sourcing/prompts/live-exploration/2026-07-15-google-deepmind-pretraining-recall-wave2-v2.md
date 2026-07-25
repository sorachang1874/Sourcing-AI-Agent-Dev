You are executing a discovery-first challenger wave of a read-only, high-recall researcher-sourcing experiment with
Grok's native X tools.

Target: novel public X accounts missed by the prior Google DeepMind wave that have public professional evidence of
current or historical Google DeepMind affiliation, pre-training/base-model training relevance, or a plausible path
to either dimension. This wave expands the recall pool; it does not require every retained lead to be fully verified.
Prior handles are exclusions except when materially new public-X-referenced, model-mediated evidence resolves
ambiguity or corrects a temporal state. Never fabricate rows or add famous accounts merely to make the result look
large.

## Mandatory execution topology: discovery before hydration

Execute two phases in strict order.

Phase D is a systematic discovery sweep. Build a case-insensitive internal seed ledger before person-scoped
hydration. Broad role or lab user searches are allowed, but do not issue an exact-name `x_user_search` or a
handle-scoped `from:<handle>` query until the discovery coverage matrix below has been exercised.

Keep target-lab affiliation discovery and pretraining relevance discovery as independent ledgers joined by handle.
Do not require one query, Bio, Post, or Reply to prove both dimensions. A Bio, self employment statement, official or
team interaction, welcome, or farewell may seed affiliation. A separate Post, Reply, quote, thread, report
discussion, project release, conference conversation, author acknowledgement, or technical exchange may seed
pretraining relevance.

Retain a novel public-professional handle when at least one public-X-referenced, model-mediated discovery row supports
a plausible path to the target population, even if the other dimension remains `ambiguous` or `unsupported`.
Missing pretraining proof is not a reason to discard a target-lab lead. Third-party rows remain discovery seeds rather
than first-party proof.

## Discovery coverage matrix

Complete a systematic sweep across these axes before Phase H. This is a coverage matrix, not a request to execute an
unbounded Cartesian product. Exercise every configured era, project/function cluster, evidence surface, and
retrieval mode; describe materially uncovered or zero-yield cells in `limitations`.

1. Organization and era:
   - current `Google DeepMind`, `DeepMind`, `@GoogleDeepMind`, and `@DeepMind` aliases;
   - current, recent-historical, and older-historical time shards;
   - predecessor or merger organizations as ambiguous discovery seeds that require separate successor-lab binding.
2. Model and project families:
   - Gemini, Gemma, Gopher, Chinchilla, RETRO, Flamingo, Sparrow, AlphaCode, Veo, Genie, SIMA, and other directly
     attributable DeepMind or Google DeepMind base, generative, multimodal, speech, vision, robotics, or world models;
   - technical reports, model cards, launch threads, conference discussions, author acknowledgements, and
     contributor conversations for those families;
   - PaLM, PaLM 2, Pathways, Imagen, Parti, AudioLM, MusicLM, and other Google Brain or Google Research families as
     ambiguous predecessor seeds unless separate X evidence binds a contributor to DeepMind or Google DeepMind.
3. Training functions:
   - data curation, mixtures, quality, and synthetic data for base-model training;
   - tokenization, architecture, mixture-of-experts, scaling laws, and compute allocation;
   - optimization, training stability, distributed training, accelerators, JAX/TPU systems, MaxText, checkpointing,
     and reliability;
   - multimodal or domain-specific pretraining and training-time evaluation or safety when it directly affects
     base-model training.
4. Professional graph surfaces:
   - official-lab Posts, Replies, quotes, launch threads, welcomes, farewells, and acknowledgements;
   - named or mentioned contributors, project and paper authors, and one-hop professional edges;
   - first-party Posts and Replies from discovered handles.
5. Retrieval shape:
   - exercise both `mode=Top` and `mode=Latest` for every major discovery family;
   - use explicit `since:` and `until:` shards where recent content would displace historical results;
   - use semantic variants for professional descriptions that omit literal `pretrain` vocabulary.

Harvest handles from official launches, acknowledgements, quoted Posts, Replies, and contributor conversations before
resolving individual profiles. When an official or attributable launch thread exposes contributors, call
`x_thread_fetch` before switching to person-scoped hydration. Search broad project, training-function, and official
or team Reply surfaces with positive `filter:replies`; do not collapse Replies into generic Posts.

`Google Brain` alone is an ambiguous predecessor seed, not Google DeepMind proof. Upgrade such a lead only when a
separate public X source explicitly binds the person to DeepMind or post-merger Google DeepMind. Paper co-authorship
or Google Brain employment alone is insufficient. A project name is also only a discovery seed: product launch,
post-training, generic evaluation, or inference work does not by itself prove pretraining experience.

## Phase H: selective hydration after discovery

Only after Phase D is complete may you hydrate novel handles. Perform at most one exact bare-handle `x_user_search`
for each novel handle and reuse its result. This is a duplicate-query constraint, not a candidate or call cap. Do not
repeat profile-query variants. The authoritative result-v2 schema can persist handle, stable platform user ID,
canonical profile URL, and Bio excerpt only. Do not invent or serialize display name, location, professional category,
affiliation-badge context, website, or other profile fields outside that schema; note a materially missing field in
`limitations` rather than forcing it into another field.

If discovery already produced direct technical evidence, do not issue another technical search merely for
corroboration. When technical relevance remains unresolved, start with distinct authored-Post and Reply surfaces:

- `from:<handle> (<technical vocabulary>) -filter:replies`
- `from:<handle> (<technical vocabulary>) filter:replies`

Use semantic or thread expansion only for a material ambiguity or conflict. Preserve incomplete candidates instead
of spending repeated calls forcing a confident label. Retain the minimum sufficient public-X-referenced,
model-mediated evidence for each dimension, normally the strongest affiliation row and strongest technical row plus
any row needed to express a temporal conflict. Preserve Post, Reply, quote, thread-root, and thread-Reply topology in
`thread_relation`, and use typed dimension plus temporal-value support claims.

## Under-coverage and convergence

The prior wave retained only 18 accounts. For an organization of this scale, treat that as an under-coverage
diagnostic, not convergence, a quota, or permission to add weak filler. If the cumulative pool remains near that
order of magnitude after the first coverage sweep, run an explicit gap audit and another materially different
discovery pass across uncovered eras, projects, training functions, official/team graphs, Top results, Replies, and
threads.

Only discovery expansions count toward convergence. Exact profile lookups, handle-scoped Post/Reply hydration,
repeated known-name searches, and corroboration of an existing candidate do not count as zero-yield discovery
expansions. Discovery is converged only after:

1. every configured coverage axis has been exercised;
2. Top and Latest plus required historical shards have been covered;
3. an explicit low-result coverage audit has been completed; and
4. three consecutive materially different discovery expansions from different strategy cells add neither a novel
   evidence-bearing handle nor a material temporal correction.

If the external deadline arrives first, use the schema-valid partial status and list uncovered cells in limitations;
do not claim population exhaustion.

Use only native X keyword, semantic, user, and thread tools. No generic web search/fetch, browser, filesystem, shell,
connectors, memory, subagents, mutations, or contact. Use public professional evidence only; never query or infer
protected identity. Do not impose a candidate, observation, or native-X-call business cap. The external operator owns
the deadline, turn ceiling, and emergency kill switch. Deduplicate handles case-insensitively and keep target-lab and
pretraining temporality independent.

Return exactly one JSON object matching the authoritative result schema appended by the operator. Emit no Markdown,
progress prose, prefix, or suffix. `native_x_tool_provenance` is diagnostic; report tools and queries faithfully, but
the operator will derive mechanical call counts and reconciliation from the session trace. Never invent IDs, Bios,
profile fields, timestamps, URLs, excerpts, relationships, or claims.
