# GDM Wave-S Reply-Top semantic diagnostic

Run date: 2026-07-17

Report date: 2026-07-17

Status: `diagnostic_only_unattested`. This candidate-free artifact is not a population-exhaustion claim, canonical
state update, promotion decision, outreach authority, or authorization to scale the method to another lab.

## Decision

Wave S closes one exact configuration: the frozen Wave-Q authored-Reply token/data/objective/pretraining query family
with only the retrieval mode changed from `Latest` to `Top`. It does not close candidate-authored Posts, mention-edge
expansion, profile enrichment, or GDM mapping as a whole.

The live run made 25 native `x_keyword_search` calls over the same 25-account cohort. It returned 49 unique stable
Post IDs, of which 33 were already in the 239-ID frozen frontier and 16 were new. Exact hydration recovered all 16
new records with matching Post ID and author. Luna then selected two of those 16 observations as pretraining evidence
across two accounts.

The strict marginal metrics are:

| Metric | Result |
|---|---:|
| Native-X retrieval calls | 25 |
| New stable IDs | 16 |
| Raw new IDs per retrieval call | `16/25 = 0.64` |
| Exact source-bound hydration | `16/16 = 1.00` |
| Luna-selected new pretraining observations | 2 |
| Selected evidence per retrieval call, `E_retrieval` | `2/25 = 0.08` |
| Selected evidence per exact hydrated observation, `E_hydrated` | `2/16 = 0.125` |
| Accounts with selected Wave-S evidence | 2 |
| Selected-Wave-S-associated newly both-concrete accounts, `B` | **0** |

Two independent A/B model outputs changed pretraining lifecycle state, including one apparent newly both-concrete
row, but neither changed row selected a Wave-S observation. Those movements are unattributed stochastic or context
variation and are not counted as Wave-S gains. The two accounts that did select Wave-S evidence retained their prior
lifecycle states; the new evidence strengthened support without expanding the both-concrete population.

Therefore this exact Reply-Top configuration is **configuration-locally saturated** under the current rule
`E_retrieval < 0.10` and `B <= 1`. It is not a method-level plateau: `E_retrieval` is not below the stronger `0.05`
method-stop threshold, and this is not a second completed orthogonal challenger with zero business increment.

The next orthogonal experiment is source-bound mention-edge expansion. Its cohort must not exclude an account merely
because a prior `x_user_search` profile lookup occurred; profile lookup is not semantic discovery coverage. The
corrected frozen cohort therefore retains two profile-only handles and contains 50 handles rather than 48. Search
`post_intent` and `reply_intent` labels remain retrieval intents only. Exact hydration must establish stable ID,
target author, and Reply/root surface before any result becomes subject-bound evidence.

Candidate names, handles, Bios, Post or Reply text, URLs, observation IDs, credentials, and per-account decisions
remain in private roots. Nothing in this report authorizes an assertion, export, canonical write, or outreach action.

## Byte-exact artifact bindings

All values in the tables below are SHA-256 hashes over the current artifact bytes.

### Wave-S native-X retrieval

Root:
`~/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm37-waves-waveq-cohort-reply-top-20260717T100449Z-v3`

| Artifact | SHA-256 |
|---|---|
| `gdm37_waves_controller.py` | `f234eb05031c999354ac11628007a51b1a02604058a6aa53d3c5fde6abfa5c96` |
| `candidate-free-preparation-receipt.json` | `45e061e2334c8948cef24377c9ae1608f2e33edd4745da2ac07710fd44d38f3d` |
| `candidate-free-summary.json` | `92a9129871f1d478ad9c5681e247803b38544060cc0270beb9d8477fb1255cbc` |
| `candidate-free-receipt.json` | `38690e73e0363cc9be5e6cb1551edd9bc4e9e2666d35449b355ebd930b5a7f46` |
| `frontier-registry.private.json` | `b9c27b6a54ad9ae420c55dafc33d83a72ede1c18e0a17608b5f778e91b85114d` |
| `live/live-one-shot-ledger.json` | `e93dcc5f6dc37854dfa2397d41d44e7edd5c734cdb05aef70b1c0ea5a7ede710` |
| `live/attempt/execution-receipt.json` | `c27b12266327a73f1bb6f45904279b379e21847a2376a55662ccf8d5c54e291b` |
| `live/attempt/raw-session/raw-session-manifest.json` | `757fff1eee4f6b998d89e6caaf2a8639009578f161190364ad3805817eba8c09` |
| `live/attempt/replay.private.json` | `c681b01a405018750ded14c543b88a1ca8520c30cdfa8203850c88938d8c2705` |

The run exited zero after 106.417 seconds, used exactly 25 native-X calls, returned 49 reference occurrences and 49
unique IDs, used no fallback, and accepted the headless completion variant without `turn_completed`. One call returned
the technical maximum of ten results; it contributed one new and nine known IDs. The retained replay performed zero
Grok or network calls.

Two earlier append-only transport roots are not part of the performance denominator. One failed before native-X
execution because the retained session tree exceeded the old file-count validation; the next reached provider-state
ambiguity but committed zero updates and zero native-X calls. The fixed-forward v3 controller removed the dynamic
ephemeral-tree scan, retained the exact raw-six contract, and is the only successful Wave-S retrieval root.

### Exact hydration

Root:
`~/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm37-waves-frontier-exact-hydration-20260717T113000Z-v2c`

| Artifact | SHA-256 |
|---|---|
| `gdm37_waves_frontier_exact_hydration_controller.py` | `ca6ec64d11afd59261aaa1e9f0d40ec6bd0dea1e905800bbe90e68cd1f0c06b3` |
| `frozen-input.private.json` | `9ce93bbd19e29471f0dc6136b88747aa62f35c485a81e4f736383431f759c9c3` |
| `preparation-seal.json` | `f327a46f39e390c7a8abe74b10604729ae4d2a0bcf1a4ae7b6462585b843013b` |
| `offline-self-test.json` | `d744912a7c427bd8dabe2b6dd84459f8ac05830eb367600821211875c5228dd5` |
| `hydrated-records.private.json` | `e594fd1bc792449fc9d54a25439b472f57a0db9d4c91675a01a4678b999dc4af` |
| `candidate-free-summary.json` | `87b9402e9bbdb47f0e926f3425c6d56fd89b59cd0b26e65bccce9bc36dad5882` |
| `candidate-free-receipt.json` | `597a3b2b8f9aa380493cb3346d72617aced51ede51a7a2aa02c16b5647be6327` |
| `live-completion-seal.json` | `cd5d6a83bf7c2cce33c9153094bd434d947add5976a95ca34c793f32bdd5d133` |

The hydration lane used two accepted eight-record attempts and exactly 16 `x_thread_fetch` calls. It produced 16
full texts with exact requested/returned ID and expected-author matches, zero `not_found`, zero `tool_error`, zero
failed or rejected leaves, zero fallback, and no other native-X tool. Offline replay reproduced both sessions with
zero Grok or network calls.

### Paired Luna semantic adjudication

Root:
`~/.local/state/x-first-researcher-sourcing/luna-diagnostics/gdm37-waves-luna-paired-v2-20260717T122500Z`

| Artifact | SHA-256 |
|---|---|
| `gdm37_waves_luna_paired_controller.py` | `d40dddf1bd59639feb37f7a62f0e9086dec7d521dc186f6727e1ab82bc3b5d42` |
| `preparation-receipt.json` | `f19656909766d5f160f56555458ce5de15e1fd041be3df0008081c86e3457c2f` |
| `replay-receipt.json` | `20029036cb9f74266c4f76743f08f14b6912689106a77cb5ccf4a71663be1988` |
| `offline-self-test.json` | `6d5728235165670d23f788a33e2cf7bc0b8c4ff413926410d69778d6caace598` |
| `paired-analysis.private.json` | `b2ed824fc2b06333b473afa5ff7cf7ec865d791484e75df07b0fdd0a9e58b96f` |
| `paired-analysis-summary.json` | `4f9a1468a7c23534695142fec7b029dc3acc1f096b05ca781689cf3285d6f04b` |
| `arms/A/campaign-summary.json` | `2f5fc8d1b2fcdb3ef6db25a5ec41abd6ab38b9f616e6f6fafe89035854968ebc` |
| `arms/B/campaign-summary.json` | `13958a3c6f617a7d0838e23954831891ba78e1bc49816d565b3b7e071b0fac9d` |
| `arms/A/arm-output-receipt.json` | `19a01ec5bf0d02cd26169540f337c7166aca4a046a870c94edb2752235e30d5a` |
| `arms/B/arm-output-receipt.json` | `b105d51f1140012d8d64212315a9c50018bcbdb7d809fdb19e1de0c8b7d66a86` |
| `post-run-analysis-summary.json` | `787b0cb08f66986bb808e09cbaed96339f0ce09da2ab2d3a6f3acdf306a652d5` |
| `post-run-analysis-receipt.json` | `45087fcb0c69da884733f512671c5bbe2ad000b442319309aab1a5da3fbc16df` |

Arm A used the latest cumulative source-bound snapshot available per account: three accounts came from the Wave-R
Arm-B cumulative snapshot and two from the Wave-Q cumulative snapshot. Arm B preserved each account's exact Arm-A
prefix and appended the 16 Wave-S observations. The resulting observation totals were 86 for Arm A and 102 for Arm
B. The strict schema, candidate sequence, instructions, model, reasoning effort, and `as_of` were identical.

Both arms completed 5/5 HTTP-200 calls with exact returned model `gpt-5.6-luna`, zero failures, zero retries, zero
fallback, no tools, `store=false`, and no secret leak. Arm A used 32,841 tokens over 52.557 seconds of phase time;
Arm B used 38,057 tokens over 67.943 seconds. The combined denominator is 70,898 tokens and 120.500 seconds.

The live controller was fixed-forward before execution after a non-author scoped review found two blockers: the
one-shot claim lacked a parent-directory durability barrier, and exact returned-model identity was not an explicit
campaign completion requirement. The successor fsyncs the claim and parent directory, rejects model mismatch as a
campaign-fatal error, and requires
`exact_returned_model_attempt_count == model_external_call_count == accepted_attempt_count == 5`. A second scoped
review found no remaining P0-P3 finding. This report records that review history but is not itself the independent
review artifact or a formal milestone `GO`.

## Stop and continuation rule

Do not repeat the exact Wave-S Reply-Top configuration. Continue GDM-only evaluation with the orthogonal mention-edge
challenger. A zero result there may close that explicit mention-edge configuration, but it still cannot prove global
GDM saturation because five of 255 known statuses lack source-bound full text and explicit `@handle` extraction does
not cover untagged names, links, media, or every quote/repost relationship.

Method-level plateau requires two completed orthogonal challengers with exact hydration at or above 90%,
`E_retrieval < 0.05`, `B = 0`, and no unresolved contract failure. Until that condition is met, scaling to other labs
remains deferred.

## 2026-07-18 continuation note: Grok CLI model/transport drift

Status: `transport_blocked_not_retrieval_plateau`.

Continuation work attempted to resume the orthogonal GDM mention-edge challenger rather than scale to another lab.
The intended next live work was slice 4 and slice 5 of the 100-call mention-edge semantic plan, after the first three
slices showed substantial incremental raw retrieval. That path did not reach a new native-X retrieval denominator.

Observed failures were transport/model-layer failures:

| Root | Attempt | Result |
|---|---|---|
| `gdm37-mention-edge-slice1-exact-hydration-20260717T142127Z-v2i` | pre-live exact hydration gate | blocked before live because cached Grok OAuth hash changed |
| `gdm37-mention-edge-slice1-exact-hydration-20260717T142127Z-v2j` | append-only OAuth rebind | offline prepare, `--verify-prepared-only`, and `--preclaim-dry-probe-only` passed; independent review did not return a usable verdict, so hydration live stayed blocked |
| `gdm37-mention-edge-semantic-challenger-20260717T124050Z-v3k` | slice 4 | blocked before provider because prepared `transport.private.json` no longer matched current Grok auth |
| `gdm37-mention-edge-semantic-challenger-20260717T124050Z-v3m` | slice 3 rerun under current auth | reached Grok CLI but exited 1 before X search because `grok-4.5` is now an unknown model id |
| direct Grok CLI canary | `grok-build` profile/X capability probe | interrupted after repeated `cli-chat-proxy.grok.com` model/settings/bundle timeouts; no X retrieval result was produced |

`grok models` eventually reported the local default and only retained model as `grok-build`, after repeated network
timeouts fetching `/v1/models` and `/v1/settings`. This means old prepared live controllers that pin `grok-4.5` are
stale even when OAuth is fresh. It also means simply changing the model id is not enough while the Grok backend
settings/model endpoints are timing out.

Method implication:

- Do not count the 2026-07-18 attempts in GDM recall/precision performance denominators. They produced zero accepted
  new native-X retrieval calls.
- Before the next mention-edge live wave, add a cheap transport canary that runs before any 24-call slice: current
  model list/default, settings fetch health, OAuth hash, executable hash, and a one-call native-X capability probe.
- Decouple predecessor adoption from exact model id when the predecessor is already sealed and replayed offline.
  Model id should remain part of the local live attempt identity, but historical predecessor slices should not force a
  stale model id onto future local slices.
- Prefer smaller live slices while Grok backend stability is variable. A 24-call slice made failure expensive in wall
  time even though it produced no retrieval output.

This continuation does not change the prior Wave-S conclusion and does not establish GDM method-level plateau. The
next useful step is a `grok-build` transport-health successor with a one-call native-X canary. Only after that can
slice 4/5 mention-edge retrieval performance be measured.

## 2026-07-18 continuation note: transport recovered, tiny GDM canary

Status: `transport_recovered_canary_only`.

After adding the transport-health gate, `grok models` became stable again for the current local CLI surface. The valid
model-health verdict for `grok-4.5` was:

```json
{"available_models":["grok-4.5"],"backend_unstable":false,"large_wave_allowed":false,"native_x_canary_required":true,"requested_model":"grok-4.5","selected_model":"grok-4.5","stale_requested_model":false,"status":"degraded_requires_single_call_canary"}
```

A one-call public-profile native-X canary against `@lilianweng` then exited zero, reported `native_x_used=true`, and
confirmed that the current Grok CLI can retrieve Bio-like public profile text plus at most one public Post. The actual
model usage key was `grok-4.5-build`; the requested command model remained `grok-4.5`. The canary also repeated the
earlier profile-field limitation: location and website URL were not exposed by the available native-X path.

Combining that canary exit code with a fresh `grok models` health check produced:

```json
{"available_models":["grok-4.5"],"backend_unstable":false,"large_wave_allowed":true,"native_x_canary_required":false,"requested_model":"grok-4.5","selected_model":"grok-4.5","stale_requested_model":false,"status":"ready_for_native_x_canary"}
```

A tiny GDM pretraining canary then ran with native-X only and exited zero. It used one model call, reported
`native_x_used=true`, and returned five model-mediated public evidence rows. This canary is deliberately not counted
in the GDM wave denominators because it was not run through the sealed mention-edge controller and did not hydrate or
source-bind candidate rows. It did provide useful method feedback:

- official Google DeepMind posts are a high-yield anchor for pretraining evidence;
- individual researcher self-authored evidence remains much harder to obtain in a tiny generic canary;
- third-party scaling-law and Chinchilla summaries are common and useful for query inspiration, but they are weaker
  than subject, official, or colleague/team evidence;
- semantic search still returns mixed non-DeepMind scaling-law material, so source-bound author/surface hydration is
  required before candidate scoring.

Next method step: do not jump directly to another broad lab. Build or run a small controller successor that starts
from official-account pretraining evidence, extracts reply/mention/author handles, then executes small source-bound
expansion slices. Only after that should the pending GDM mention-edge S4/S5 denominator be resumed or replaced.

A second tiny canary tested that exact official-source expansion shape against two official Google DeepMind
pretraining-evidence Post IDs. It exited zero, reported `native_x_used=true`, and returned public expansion handles
from four relationship types: source author, author-Bio mention, reply author, and quote author. The observed field
limits are important:

- `x_thread_fetch` and related native-X retrieval exposed partial thread/conversation context, not exhaustive
  reply/quote/engagement sets;
- the official source posts did not expose named researcher co-author handles in their text;
- mentioned users were not available as a clean structured entity list beyond free-text post/Bio text;
- likes, reposts, bookmarks, media OCR, and link landing pages were not exposed by this native-X path;
- reply/quote handles are only engagement graph seeds and must not be treated as affiliation or researcher evidence.

Therefore the next GDM method should use official pretraining posts as high-authority evidence anchors, but should
treat their reply/quote/conversation handles as low-authority expansion seeds. Those seeds need their own handle-scoped
self-authored pretraining queries before entering candidate scoring. This is stronger than a generic semantic search,
but weaker than source posts that directly name individual researchers.

## 2026-07-18 continuation note: official-source expansion controller slice

Status: `native_x_canary_completed_controller_fixture_validated`.

A follow-up native-X canary repeated the official-source expansion shape against the same two Google DeepMind source
Posts:

- `https://x.com/GoogleDeepMind/status/1806373232250917334`
- `https://x.com/GoogleDeepMind/status/1968371377637048335`

The Grok CLI command exited zero with requested model `grok-4.5`, actual usage key `grok-4.5-build`, one model call,
`native_x_used=true`, 63,725 total tokens, and reported cost `$0.135246`. The returned response was a diagnostic JSON
inside the CLI wrapper, not a durable production receipt.

The canary returned two source Post records and 22 normalized expansion rows. Relationship coverage was:

- `official_lab_self_post`: official source Post anchors;
- `official_lab_author`: official account author/profile rows;
- `author_bio_mention`: the `@Google` mention in the official account Bio;
- `reply_author`: visible direct reply authors;
- `conversation_participant`: parent/sibling/thread participants.

The latest canary did not expose usable `quote_author` rows. It reported that direct quote/reply advanced searches
against the anchors hit X upstream `DependencyError` on ranked tweets, while `x_thread_fetch` exposed only a partial
conversation tree. This strengthens the current method conclusion: official Posts are high-authority strategy anchors,
but reply/conversation handles are noisy graph seeds. They are not candidates and cannot be scored until a separate
handle-scoped self-authored pretraining query finds independent evidence.

A provider-free helper, `build_official_source_expansion_plan` (private-root controller lane; NOT yet landed in this
repository — planned as the official-source expansion controller slice), projects normalized official-source rows into:

- `anchors`: official-source anchors with `candidate_scoring_allowed=false`;
- `seed_followup_queue`: one row per deduped seed handle with `seed_authority=low_authority_graph_seed_only`,
  `candidate_scoring_allowed=false`, and planned `from:<handle>` champion queries using the shared pretraining alias
  groups;
- `rejected_rows`: non-evidence fields, non-official source authors, malformed rows, and official-lab self-thread rows
  that would otherwise pollute the candidate queue.

Representative live-shape rows from the canary were replayed locally through the helper. The projection produced
`2` anchors, `2` seed follow-up tasks (`aiscout373467`, `ShenzhiWang_THU`), and `1` rejected official self-thread row
with reason `seed_subject_is_official_lab_handle`. The first follow-up task generated the expected two
handle-scoped keyword calls, including the direct-core query:

```text
from:aiscout373467 (pretrain OR pretraining OR "pre-training" OR tokenizer OR tokenization OR "training data" OR "data mixture" OR scaling OR optimization OR "training stability" OR "distributed training" OR accelerator OR TPU OR multimodal OR "base model" OR "foundation model" OR "model training")
```

This controller slice directly addresses the earlier method gap: GDM discovery should not depend on Bio-only evidence
or broad semantic search alone. Official source Posts can now seed a bounded graph expansion lane, and only the
subsequent self-authored Post/Reply evidence can move a handle toward the recall pool.

## 2026-07-18 continuation note: handle-scoped seed self-evidence canary

Status: `low_authority_seed_lane_viable_but_noisy`.

A direct follow-up live canary tested the next hop for ten low-authority graph seeds emitted by the official-source
expansion canary:

```text
aiscout373467, ShenzhiWang_THU, TheAI_Frontier, hubofthewheel_, Qwazer9, siberversegame, GeminiApp,
CyberGodXwolfX, AudioArtis90175, BuckedUnicorn
```

The Grok CLI command exited zero with requested model `grok-4.5`, actual usage key `grok-4.5-build`,
`native_x_used=true`, one model call, 135,036 total tokens, and reported cost `$0.320308`. It used native-X
handle-scoped searches only and explicitly ignored the fact that the handles appeared near GoogleDeepMind.

Result shape:

- checked handles: `10`;
- handles with no visible self-authored pretraining evidence: `6`;
- handles with at least one keyword-relevant self-authored row: `4`;
- rows classified as strong current pretraining evidence: `0`;
- rows classified as mostly `ambiguous`: base-model commentary, RL/post-training experiments, finetune dataset claims,
  end-user training-data comments, or promotional architecture language;
- one `historical` row was a self-post about a larger training dataset for a finetuned Llama3 Chinese chat model, not
  foundation-model pretraining.

Method implication: the official engagement graph is not a high-precision researcher source by itself. It remains
useful as a recall expansion lane because it can surface technically adjacent accounts such as `ShenzhiWang_THU`, but
the follow-up filter must stay strict: only self-authored Post/Reply evidence can move a seed further, and most seeds
should remain unscored or low-priority. For GDM-scale mapping, higher-yield next lanes are still likely:

- official/project Posts that directly name authors or teams;
- paper/project author handles and first-party announcements;
- colleague mentions from known current/historical lab researchers;
- source-bound profile hydration for already discovered candidate handles.

## 2026-07-18 continuation note: direct researcher-mention canary

Status: `project_author_credit_lane_promising_costly`.

A native-X-only direct researcher-mention canary searched for Google DeepMind / DeepMind pretraining-related source
Posts that directly name or `@mention` individual researchers/authors/engineers. It excluded replies, likes, generic
fans, org-only mentions, and engagement graph participants. The command exited zero with requested model `grok-4.5`,
actual usage key `grok-4.5-build`, `native_x_used=true`, one model call, 650,826 total tokens, and reported cost
`$1.45876`.

Result shape:

- direct individual mention rows: `4`;
- strongest source: a project-author Chinchilla thread by `borgeaud_s` with direct coauthor handles
  `jordanhoffmann`, `arthurmensch`, and `laurentsifre`;
- one additional bounded seed from `demishassabis` congratulating `bodonoghue85` on DiffusionGemma;
- multiple official `GoogleDeepMind` / `googlegemma` source Posts were relevant to Chinchilla, Gopher, Gemma, Gemini,
  pre-training, or technical reports but used collective/team wording and did not directly name individual authors.

Method implication: direct researcher mentions are higher quality than reply/conversation graph seeds, but the source
surface is not primarily the official lab account. The highest-value hits came from project-author credit threads and
known-researcher posts. Therefore the next GDM method should split source discovery into three distinct lanes:

1. official lab/project Posts for high-authority technical anchors;
2. project-author or known-researcher credit threads for named collaborator seeds;
3. handle-scoped self-authored evidence for every discovered seed before scoring.

This lane is also expensive in its current broad form. It should be sharded by project family (`Chinchilla`, `Gopher`,
`Gemma`, `Gemini`) and source relationship, then evaluated by marginal named-seed yield per model dollar and per
native-X call.

## 2026-07-18 continuation note: project-family sharding and workstream A/B

Status: `project_sharding_recall_positive_strict_in_search_cost_negative`.

A fresh transport canary against `@lilianweng` exited zero before the project shards. It used one native-X lookup,
returned the public Bio and profile URL, requested `grok-4.5`, reported actual usage key `grok-4.5-build`, consumed
32,019 total tokens, and cost `$0.054238`. Combined with the current `grok models` output, the transport gate returned
`large_wave_allowed=true`.

The Chinchilla shard searched native X without a supplied candidate list. It started from the project/paper aliases,
then expanded official lab, project-author, and known-researcher credit Posts. The command exited zero with
`native_x_used=true`, 389,502 total tokens, and cost `$0.890792`. It returned:

- `8` distinct handle seeds: `jordanhoffmann`, `borgeaud_s`, `arthurmensch`, `eliza_rfd`, `drjwrae`,
  `laurentsifre`, `jack_w_rae`, and `_aidan_clark_`;
- `2` name-only seeds, both `Jordan` but bound to distinct source Post IDs and not identity-merged;
- `13` mechanically enumerated distinct direct-credit source Post IDs. The model-written aggregate said `12` while
  listing 13 IDs, so the prose count is rejected in favor of exact ID reconciliation;
- the strongest source cluster was one official GoogleDeepMind credit Post plus project-author self-claims and
  collaborator-credit threads.

This is materially better than the earlier unsharded direct-mention canary: more named seeds at lower reported cost.
It also exposed a missing relationship: a project author who explicitly says they trained or worked on the project is
valuable `self_authored_project_claim` evidence and cannot be discarded because source and subject handles match.

The first Gopher shard used a broad project-family definition. It exited zero with `native_x_used=true`, 720,516 total
tokens, and cost `$1.455136`. It reported `20` distinct handle seeds, `5` name-only seeds, and `13` primary source
Posts. Recall was high but workstream precision was poor: red-teaming, toxicity, FEVER/fact-checking, GopherCite,
prompting, and evaluation contributors were mixed with Gopher base-model training, MassiveText, scaling, and training
systems.

A second Gopher A/B call put the strict pretraining boundary inside the Grok search prompt. It exited zero with
`native_x_used=true`, 1,386,231 total tokens, and cost `$2.267052`. It reduced the active set to `4` handles and
deferred `10` evaluation/post-training groups. It added one core handle not returned by the broad shard
(`laurentsifre`), but cost more than the broad shard and missed plausible training-adjacent broad-shard evidence such
as `__nmca__`. Therefore strict semantic adjudication inside the retrieval loop is not the default strategy: it
improves precision but causes expensive repeated searching and can lower recall.

## 2026-07-18 continuation note: Luna workstream classifier canary

Status: `broad_grok_then_fast_luna_supported`.

A direct chshapi Responses call used returned model `gpt-5.6-luna` to classify 12 representative evidence excerpts
from the broad Gopher shard. It returned HTTP 200 in about 24 seconds, used 2,361 total tokens, made no web or native-X
calls, and produced one terminal review for every input row:

- `pretraining_core`: `4` rows;
- `training_adjacent`: `2` rows;
- `ambiguous`: `0` rows;
- `evaluation_or_posttraining`: `6` rows.

The classifications matched the intended business boundary on the diagnostic sample: explicit 280B Gopher training,
training FLOPs/scaling, and TPU pipelining/sharding were core; warm-start experiments and a less explicit “working on
a 280B model” claim were adjacent; toxicity, FEVER, red-teaming, and GopherCite fine-tuning were deferred. This live
result supports the following default method:

1. shard Grok retrieval by project family and search broadly for exact source Posts and direct individual credits;
2. pass the retained public excerpts through a fast Luna workstream review;
3. actively follow up `pretraining_core`, `training_adjacent`, and unresolved `ambiguous` handles; keep
   `evaluation_or_posttraining` recall-visible but deferred;
4. use a strict Grok core-only delta query only when the source/project coverage ledger shows a concrete missing
   family, not as the default first pass;
5. preserve lab-affiliation and pretraining-experience temporality as separate downstream axes.

The provider-free mapping policy now admits `self_authored_project_claim`, preserves workstream scope independently of
source type and relationship, and requires real-host normalized rows to identify
`workstream_scope_source=luna_model_mediated_unverified`. Fixture rows may use `fixture_asserted`; a real `x.com` row
with that fixture source is rejected. The scope review remains diagnostic routing, not candidate identity or final
eligibility.
