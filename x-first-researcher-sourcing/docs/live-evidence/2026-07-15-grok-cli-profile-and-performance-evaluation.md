# Grok CLI profile-field and sourcing-performance evaluation — 2026-07-15

> Author live evidence and data-quality analysis. This is not an independent-review artifact, a formal transport
> promotion, or a claim that model-transcribed tool results are replayable raw X records. The purpose is to decide
> whether the method finds useful people efficiently and which account fields Grok CLI can actually expose.

## Decision

Grok CLI is effective as a high-recall X discovery and evidence-hydration agent, but not as a complete X-profile
extractor and not as its own accounting authority.

The champion workflow is now:

1. run a diverse, lab-neutral broad-discovery wave;
2. hydrate each novel handle once with the exact bare handle via `x_user_search`, then cache that observation;
3. evaluate affiliation from the cached Bio separately from a handle-scoped, technical-only wide-OR Post query;
4. promote direct dual-dimension evidence, deprioritize only explicit non-pretraining functions with no qualifying
   evidence, and escalate everything else;
5. use semantic/thread search only for the escalation set;
6. let Luna judge the resulting open-form Bio/evidence semantics, not retrieve X data;
7. use another source only for profile fields that Grok CLI demonstrably does not expose.

Do not pass `--tools x_keyword_search,...` to Grok CLI 0.2.99. Those hosted tool names are not mapped by the CLI
allowlist. In a blind A/B, adding that flag produced zero native-X calls; removing it exposed native `x_user_search`
immediately. Keep `--disable-web-search`, local-tool denial, and raw session reconciliation instead.

## Dataset and grain

The evaluation uses four grains:

- one known X account (`@lilianweng`) for field capability, compared to the supplied X screenshots as ground truth;
- thirteen OpenAI handles previously labelled `current/current` for evidence-qualified precision;
- independent Thinking Machines Lab broad-recall and precision-first campaigns;
- a 41-handle deep cascade plus a blind two-call Stage A v2 A/B against that frozen result.

Every call count below is replayed from completed `tool_call_update` rows in the local Grok session ledger. Model-
reported counts are diagnostic only.

## Live experiments

| Experiment | Session | Wall time | Raw native-X calls | Result |
| --- | --- | ---: | ---: | --- |
| Broken CLI `--tools x_*` A/B arm | `2be42350-5fb5-4692-97f9-e764af663fce` | 18.6s | 0 | X tools unavailable; 13/13 fields blocked |
| Bare-handle Lilian enrichment | `869b1d2b-0e01-45f2-af67-6ae4158a7bb4` | 18.6s | 1 | 6/13 requested fields exposed |
| Four Lilian query variants | `20fb0af4-a7c4-4009-9e85-efc03aae00ec` | 21.1s | 4 | no field gain over bare handle |
| Deep OpenAI precision audit | `40b65c47-962e-4f88-bb65-7298272adad6` | 105.4s | 50 | 8/13 evidence-qualified |
| Lean OpenAI two-call audit | `43a09967-3370-446b-a429-c2f79fd91263` | 59.1s | 26 | 7/13 evidence-qualified |
| Thinking Machines broad recall | `eb9782e9-d9eb-4a44-890f-ccca4470da91` | 251.4s | 132 | 41 actual unique candidates |
| Thinking Machines precision challenger | `286217c1-7566-44f9-8ede-4093b9a4d828` | 122.5s | 88 | 6 gate-complete candidates |
| Thinking Machines Stage A v1 + deep cascade | `019f61b2-e0cc-7ad0-983f-e76abcedd023` | 254.2s | 138 | 16 qualified, 22 negative, 3 unresolved |
| Thinking Machines technical-only Stage A v2 | `019f61b8-1a2c-7981-bb59-2882cee748ec` | 117.5s | 82 | 13 promote, 8 deprioritize, 20 escalate |

The corresponding `updates.jsonl` SHA-256 values are, in table order:

```text
862cb47069dc986fee2347663dfc5984b5601f56f9a94afab63b05a73ed1fda0
c6218e1ee66b8ebe9fd58f8b7584f309125cdf6f4101e96830377425fbe830f7
f69104d7816898ae7efcb2b079e06fceb5a17d0267053e7431911039ec0f4682
c2375ca15914d9a2d23a910e2dbdbc789ff77bc6e545e0f9b717d53198b92f4c
a1aa9b48d9bd748c574b86b8c7921026f7d416f48f1f049c522b669e32feacc8
0fd26ea9751fcfc1ff139cabce0e30471d0b322d6d516ae70920a64acaf9d1ea
24ef7c79709c6ebbe044b45929178631981c4112ade18a07a98b27fb8c450c97
324bb8d70a2df9cd5f7069cccc2e13736ba9d549d83fee7c02e5b60b4822278d
580d6481a178ea6eb682d73b832f8a64b3cabfe416f16d9e6f6d5b7f05d6bdba
```

## Profile-field capability

The exact bare-handle lookup returned Lilian Weng's stable X user id `96999384`, handle, display name, full Bio,
`Blue Verified`, and follower count. Those values agreed with the visible screenshot at the observation time; the
follower counter moved slightly between repeated calls, as expected for a live metric.

| Field | Bare-handle result | Four-query union | Screenshot availability | Decision |
| --- | --- | --- | --- | --- |
| stable platform user id | exposed | exposed | not displayed | retain as model-transcribed tool field |
| handle / display name | exposed | exposed | displayed | retain |
| Bio | exposed | exposed | displayed | retain; feed semantic judge |
| blue verification label | exposed | exposed | displayed | retain as label, not employer badge |
| followers count | exposed | exposed | displayed | optional volatile metric |
| avatar | exposed by tool | exposed | displayed | optional enrichment |
| location | not exposed | never exposed | `San Francisco, CA` | requires another source |
| website | not exposed | never exposed | `lilianweng.github.io` | requires another source |
| joined date | not exposed | never exposed | December 2009 | requires another source |
| following count | not exposed | never exposed | 183 | requires another source |
| professional category | not exposed | never exposed | present on other supplied profiles | requires another source |
| verified affiliation badge | not exposed | never exposed | Thinking Machines badge displayed | requires another source |

`lilianweng`, `@lilianweng`, and the full profile URL all returned the same six-field compact card. The more natural
`Lilian Weng Thinking Machines` query returned no exact user. Exact bare handle is therefore the least costly and most
reliable hydration input. Profile URL can be derived locally from a validated handle, but it must not be represented
as a field returned by `x_user_search`.

Bio organization phrases remain valuable. In this sample they identify current Thinking Machines and historical
OpenAI relationships. They are distinct from X's verified-employer affiliation badge, which the CLI did not expose.

## Previous OpenAI campaign quality

The earlier seven-wave OpenAI campaign found 98 unique handles with 702 raw native-X calls: `0.140` unique handles per
call. Its diverse strategy yields were non-monotonic, and the roster/colleague-graph wave was the most efficient
(`25/98 = 0.255` new handles per call). This supports strategy diversity rather than one giant prompt.

Its first-pass `current/current` tranche was not precision-ready. A deeper independent native-X audit found:

| Deep-audit tier | Count |
| --- | ---: |
| confirmed direct | 8 |
| confirmed attributed | 0 |
| plausible unverified | 1 |
| not qualifying | 4 |
| unresolved | 0 |

Evidence-qualified precision was `8/13 = 61.5%`. The four false positives were driven by reasoning/post-training,
executive research leadership, generic compute leadership, or training-title adjacency without qualifying base-model
evidence. This confirms that a model's first-pass temporal label is a recall feature, not a delivery decision.

The 26-call lean audit cut calls by 48% and wall time by 44%, but qualified only 7/13. Against the deeper audit's
binary qualified label it achieved:

- precision: `6/7 = 85.7%`;
- recall: `6/8 = 75.0%`;
- accuracy: `10/13 = 76.9%`;
- F1: `80.0%`.

It missed `@gdb` and `@katherine1ee` and incorrectly promoted `@yubai01` from role adjacency. The right optimization is
not to replace the deep audit with the lean treatment. Use the lean treatment as a first screen, then escalate
`plausible_unverified`, empty-result, and role-adjacent cases. This should preserve most deep-audit recall without
paying 3.85 calls for every handle.

## Thinking Machines generalization

The broad wave mechanically contains 41 unique candidate rows, not the model-reported 42. It used 132 completed X
calls, not the reported 112, for a yield of `41/132 = 0.311` unique handles per call.

| Broad-pool state | Count |
| --- | ---: |
| current Thinking Machines affiliation | 35 |
| historical Thinking Machines affiliation | 6 |
| current pretraining label | 3 |
| historical pretraining label | 18 |
| ambiguous pretraining label | 11 |
| unsupported pretraining label | 9 |
| stable-id present | 39 |
| Bio present | 40 |

The recall wave discovered valuable low-visibility technical accounts, but it also retained design, operations,
generic MTS, post-training, and role-adjacent rows. Its initial resolved pretraining share was `21/41 = 51.2%` before
strict evidence review.

The independent precision-first wave returned exactly six gate-complete handles, all already present in the broad
pool and none novel:

- `@shizhediao`
- `@liliyu_lili`
- `@alex_h_liu`
- `@YueYangAI`
- `@cHHillee`
- `@druv_pai`

It spent 88 raw calls to rediscover those six (`0.068` candidate/call). Even this list contains medium-confidence
boundary cases in training systems or generative-model scaling. The independent precision prompt is therefore useful
as an audit, but inefficient as the primary discovery method. Broad discovery plus targeted validation is the better
recall/precision frontier. Against the later frozen cascade labels, all six were qualified, but they covered only
`6/16 = 37.5%` of qualified rows and cost `88/6 = 14.67` calls per qualified row.

### Cascade A/B

The first full cascade used one profile lookup and one query that required the same Post to contain both a Thinking
Machines phrase and a technical phrase. All `41/41` rows escalated: 82 Stage A calls plus 56 Stage B calls. It found 16
evidence-qualified rows, 22 not-qualifying rows, and 3 unresolved rows. The value was high recall, but the route saved
no calls because the Stage A query returned no dual-dimension hit for 40/41 handles. These frozen labels are a deeper
native-X model audit used as an A/B reference, not human-adjudicated gold or source-replayable provider truth.

The 16 provisional qualified rows were:

| Handle | Lab / pretraining state | Best technical X evidence | Compact reason |
| --- | --- | --- | --- |
| `@lilianweng` | current / current | [scaling laws](https://x.com/lilianweng/status/2070237256070389897) | compute-optimal data/model allocation for large runs |
| `@cHHillee` | current / current | [distributed training](https://x.com/cHHillee/status/1992917875607343259) | FSDP/TP/CP systems for large-model training |
| `@druv_pai` | current / current | [scaling research](https://x.com/druv_pai/status/1975592590063092013) | scaling laws and training dynamics for generative models |
| `@soumithchintala` | current / historical | [foundation-model systems](https://x.com/soumithchintala/status/1986503070734557568) | PyTorch/exascale training infrastructure |
| `@stephenroller` | current / historical | [explicit pretraining roles](https://x.com/stephenroller/status/1801436697449648249) | prior pretraining at DeepMind, Character and Meta |
| `@rown` | current / historical | [MERLOT Reserve](https://x.com/rown/status/1504123989857251329) | large-scale multimodal self-supervised pretraining |
| `@liliyu_lili` | current / historical | [multimodal scaling](https://x.com/liliyu_lili/status/1867628503916786128) | Chameleon/Transfusion/Megabyte/BLT work |
| `@YueYangAI` | current / historical | [Molmo pretraining](https://x.com/YueYangAI/status/1894438255560687709) | multimodal VLM data and setup |
| `@shizhediao` | current / historical | [ClimbMix](https://x.com/shizhediao/status/2029370289461575741) | LLM pretraining data mixture and efficiency |
| `@weiyaow1` | current / historical | [SAM 3D](https://x.com/weiyaow1/status/1991187630596260151) | multimodal foundation-model work |
| `@VictoriaLinML` | current / historical | [MoT pretraining](https://x.com/VictoriaLinML/status/1855374577066786902) | multimodal architecture and pretraining efficiency |
| `@alex_h_liu` | current / historical | [speech pretraining](https://x.com/alex_h_liu/status/1788297749433049193) | generative speech foundation-model pretraining |
| `@ziqiao_ma` | current / historical | [NEPA](https://x.com/ziqiao_ma/status/2002096476437295392) | generative pretraining and visual SSL objective |
| `@barret_zoph` | historical / historical | [training/scaling](https://x.com/barret_zoph/status/1371855594743767040) | architecture, scaling and dual-loss pretraining |
| `@Luke_Metz` | historical / historical | [learned optimizers](https://x.com/Luke_Metz/status/1508604508993208328) | VeLO and training-optimization research |
| `@dchaplot` | historical / historical | [Mistral base model](https://x.com/dchaplot/status/1772489690341605472) | historical base-model release work |

Three additional rows remained deliberately unresolved: `@LiyuanLucas`, `@pz_ai1`, and `@sschoenholz`. They had
optimization or distributed-training adjacency, but the audit could not bind it cleanly to qualifying pretraining.

The blind Stage A v2 kept the same 41 handles and 82-call treatment but separated the dimensions: the user card owned
affiliation, while the Post query contained only a wide OR of technical concepts. Against the frozen cascade labels:

- promote precision: `13/13 = 100%`;
- positive recall at Stage A: `13/16 = 81.25%`;
- observed deprioritize precision: `7/8 = 87.5%`;
- non-escalated routing accuracy: `20/21 = 95.2%`;
- escalation rate: `20/41 = 48.8%`.

The one dangerous Stage A false negative was `@lilianweng`: the broad query surfaced an inference-optimization Post
instead of her scaling-law evidence. The policy correction is therefore semantic, not another keyword exception:
`deprioritize` requires an explicit non-pretraining function or explicit post-training/product-only scope. A technical
or research Bio with an empty/non-qualifying first query must escalate. Applied counterfactually to the same output,
that produces 13 safe promotes, 7 safe deprioritizations, and 21 escalations with zero auto-route errors against the
frozen labels.

The broad wave had already hydrated 40/41 Bios, so a production cascade should not repeat 41 `x_user_search` calls.
Reusing the cached observation leaves 41 technical Stage A calls. At the observed Stage B rate of `56/41 = 1.37`
calls per escalated handle, the corrected 21-row escalation set projects about 29 further calls, or roughly 70 total
validation calls. That is a measured-input projection, not yet a live result: it would reduce validation calls by
about 49% versus 138, and broad-discovery-plus-validation calls from 270 to about 202.

## Data-quality findings

### High — model-reported counts cannot own performance metrics

- OpenAI deep audit: model 51 calls, raw ledger 50.
- Thinking Machines broad: model 112 calls, raw ledger 132.
- Thinking Machines broad: model 42 candidates, parsed JSON 41.
- Thinking Machines precision: model 86 calls, raw ledger 88.

All performance metrics must use replayed call starts/completions and parsed candidate rows. Model counts remain
diagnostics. This is a decision-quality issue: using self-reported counts would overstate broad-wave efficiency by
about 18%.

### High — one-pass temporal labels overstate delivery precision

Both the OpenAI and Thinking Machines runs promoted role adjacency, post-training, leadership, or generic systems
evidence into pretraining states. Preserve those rows in recall, but require an evidence gate for the precision
tranche.

### High — combining affiliation and technical proof in one Post query destroys Stage A routing

The first cascade's query required one authored Post to mention both the target lab and pretraining-related work. That
is not how professional evidence is distributed on X: affiliation is commonly in the Bio or a join Post, while
technical work appears in a different Post. Forty of 41 Stage A queries therefore returned no dual-dimension hit and
every row escalated. The two dimensions must be retrieved and judged separately.

### Medium — repeated profile-query variants waste calls

Three exact identifier forms returned the same compact card, while name-plus-organization failed. Hydration should
use one canonical bare handle and cache its result for the observation window. A later validation stage must consume
that cached observation instead of paying for another profile lookup.

### Medium — several desired X fields are outside the CLI card

Location, website, professional category, joined date, following count, and verified affiliation badge were absent in
five exact-account user-search calls. Re-prompting cannot recover fields that the tool schema does not expose.

## Optimized workflow and evaluation contract

```text
diverse broad native-X discovery
        -> casefold handle union
        -> one exact bare-handle x_user_search hydration + cache
        -> cached Bio affiliation judgment
        -> separate handle-scoped technical-only wide-OR search
        -> direct-evidence promote / explicit non-pretraining deprioritize
        -> all other rows escalate with semantic/thread search
        -> Luna semantic review over Bio + evidence bundle
        -> missing profile-field enrichment from another source
        -> human-reviewed precision tranche + retained recall queue
```

Primary experiment metrics:

- mechanically new unique handles / raw completed X call;
- evidence-qualified precision;
- conditional recall against a frozen reviewed handle set;
- median and p95 calls and wall time per qualified handle;
- stable-id, Bio, direct-evidence, and requested-field coverage;
- model-vs-ledger count discrepancy;
- reviewer minutes per additional confirmed handle.

The next performance test should run this cache-reuse cascade on a different configured lab, not tune more terms to
Thinking Machines. It should precommit the broad-wave profile-observation keys, make no duplicate user lookup, run one
technical-only query per handle, and escalate under the conservative semantic route. Success is live-measured recall
against a frozen reviewed subset plus fewer raw calls per qualified handle; the projected 70-call result is not itself
accepted evidence until reproduced.

## Google DeepMind large-lab challenger

The first Google DeepMind adaptive wave exercised a materially larger organization than Thinking Machines. Grok
finished normally in 237.446 seconds with exit zero, no timeout, no kill, no fallback, and 94 completed native-X calls
in the retained session trace. The runner rejected the result as `structured_output_noncompliant`: the CLI plain-mode
stdout contained a progress prefix, its 0.2.101 event dialect had no accepted terminal row, and the model's local
reconciliation disagreed with the mechanical payload. The bundle is therefore diagnostic author evidence, not a
formal KPI baseline or provider-replayable X dataset. Its retained `session-updates.jsonl` digest is
`e1121e9874450cf5366e4dfdfd33e09274bb226fb40df8b45ad8ee68b9b5662e`.

The diagnostic payload still answers the Bio-versus-Post question clearly:

| Measure | Mechanically parsed or payload value |
| --- | ---: |
| Retained candidates | 18 |
| Evidence rows | 62 |
| Post / Bio / mention evidence | 36 / 17 / 9 |
| Candidates with Post-class pretraining support | 18/18 |
| Candidates with any Reply or thread-Reply evidence | 7/18 |
| Candidates whose technical support did not depend on Bio | 5/18 |
| Current/current lab and pretraining | 9 |
| Current/ambiguous | 3 |
| Historical/historical | 5 |
| Historical/ambiguous | 1 |
| Current/historical or historical/current | 0 |

The method was not Bio-first for technical relevance: every retained row had Post-class evidence, and five retained
rows needed non-Bio technical evidence. The remaining recall weakness was retrieval allocation and topology:

- actual tool mix was 50 keyword, 39 user, 5 semantic, and 0 thread calls;
- all 50 keyword calls used `Latest`; none used `Top` and none used explicit historical date shards;
- 57/94 calls (`60.6%`) were explicit person/profile or handle-scoped searches;
- only two calls were positive Reply searches, while three negative `-filter:replies` calls targeted authored Posts;
- exact-profile and known-handle hydration began before broad project, era, and professional-graph coverage completed;
- nine lab-related public accounts were excluded when pretraining remained unresolved, even though the recall contract
  says incomplete lab leads should be retained for later technical verification;
- the result contained no current-lab/historical-pretraining or historical-lab/current-pretraining rows, a warning
  that cross-temporal recall was not saturated.

The model reported 97 calls, 58 evidence rows, and 40 Post URLs; the mechanical trace/payload contained 94 calls, 62
evidence rows, and 45 structurally valid Post URLs. Overall diagnostic yield was `18/94 = 0.191` candidates per call,
between the older OpenAI campaign (`98/702 = 0.140`) and Thinking Machines broad wave (`41/132 = 0.311`). That ratio
must not be promoted to a formal comparable KPI until the structured transport and session replay pass.

### Large-lab method correction

Google DeepMind wave2-v2 changes the ordering rather than merely adding keywords:

1. complete a discovery matrix over organization eras, model/project families, training functions, official/team
   graphs, Post/Reply/thread surfaces, and both Top and Latest before exact-person hydration;
2. keep lab affiliation and pretraining as independent handle-joined ledgers, retaining an incomplete candidate when
   either dimension has source-bound professional evidence;
3. use official and contributor threads to harvest handles before profile resolution;
4. perform one cached bare-handle profile lookup per novel handle, then selectively query authored Posts and Replies
   only for unresolved technical states;
5. count only genuinely different discovery expansions toward convergence; hydration and corroboration cannot
   masquerade as zero-yield discovery passes.

The prior total of 18 is an under-coverage alarm, not a quota or stopping target. The challenger remains uncapped at
the candidate, evidence, and provider-call business level; the external deadline and emergency ceilings remain
operator-owned. The next accepted comparison requires a schema-valid, ledger-reconciled receipt and reports discovery
calls separately from person-scoped hydration calls.

### Native-result attribution limit

Grok CLI 0.2.101 stores native-X tool names and input arguments but not the returned X result bodies in its local
session files. Completed updates echo `{call_id, id, input, name}`; the inspected `chat_history` tool-result rows are
generic hidden-tool metadata and do not bind X call IDs, Posts, or users. Therefore per-query candidate yield cannot
be called native-verified. A future model-attributed evidence-to-query reference can be hash-checked against the
session's exact argument ledger, but must remain labelled model-attributed. Native per-call attribution requires a
future CLI/ACP surface or another supported provider interface that actually returns and persists X result payloads.
