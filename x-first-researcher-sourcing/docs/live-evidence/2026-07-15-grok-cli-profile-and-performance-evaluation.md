# Grok CLI profile-field and sourcing-performance evaluation — 2026-07-15

> Author live evidence and data-quality analysis. This is not an independent-review artifact, a formal transport
> promotion, or a claim that model-transcribed tool results are replayable raw X records. The purpose is to decide
> whether the method finds useful people efficiently and which account fields Grok CLI can actually expose.

## Decision

Grok CLI is effective as a high-recall X discovery and evidence-hydration agent, but not as a complete X-profile
extractor and not as its own accounting authority.

The champion workflow is now:

1. run a diverse, lab-neutral broad-discovery wave;
2. hydrate each novel handle once with the exact bare handle via `x_user_search`;
3. run a cheap two-call screen per handle;
4. escalate only ambiguous or role-adjacent rows to semantic/thread evidence search;
5. let Luna judge the resulting Bio/evidence semantics, not retrieve X data;
6. use another source only for profile fields that Grok CLI demonstrably does not expose.

Do not pass `--tools x_keyword_search,...` to Grok CLI 0.2.99. Those hosted tool names are not mapped by the CLI
allowlist. In a blind A/B, adding that flag produced zero native-X calls; removing it exposed native `x_user_search`
immediately. Keep `--disable-web-search`, local-tool denial, and raw session reconciliation instead.

## Dataset and grain

The evaluation uses three grains:

- one known X account (`@lilianweng`) for field capability, compared to the supplied X screenshots as ground truth;
- thirteen OpenAI handles previously labelled `current/current` for evidence-qualified precision;
- independent Thinking Machines Lab broad-recall and precision-first campaigns.

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

The corresponding `updates.jsonl` SHA-256 values are, in table order:

```text
862cb47069dc986fee2347663dfc5984b5601f56f9a94afab63b05a73ed1fda0
c6218e1ee66b8ebe9fd58f8b7584f309125cdf6f4101e96830377425fbe830f7
f69104d7816898ae7efcb2b079e06fceb5a17d0267053e7431911039ec0f4682
c2375ca15914d9a2d23a910e2dbdbc789ff77bc6e545e0f9b717d53198b92f4c
a1aa9b48d9bd748c574b86b8c7921026f7d416f48f1f049c522b669e32feacc8
0fd26ea9751fcfc1ff139cabce0e30471d0b322d6d516ae70920a64acaf9d1ea
24ef7c79709c6ebbe044b45929178631981c4112ade18a07a98b27fb8c450c97
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
recall/precision frontier.

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

### Medium — repeated profile-query variants waste calls

Three exact identifier forms returned the same compact card, while name-plus-organization failed. Hydration should
use one canonical bare handle and cache its result for the observation window.

### Medium — several desired X fields are outside the CLI card

Location, website, professional category, joined date, following count, and verified affiliation badge were absent in
five exact-account user-search calls. Re-prompting cannot recover fields that the tool schema does not expose.

## Optimized workflow and evaluation contract

```text
diverse broad native-X discovery
        -> casefold handle union
        -> one exact bare-handle x_user_search hydration
        -> lab + technical-role + pretraining lean screen
        -> ambiguity/adjacency escalation with semantic/thread search
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

The next bounded experiment should apply the lean-then-escalate cascade to the 41-row Thinking Machines pool and
compare it with the six-candidate independent precision run. The goal is to recover additional true historical
pretraining candidates without repeating 88 calls or admitting the broad pool's role/adjacency false positives.
