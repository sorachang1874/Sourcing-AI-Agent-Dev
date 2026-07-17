# GDM Wave-Q reply expansion diagnostic

Run date: 2026-07-17

Report date: 2026-07-17

Status: `diagnostic_only_unattested`. This candidate-free artifact is not an independent-review artifact, a formal
`GO`, promotion authority, a canonical state update, or a population-exhaustion claim.

## Decision

Wave Q materially improved the evidence frontier, so the mapping method has **not** reached a recall plateau. Fifty
native-X retrieval calls produced 33 previously unseen stable Post IDs. Exact hydration plus one append-only singleton
retry produced 32 source-bound full texts and left one persistent `tool_error`. Ten Luna calls then accepted all ten
affected candidate bundles and selected 12 Wave-Q pretraining observations across seven candidates.

The two query families were not equally productive. The authored-Reply family produced 22 new IDs across eight
candidates (`0.88` new IDs per retrieval call), and 16 of its 21 exactly hydrated texts passed the offline
pretraining-lexical screen. The generic architecture/non-Reply family produced 11 new IDs across six candidates
(`0.44` per call), but only one of 11 exact texts passed that screen. This supports keeping Reply/conversation context
while replacing the generic architecture/non-Reply family with two narrower Reply challengers:
scaling/optimization/training systems and base-model/multimodal/representation learning. Wave R separates `Top` and
`Latest` for both groups so mode and topic marginal yield remain independently measurable.

The Luna output remains cumulative and diagnostic. Four of nine prior-comparable proposal pairs changed, but the
model selected zero Wave-Q observations for the lab axis. Lab-axis changes therefore cannot be attributed to Wave Q.
The mechanically merged 37-candidate diagnostic matrix moves the evidence-supported recall pool from 20 to 21 and
the unambiguous subset from 11 to 13, while the `current/current` precision tranche remains four. No result in this
report is promoted.

Candidate handles, names, Bios, Post/Reply text, URLs, credentials, and per-candidate decisions remain only in the
owner-private roots. This tracked artifact contains root labels, byte SHA-256 bindings, and aggregate counts only.

## Byte-exact artifact bindings

Every artifact SHA-256 below was recomputed over the current file bytes. Values described as semantic digests are
separately labelled; they are not substituted for artifact-byte hashes.

### Wave-Q retrieval

Root label: `gdm37-waveq-unresolved-authored-20260717T050123Z`

| Artifact | Recomputed byte SHA-256 |
|---|---|
| Controller | `3a2e4e0e876ae5bb8b5fbf73184877474e22e723e279cc03caa80bfba481fffb` |
| Campaign intent | `ae6ea3291c54ffcf16427627325f18dbec00ad4bee2ea08ef4b40d2738808d1a` |
| Frozen input | `ac48ce1dde656a31ef2f25a320cbcfbecae677a1b216a742e60df7f65dcda759` |
| Query-policy file | `905535db63b8f981fc763f6ca5ad325463456d2402e6683043fa444324e8614d` |
| Preparation receipt | `894ba732e0fcc4be02310a95138e922458c6116517b942147233b3efdb19e019` |
| Candidate-free summary | `aa3119d33dc622efa634d17d7f495cf788f108b1a7456a84f688c26ee90ffb8e` |
| Candidate-free receipt | `22db5c5f339a5ec8332a871377d8f2da1459b2ab1721bd509f5a5b54c5571a0a` |
| Committed records | `d536fe8810c3798306fd5cf937b10801b14ceaf29b8cff8b1ba835438a7ea1bb` |
| Frontier marginal | `795436179ede74800b0b2dcbfd19a1e8d5995f32474939f650e889c7233775ab` |

The campaign intent binds the controller, frozen-input, and query-policy file byte hashes shown above, semantic
query-policy digest `006b281b5a4e94b80b96b0470af2c8cb0c1543682626624440f0705a39c4b77d`,
input-order digest `1540f3f39da69cdc01816fa5d09bed5460f19ec79b33e2acab5a56a443a62005`, and source-binding-set digest
`c92d76abb885a9eb0f76f41671e954122e863965ce540e4460352e0134a2778f`.

### Exact hydration and singleton retry

Primary root label: `gdm37-waveq-frontier-exact-hydration-20260717T055931Z`

| Artifact | Recomputed byte SHA-256 |
|---|---|
| Controller | `e9bb04a501b32132a34aa8413e404d3d6180d912a90209691f5f6f98ec1254e6` |
| Campaign intent | `f25f02adf7f4351a86423b7b651f9d709a03c1dc292009ff5c27a3fdeea63692` |
| Frozen input | `f2d095c51831b16e3ba6a26b25421de259a51ad2c109ea0d009e1991700d2d97` |
| Hydration-policy file | `ba3835c94d1c7809be7188627f3cf97b5c0931135b078729bd9d03f9e6cfd116` |
| Preparation receipt | `721cc06864549cbbe31a33b9a9f5281496f468a32b4b62b4320075b6c1dd0523` |
| Candidate-free summary | `abf5bbbc4fb43c5c5bd4ef51c9462ba5fcc0bafb014b423960c0eee64d5ff002` |
| Candidate-free receipt | `c901e235bd507d3658e8ec17c9e07414248db652b9ad522d4da052c479db459d` |
| Hydrated records | `0d7850f38c11012a9dd035838f46e4f1e62194c76ac45cb5c64c914f5fc9e5d1` |

The primary campaign binds the retrieval frontier, parent controller, parent summary, and parent receipt byte hashes
shown in the retrieval table, ordered-binding digest
`4714008933cc90a8de38986dd88ec77fa2b0063e771f2992a4c18750fb6d9e77`, and semantic hydration-policy digest
`419de3058a42846cf1dd1386c2cefbd0ae0cf7e6a4646fcd5d38c69ffb78eac3`.

Retry root label: `gdm37-waveq-frontier-exact-hydration-tool-error-retry-20260717T061637Z`

| Artifact | Recomputed byte SHA-256 |
|---|---|
| Controller | `95aed09c142eb09636eba6a36af9e3b643bb33c284846f8c16e1e6295eb3b68e` |
| Campaign intent | `7224ffc32e589881b523e9095fb50b3acc60291e3375e0f0b22a915f0146e6c4` |
| Frozen input | `dea7d6afaa3937b4faafa4842be1aecdddd5367b42d6c87c05581cbe2a3acb03` |
| Hydration-policy file | `e58e958f28eb86cc5805e7d07af39317426ddf751fe91739988c66c98273cd85` |
| Preparation receipt | `0f7fccc7e3ce04a94d67e2e1bf7670227f65601cbaab22007816c4f92638eb11` |
| Candidate-free summary | `c78b44894e893b5ef4baf9a616561a8cec342243d360d86332d061ae189c6652` |
| Candidate-free receipt | `4f31600e26d3890a9f0ad8482f56016fe7883868f3b2cf610fec97436207f96d` |
| Retry hydrated record | `8022954f8476350d47b74c466574f3390d58f2adf8ea307a5f511935c1f7dcfb` |

The retry binds the primary hydrated-record, controller, summary, and receipt byte hashes shown in the primary table,
ordered-binding digest
`20b04f0cab8a767e1a1a123802acb118e882116b187ef4890df3b8ec6acc82eb`, and semantic retry-policy digest
`90f47d23aa00e62d8ddc86c8268c5bf109c697f092dbcd540deae9f3af070af8`.

### Luna incremental adjudication

Root label: `gdm37-waveq-luna-incremental-v1-20260717T061348Z`

| Artifact | Recomputed byte SHA-256 |
|---|---|
| Controller | `80500809b1353a988967153e67f4e2cc07a1b862fe21da9185b23d9a62deab43` |
| Campaign input binding | `ca67a097b4cc8a1704d5788a49af0abefbb3d298fb994d96ee3476daeaf593fa` |
| Prior-result lineage | `f8308591f7fa2fb666f67ff3a4bb782a301f4bd9b862a1f44cf0fa9a681cb3d1` |
| Preparation receipt | `43920854018f1a2c02a8ce7efe4b21222fccb45eeb40e7ba78c2fefa3484fa40` |
| Live one-shot ledger | `40d3e10b1b245c16b948b9ad8c6437dc8ec1472946c4d76d6099b221a66bd78a` |
| Phase-C inputs | `c61d37cc397e8cd03e871bb2c9f5b1987b98610a22708a30000089b16c2d5c44` |
| Technical batch plan | `3151ca8f023039f0dee84ad206271d5dc578e03b9a87fed7c026c45e5bf86957` |
| Reference results | `50bf568a8a5a6c342b957607305ec410ff253d534780fe34bb983134041cadf4` |
| Projected results | `1b720a4ea02c3af3c427e441ab0d40285f2c4350e812a658773b3cf387085df0` |
| Phase summary | `f278789823fff2ad215023b9da52e12d7ea885b117ba203cdfce593cdda13e61` |
| Campaign summary | `5012d719a9089e7a90b476f2ca3a6fa14c8ce6398cdc780da2f95de64db05b2e` |
| Catalog receipt | `257e0b4b6bf02ead3cb06f79a191711ab55580754881bfddfdeb6edd0b15bbee` |
| Offline replay receipt | `ba71402a1f32ae457901ec25077d2c5b2ea9aca7523626b2ff4828c4b9c1562c` |

The Luna campaign-input artifact binds ten ordered candidate snapshots, 143 cumulative observations (111 prior plus
32 Wave Q), candidate-input-set digest `fdf68a1c2ba7d5bca0f40f896c0f5334d86eccfccd028af99e472ec2c77cef4e`,
candidate-sequence digest `4d7410252f95e07abc84f13ee7eec63fcb1b6b6fd50feb27aa00e7d3c40fad3e`, all-observation-ID-set digest
`4e263b096567ba29f1b0dfe92e0c7fd01d3c2207b43e52cd181acc4b9be45c86`, all-source-ref-set digest
`8b06337e5c59971bed6ed82cb4a01569c0a0e7a0a0716ec600adf34628137205`, and target-descriptor digest
`b6ad62dbd7290c3c9fdc874168cc21cb496942478585e83031bbebc96650db21`. It also binds the primary hydration
summary/receipt/controller/frozen-input/hydrated-record bytes and the prior Wave-P result lineage. The input binding
sets model `gpt-5.6-luna`, low reasoning effort, strict schema, `tools=[]`, `store=false`, no fallback, ten-call
ceiling, and `authorized_state_transition_count=0`.

## Four-stage performance

| Stage | Exact denominator | Outcome | Efficiency |
|---|---:|---:|---:|
| X-native retrieval | 50/50 planned calls; 25/25 candidates; 9/9 accepted sessions | 57 stable unique IDs; 24 known overlaps; **33 new IDs** | **0.66 new IDs/retrieval call** |
| Exact hydration | 33 initial calls + 1 singleton retry | **32 exact source-bound texts**; 1 persistent `tool_error` | 32/33 unique-ID coverage (96.97%); **32/34 exact texts/hydration call (0.941)** |
| Luna transport | 10/10 candidate calls | 10 accepted; 0 failed; 60,368 tokens | 1.0 accepted result/model call |
| Wave-Q semantic marginal | 32 new exact observations reviewed | **12 selected pretraining observations across 7 candidates**; 0 selected lab observations | 12/50 = **0.24 selected observations/retrieval call**; 12/10 = 1.2/model call |

Retrieval recorded zero failed leaves, rejected sessions, fallback calls, generic/unlisted tool calls, or
`x_user_search` calls. Hydration used only `x_thread_fetch`; the singleton append-only retry reproduced the same
`tool_error`, so the residual is retained instead of spending another same-configuration call. Luna returned the exact
requested model for 10/10 HTTP-200 attempts, used no tools or store, and completed in 134.442 seconds with 55,324 input
and 5,044 output tokens.

## Query-family comparison

Both families received 25 retrieval calls. Candidate counts below count candidates with at least one new stable ID;
they are not population counts.

| Query family | New IDs | Candidates with new IDs | New IDs/call | Exact texts | Lexical-signal exact texts |
|---|---:|---:|---:|---:|---:|
| Authored Reply: token/data/objective/pretraining | **22** | **8** | **0.88** | 21 + 1 persistent `tool_error` | **16/21** |
| Authored non-Reply: architecture/training systems | 11 | 6 | 0.44 | 11 | **1/11** |

The lexical column is a deterministic, case-insensitive diagnostic over exact text using the stems/phrases
`pretrain`, `pre-training`, `tokeniz`, `tokenizer`, `training data`, `data mixture`, `training recipe`, `curriculum`,
`deduplicat`, `dataset`, `corpus`, `objective`, and `filter`. It deliberately excludes generic `data` and `loss`
matches. It is not the Luna classifier, is not an eligibility rule, and cannot replace semantic review. Its only use
here is to compare how much clearly on-topic text each retrieval family placed into the exact-hydration lane.

## Diagnostic state delta and attribution boundary

The prior Wave-P recovery contained 36 accepted candidates and one model-binding failure. Wave Q adjudicated the ten
candidates that received new frontier evidence, including that previously unadjudicated candidate. A candidate-free
merge replaces the nine comparable prior rows with the new diagnostic proposals and adds the newly adjudicated row;
it does not write canonical state.

| Metric | Prior diagnostic | Merged 37 diagnostic | Delta |
|---|---:|---:|---:|
| Evidence-supported recall pool (`lab != unsupported` and `pretraining != unsupported`) | 20 | **21** | +1 |
| Unambiguous (`current|historical` on both axes) | 11 | **13** | +2 |
| `current/current` precision tranche | 4 | **4** | 0 |

Among the nine comparable rows, four proposal pairs changed: three included a lab-axis change, two included a
pretraining-axis change, and one changed both axes. Only one of the two pretraining-axis changes cited selected Wave-Q
evidence; two of the four pair changes selected no Wave-Q pretraining evidence at all. More importantly, Luna selected
**zero** Wave-Q observations for the lab axis. Therefore:

- the four changed pairs are an observed cumulative-model comparison, not four Wave-Q-attributable upgrades;
- none of the lab-axis changes can be attributed to Wave Q;
- even a pretraining change that cites Wave-Q evidence is evidence-associated, not causal, without a paired ablation;
- `authorized_state_transition_count=0`, `promotion_authorized=false`, and `diagnostic_only_unattested=true` remain in
  force.

## Plateau and residual assessment

Wave Q is not a plateau result. Thirty-three new IDs, 12 semantically selected new observations across seven
candidates, and a +1/+2 movement in the merged recall/unambiguous diagnostics are too material to declare marginal
exhaustion. At the same time, the architecture family has weak semantic density and should not be repeated unchanged.

The sole unresolved evidence item is one persistent `tool_error` after an exact singleton retry. It stays excluded
from Luna input. This is a one-record evidence-availability residual, not proof that the underlying Post is absent and
not permission to infer its content.

## Wave-R hypothesis and evaluation contract

Wave R should preserve the proven candidate-authored Reply topology while changing the topic surface. It uses two
semantically partitioned groups—scaling/optimization/training systems and base-model/multimodal/representation
learning—and executes each in both `Top` and `Latest`. This tests whether the Reply advantage transfers beyond the
token/data/objective vocabulary and whether ranking mode contributes distinct evidence. Candidate scope is the 24
rows whose two diagnostic axes are not both concrete `current|historical`; the 213-ID known frontier, exact hydration,
Luna contract, and lifecycle-state semantics stay fixed. Project/paper/official/thread challengers remain the next
topology change if this authored-Reply expansion has small semantic marginal yield.

Wave R should report four primary ratios with explicit call owners:

1. **New IDs per native-X retrieval call** = stable IDs not in the frozen known-ID set / accepted native-X retrieval
   calls. Wave-Q reference: `33/50 = 0.66`.
2. **Exact texts per hydration call** = unique source-bound frontier texts / all hydration calls, including retries.
   Wave-Q reference: `32/34 = 0.941`.
3. **Selected new-wave evidence per originating retrieval call** = Luna-selected pretraining observations whose
   `source_generation` is Wave R / native-X retrieval calls. Also report the processing ratio per Luna call. Wave-Q
   references: `12/50 = 0.24` and `12/10 = 1.2`.
4. **Evidence-associated candidate upgrades per originating retrieval call** = candidates whose pretraining proposal
   becomes newly concrete or newly supported and cites at least one selected Wave-R observation / native-X retrieval
   calls. Report lab-only changes and all cumulative pair changes separately. Do not label them attributable upgrades
   unless the changed axis selects Wave-R evidence; for causal wording, add a frozen paired prior-only versus
   prior-plus-Wave-R ablation.

The plateau decision must be based on semantic marginal yield and candidate-state utility, not answer length, an
arbitrary candidate cap, or raw ID count alone. Wave R therefore remains a diagnostic next experiment; this artifact
does not authorize scaling to another lab or promoting any candidate.
