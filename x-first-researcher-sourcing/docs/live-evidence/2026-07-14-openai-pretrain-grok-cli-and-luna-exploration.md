# OpenAI pretraining Grok CLI + Luna exploration — 2026-07-14

> Author evidence only. This is not an independent-review artifact, a Stage 2 `GO`, permission to scale, proof of
> exhaustive researcher coverage, or authority to write product/CRM/outreach state.

## Decisions tested

Two live questions were tested separately:

1. Can the installed OAuth Grok CLI execute hosted native-X search tools for a bounded OpenAI/pretraining discovery
   prompt?
2. Can exact `gpt-5.6-luna` interpret open-form Bio text using the business rule that subject-owned China digital-
   ecosystem professional activity is a strong China/Asia professional-experience proxy, while Chinese professional
   or technical content is a weak proxy?

The retained Grok leads were subsequently re-evaluated under two independent configured temporal dimensions:
target-lab affiliation and pretraining experience. This was an offline reinterpretation of the already-sanitized
result; it did not make another provider call.

Neither experiment inferred nationality, ethnicity, or another protected identity. Physical geography remained a
separate explicit-only field.

## Grok CLI native-X result

The successful run used installed `grok 0.2.99`, model `grok-4.5`, generic web disabled, and local terminal/read/write
tools removed. The private raw session is bound to:

- opaque `run_binding_sha256` `b8947f7e2fb986dc7364e7a62203dcba2eb9766ab3de3bc67c93fa234a776f68`;
- eight completed hosted-X calls: four `x_keyword_search`, two `x_semantic_search`, and two `x_user_search`;
- 68 observations reported by the model, eight retained candidate leads, 18 evidence rows, and 13 Post/mention rows.

The deterministic evaluator re-hashed `summary.json`, `updates.jsonl`, `events.jsonl`, and `chat_history.jsonl`, then
re-extracted every completed tool-call ID, provider-call ID, tool name, and argument from `updates.jsonl`. All `8/8`
rows matched the sanitized receipt.

This proves native-X tool execution for this run. It does not make the provider result bodies replayable: those bodies
remain encrypted in model context, while the retained candidate/evidence excerpts are model-mediated.

### Recomputed diagnostic metrics

| Metric | Result |
| --- | ---: |
| Candidate leads / completed tool call | `1.000` |
| Model-mediated Bio presence | `5/8 = 62.5%` |
| Model-mediated numeric platform-user-ID presence | `1/8 = 12.5%` |
| Model-mediated experience-Recall pool | `4/8 = 50.0%` |
| Evidence-qualified Precision tranche | `0/8 = 0%` |
| Model-mediated high-authority support coverage | `4/8 = 50.0%` |
| Third-party-only lead rate | `2/8 = 25.0%` |
| Provider Post-body replayability | `0/13 = 0%` |

The configured five-way lead segmentation was:

| Target-lab state | Pretraining state | Lead count | Interpretation |
| --- | --- | ---: | --- |
| current | current | `2` | Precision-oriented segment, but neither row yet passed the full delivery completeness rule |
| current | historical | `1` | Current target-lab researcher with historical pretraining experience; retained for Recall |
| historical | current | `0` | Former target-lab researcher still doing current pretraining; valid segment, absent in this sample |
| historical | historical | `1` | Target-lab alumnus with historical pretraining experience; retained for Recall |
| ambiguous/unsupported in either axis | any | `4` | Evidence/hydration queue, not silently excluded |

The two current/current rows still lacked stable numeric platform IDs, so segment membership must not be reported as
business-ready Precision output. Historical rows were queued for missing identity or high-authority evidence, not
because historical state is itself negative.

“High-authority support coverage” only means that both dimension labels have at least one self/official/team evidence
row; it does not mean that an ambiguous label is resolved or that the packet is deliverable.

The scale verdict is `no_go`. Mechanical blockers include unreplayable provider Post bodies, no structured researcher
role/function field, Bios/IDs that are present only in model-mediated output rather than source snapshots, incomplete
Bio/ID presence, raw-session privacy below owner-only, and CLI/tool-disable settings that are not fully bound by the
raw session.

The final point is operationally important: sanitized project artifacts are `0600` under an owner-only runtime
directory, but the original `~/.grok` session directory/files are currently `0755/0644` under a `0750` home. The raw
session is therefore potentially group-readable and cannot be described as owner-only private. No permission or VPN/
proxy setting was changed during this audit.

## Luna business-semantics result

The corrected live request returned HTTP `200` from the configured relay with exact model `gpt-5.6-luna` in `26,349
ms`. Usage was 1,136 input, 1,299 output, 200 reasoning, and 2,435 total tokens. Four cases returned the requested
strict structure:

| Case | China professional-experience proxy | Asia proxy | Physical geography |
| --- | --- | --- | --- |
| Subject-owned Xiaohongshu + WeChat professional publishing | `strong_proxy` | `strong_proxy` | none asserted |
| Chinese-language pretraining/AI professional publishing | `weak_proxy` | `weak_proxy` | none asserted |
| Canada role + subject-owned Xiaohongshu AI activity | `strong_proxy` | `strong_proxy` | Canada explicit, independently |
| Chinese science-fiction interest | `none` | `none` | Canada explicit, independently |

All four records returned `protected_identity_inferred=false`. This supports an AI-first semantic lane for open Bio
text: the model proposes source-bound professional context, a deterministic versioned policy rolls it up, and a
separate high-recall verification queue may consume strong or weak proxies. It does not prove that every future model
label is correct, nor that the current semantic-v2.2 wire or Luna canary-v2 contract has passed live review.

## Private artifact integrity

File SHA-256 values:

- sanitized Grok result: `83dcc28f8234644a23f413cb4ffc3c60e315dffdf34836d1bad6fbe35b150b8a`;
- sanitized Grok tool receipt: `577108a63016903e20a5466dbf10301b96703a1a0e6daa8343a17cb1323db9c3`;
- legacy deterministic Grok evaluation: `26c2d9b9910c83d65fc1b200868d17a8c7f2b8f29e3acf91a528e51e848bdf01`;
- two-axis normalized Grok result: `0cdeb4d4586a1db7f0c5248a32dee0b11fc755564665dffa0d899506618a3163`;
- one-time two-axis migration receipt: `d1ba2c3abe45b3a0c340ca205704feb224c877bd71a2b09c1c7fbc1a1ed2b825`;
- superseded two-axis deterministic Grok evaluation: `32b9e43a4ca48cfcc1dd4bd8eff9420bf96ff727096bbc9fee4552b934b661bf`;
- superseded strict-output-contract two-axis Grok evaluation:
  `52eb7c7cac3f395eeeeb44759886d1660cb40721d8aa79bb873cdcc1b654cc59`;
- immutable-registry, source-replayed two-axis Grok evaluation:
  `eb1e08d4d64c386d27a8fa75115afafe9a55490ad7277d5922a0cdbf6eed9ffc`;
- public hash-only query-policy descriptor canonical SHA-256:
  `034cd6850993aee4388175cabab04482a2fb932fffc13f9d15c601cc083bcdd6`;
- public query-policy registry canonical SHA-256:
  `821a508d0e2cc5a384ee904561d80b6d02f9dce0a515a31a48f99f8720dcf177`;
- owner-only legacy full-policy canonical SHA-256 bound by both public descriptor and registry:
  `5a8e61b34a9ab920e82a011e7534b65a264710a72df5352a302562f59a380b1e`;
- receipt-only replay evaluation under the public descriptor contract:
  `f8dcee9534a223ab9f14cac22ca8d2187832c7d4bbc435b9c683a8a3aab00f48`;
- exploration-evaluation schema: `b8ae72f3e78d0b935586df4883539e8f0bed9c2df7cbdbc861d68dd95a20692f`;
- hydration-task schema: `94e156fa64c9f891216c72657e2b3253faec9d2a088b25aa1bbee63dd85d3814`;
- public query-policy descriptor schema: `a24e582f86b05a96631586127bba4ea2dd7b90269196caf0bb7dcdabe6490f56`;
- public query-policy registry schema: `f013108f20c3cee11bb225249953c9eef8aa469ee90625f60aef363eb955ac26`;
- sanitized Luna result: `70f93268177af6d327a436274237950be6d5ff7121eb3e5658e59500da13c5ba`;
- Luna response-body digest recorded by the caller:
  `d391c2a90fdaa49e6544aec98d756167d63a9d1ac085518fae56c82b53d181dc`.

The ignored project runtime files are mode `0600`; no credential is present in this Markdown record.

The legacy result and the earlier evaluation were not overwritten. A one-time private migration receipt binds its exact source and target hashes,
records the field/state mapping, and records the mechanically recomputed five-way counts. The normal evaluator accepts
only the source-neutral two-axis fields; the old OpenAI-specific aggregate buckets are not an alternate authority.
The tracked query-policy file is now an intended-public hash-only descriptor. It contains only the opaque
session/request binding digest plus ordered `sequence`, `tool_name`, and exact full-call SHA-256 values; query
arguments, handles, Bio text, Post URLs, and raw session/request identifiers remain owner-only. The immutable public
registry separately binds the public descriptor preimage/hash and the legacy full-policy canonical hash. During
offline replay, the evaluator validates the private receipt arguments, recomputes every full-call hash, and requires
an exact ordered match. It does not accept a caller-supplied policy body.

The old private result and receipt replayed successfully without reading or copying their query text into tracked
files. The resulting replacement evaluation is mode `0600` and remains `receipt_only_unverified` because the original
raw session directory was not supplied in that replay. The prior evaluation remains immutable historical evidence,
but its pre-migration output shape is not a current-contract substitute. Re-validation requires the private source
artifacts; no detached evaluation can validate itself.

The intended-public synthetic fixture
`fixtures/grok_cli_exploration_pre_migration_evaluation.v1.json` records only the pre-migration shape transform and
expected validator behavior. It contains no source result, receipt, query operand, profile value, credential, or raw
run identifier. Its regression proves that the detached old shape fails the current validator, while a synthetic
source result plus receipt can be replayed into a current evaluation with both descriptor and legacy-policy bindings.

## Next bounded experiment

Do not interpret the diagnostic hydration tasks as executable approval. The next reviewed slice must first:

1. isolate raw Grok session evidence under owner-only permissions with a short TTL and deletion receipt;
2. bind a profile task to lab, frozen window, prompt/model/tool-policy hashes, stable account identity, and exact call
   receipts;
3. test no more than five leads for numeric platform ID, exact Bio/hash/observed time, canonical Post ID/URL/author/
   time, and thread relationship;
4. keep model-mediated excerpts explicitly non-replayable unless a source/citation path independently verifies them;
5. run Luna semantic review only after the Bio snapshot is bound, then join via platform user ID + Bio hash;
6. measure field coverage and human-reviewed precision before increasing task or Post volume.
