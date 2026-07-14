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

- opaque v2 HMAC run commitment `d6f54c517e46a434d9b249aa7320be77b7920cb3d21d2e7e2e5d44390e672db9`;
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

- owner-only v2 commitment receipt file: `4f597160c705dc3c013570c13c45a978d3ee30a6fe891b03f1eadf6ff04fd073`;
- owner-only decision-code v2 result file: `eca67c7370ccf562ac5076aea44ef96c194313a7ecbaaf90ef4d2a52a9231041`;
- owner-only source-replayed evaluation v1 file: `71fd3f353339ef858ae579e1101a955b8ab154b7dc57186ba8648cd88aeb5ef6`;
- public keyed-commitment descriptor v2 canonical SHA-256:
  `31f797c5a870faa0997a278f496af1693aae801cce4c54afdc123e2427793d1d`;
- public query-policy registry v2 canonical SHA-256:
  `3d3283253e007f4686fd5ad60b20edde3be2c3eb04e762790aafc68bc464986f`;
- exploration-evaluation v1 schema: `61ecb2704dd3cf0363f2325dc4cacabe67444d94fa29a6bb64d8a7ecaa385cbc`;
- hydration-task v1 schema: `8bdac4a0516973d3eeb90a8e0103d6dd809ab9f8e891a830c0490f43f4038caa`;
- public query-policy descriptor v2 schema: `9c2cf0681396fa6e82de462c218ac57ba99e4a9fbb5db658a3f118a7592cbdcc`;
- public query-policy registry v2 schema: `612b6af6f7aaa78c4b9c8824bbc0c7b0fa02c59c046e29ef0d1ca229a8cf5e0a`;
- sanitized Luna result: `70f93268177af6d327a436274237950be6d5ff7121eb3e5658e59500da13c5ba`;
- Luna response-body digest recorded by the caller:
  `d391c2a90fdaa49e6544aec98d756167d63a9d1ac085518fae56c82b53d181dc`.

These hashes record the original 2026-07-14 migration evidence. The subsequent offline hardening added issuance-lineage
bindings, a durable migration/purge state machine, and stronger schemas, so the public descriptor/registry/schema hashes
above are historical evidence rather than current-contract signoff. No private artifact was rewritten and no live call
was made during that repair; promotion requires a new pinned independent review.

The ignored project runtime files are mode `0600`; no credential is present in this Markdown record.

The legacy result and the earlier evaluation were not overwritten. A one-time private migration receipt binds its
exact source and target hashes, records the field/state mapping, and records the mechanically recomputed five-way
counts. The normal evaluator accepts only the source-neutral two-axis fields; the old OpenAI-specific aggregate
buckets are not an alternate authority.

The tracked v2 query-policy descriptor contains only run-bound, domain-separated HMAC commitments plus public key and
nonce identifiers. Raw query arguments, handles, Bio text, Post URLs, raw session/request identifiers, key material,
and nonce material remain only in the owner-only `0600` receipt. The immutable public registry separately binds the
public descriptor and the keyed legacy-policy commitment. During offline replay, the evaluator validates private
receipt arguments against those commitments and requires an exact ordered match; it does not accept a caller-supplied
policy body. The prior unsalted v1 descriptor and registry were deleted from the intended-public HEAD because their
small query space was dictionary-recoverable. Their values remain a Git-history residual until an explicitly approved
history rewrite; they are not accepted by the current evaluator.

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
3. hydrate every selected lead that fits the reviewed technical byte/node/depth/deadline envelope for numeric platform
   ID, exact Bio/hash/observed time, canonical Post ID/URL/author/time, and thread relationship;
4. keep model-mediated excerpts explicitly non-replayable unless a source/citation path independently verifies them;
5. run Luna semantic review only after the Bio snapshot is bound, then join via platform user ID + Bio hash;
6. measure exact-denominator field coverage and human-reviewed precision while allowing the adaptive search to keep
   expanding until evidence saturation, budget, deadline, cancellation, or a reviewed safety boundary stops it.
