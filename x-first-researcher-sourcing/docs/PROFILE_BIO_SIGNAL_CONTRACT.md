# X profile/Bio signal contract

> Status: fixture-only, provider-neutral evidence-proposal lane. It performs no network, model, provider, discovery,
> ranking, identity merge, employment confirmation, canonical write, export, or outreach action.

## Goal

Preserve the high-recall value of public X Bio text without pretending that an X display name is a verified real name
or collapsing several different facts into one ambiguous "China experience" score.

The lane distinguishes five facts:

1. `display_alias`: a raw mutable alias only;
2. `observed_chinese_content`: directly observed Han-script content, recorded as a `zh` language candidate rather than
   proof of fluency, nationality, ethnicity, or physical location;
3. `china_ecosystem_experience_lead`: an explicit subject-authored Bio claim about a registered China professional or
   publishing ecosystem, such as a Xiaohongshu account or WeChat official account;
4. `organization_mention`: a subject-claimed current, previous, or unspecified organization relation that remains
   unresolved until the mentioned account and relation are independently checked;
5. `physical_region_experience`: a separate owner handled by the region-experience contract and never inferred from
   language, ecosystem activity, name, handle, or a Bio mention.

This means an explicit Bio statement analogous to "同名小红书…，我的公众号…" may support a
`china_digital_ecosystem` experience lead. It does not by itself say that the person lived, studied, researched, or
worked in Mainland China. A statement analogous to `Head of ... @org` or `Prev @org` creates current/previous
affiliation proposals and graph edges; it does not confirm employment.

The repository fixture is synthetic and does not copy a real profile or handle.

## AI-native boundary

Version 1 is deliberately fixture-only. This repository does not yet have an independently verifiable native-profile
receipt that can bind a completed provider tool call, stable X account ID, handle, Bio field, and Bio content hash.
Adding more self-reported fields would not create that trust root, so the v1 runtime and schemas accept only:

- `retrieval_mode=offline_fixture`;
- a reserved `.invalid` profile URL whose final path component equals `current_handle`;
- a null `native_profile_receipt_ref` and null proposal `tool_call_id`;
- an exact fixture Bio SHA-256 and exact Unicode proposal span/excerpt SHA-256.

`grok_x_native`, an `x.com` profile URL, or any purported `xcall_*` receipt fails closed. Analysis reports
`fixture_only_not_live_proven`, never `source_bound`. A future profile capability runner/schema v2 must separately
establish the receipt trust root before native profile evidence can enter this contract.

Model prose cannot create a profile field, stable identity, organization relation, or experience lead. The executable
validator rechecks fixture source spans and the canonical version-pinned policy before producing deterministic output;
it cannot access Grok, X, OAuth, or another provider.

## Owner and source-of-truth matrix

| Concern | Owner/source of truth | Rule |
| --- | --- | --- |
| Fixture account reference | Synthetic fixture | Numeric-shaped `platform_user_id` is deterministic fixture data, not live identity proof |
| Provisional subject | X-first identity contract | Opaque `pp_x_<ULID>`; no name/model merge |
| Live profile field availability | Deferred profile capability v2 | V1 has no native receipt trust root and rejects live/native claims |
| Raw Bio | Fixture profile snapshot | Exact synthetic text, observation time, content version, and SHA-256 |
| Extracted proposal | `x.profile.bio_evidence.bundle.v1` | Exact fixture Bio span and excerpt hash; null tool receipt |
| Ecosystem registry | `profile_bio_signal_policy.v1.json` | Canonical hash-pinned aliases and `{alias}` ownership templates |
| Affiliation relation | Evidence proposal | Exact handle clause contains only the declared relation; output remains unresolved |
| Physical location experience | Region-experience evidence owner | Never derived from this lane |
| Canonical person/employment/assertion | Existing product owners | No writes or confirmation from this lane |

## High-recall use

Within a population already selected by lab, current professional affiliation, role/research evidence, and pretraining
relevance, these outputs can expand the human verification queue for China-related professional experience. They are
not permitted to discover or rank people by inferred ethnicity, nationality, race, citizenship, or real name.

The recall ladder is additive and explainable:

```text
raw fixture profile field
  -> exact fixture-bound proposal
  -> observed language / ecosystem / organization lead
  -> human or independent-source verification
  -> separately owned confirmed professional or region evidence
```

No step may jump from Chinese text to nationality, from display alias to real identity, from ecosystem activity to
physical presence, or from a Bio mention to confirmed employment.

## Closed v1 semantics

- Ecosystems: `xiaohongshu`, `wechat_official_account`.
- Proposal kinds: `observed_chinese_content`, `china_ecosystem_self_claim`, `organization_mention`.
- Affiliation relations: `current`, `previous`, `unspecified`.
- V1 transport is only `offline_fixture`; native mode, live URLs, and non-null receipts are invalid.
- Ecosystem support requires one closed ownership template, such as `同名{alias}` or `我的{alias}`, to match within
  one statement. Topic/follower co-occurrence such as `小红书用户有很多粉丝` is insufficient.
- Current/previous affiliation requires an exact `@handle` and only the declared registered relation in the same
  line/punctuation-delimited clause. Bare `前` is not a marker; explicit `Prev`, `曾任`, or `前任职于` forms remain
  supported, and an explicit previous marker takes precedence over a role phrase such as `Engineer at` in that clause.
- Handle-only organization proposals remain `unresolved`; target platform user ID is null.
- All display-name, protected-identity, confirmation, discovery/ranking, and canonical-write claims are fixed false.

The runtime pins the canonical SHA-256 of the complete policy, while the policy schema pins the same complete JSON
value. Any alias, ownership template, relation marker, limit, forbidden field, or output-state change under the same
`policy_version` fails closed. Such a change requires a version bump, schema/runtime/hash/fixture/test update, and
non-author review rather than a hidden prompt change.

## Current limits

This slice proves only offline field and evidence semantics. It does not prove that the installed Grok CLI returns a
Bio, that every native X result includes a numeric author ID, that organization mentions resolve to accounts, or that
the API can batch profile enrichment. A separate profile capability runner/schema v2 must prove field-level receipt
and account/content co-binding; it remains a later owner decision beyond the post-only X-live runner.

## Validation

From `x-first-researcher-sourcing/`:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m unittest tests.test_profile_bio_signal_contract -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m x_first.profile_bio_signals \
  fixtures/profile_bio_evidence_fixture_v1.json \
  --policy configs/profile_bio_signal_policy.v1.json
../sourcing-ai-agent/.venv/bin/ruff check \
  src/x_first/profile_bio_signals.py tests/test_profile_bio_signal_contract.py
```

Run the full sibling unittest discovery before another batch consumes this artifact. Green fixture tests do not
authorize live X access, researcher mapping, profile enrichment, scale-up, or product writes.
