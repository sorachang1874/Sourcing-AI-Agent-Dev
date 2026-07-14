# X profile/Bio signal contract

> Status: fixture-only deterministic baseline and regression guardrail. It performs no network, model, provider,
> discovery, ranking, identity merge, employment confirmation, canonical write, export, or outreach action. It is not
> the primary open-text recall classifier; the separately versioned model-native semantic-review v2 lane owns that
> future role.

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

Version 1 is deliberately fixture-only and deliberately bounded to a closed grammar. It remains useful as a
deterministic champion, a source of adversarial fixtures, and a hard-regression oracle; adding aliases to it is not the
strategy for covering arbitrary X Bio prose. The model-native semantic-review v2 design instead lets a fast model
propose meanings and reasons from the complete Bio, while deterministic code validates exact source spans, hashes,
authority and prohibited outputs.

This repository does not yet have an independently verifiable native-profile
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
| Ecosystem registry | `profile_bio_signal_policy.v1.json`, policy v1.2 | Canonical hash-pinned aliases, ownership templates, bare-identifier opt-in, post-claim guards, and non-ownership continuations |
| Affiliation relation | Evidence proposal | Role text, relation marker, and sole target handle share one punctuation clause; temporal/intent ambiguity fails closed and output remains unresolved |
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
- Ecosystem support requires one registered ownership form anchored at the start of a punctuation-delimited clause.
  Xiaohongshu accepts closed forms such as `同名{alias}`, `{alias}同名`, and `我的{alias}`. The WeChat official-account
  registry additionally opts into `公众号 <bounded account identifier>` so a Bio can name a channel without an
  artificial `我的` prefix. This opt-in is per ecosystem, not a generic alias/topic heuristic.
- Negative prefixes, post-claim negation, third-party operation, and non-account objects fail closed. A positive
  template cannot win before a later `不是`/`并非`; `由朋友运营` cannot become subject ownership; and account/list/topic
  continuations such as `用户画像`, `关注列表`, `用户有`, `研究`, or `推荐` are not self-claims. The narrower `用户数`
  continuation remains supported to preserve explainable recall for a claimed account's audience count.
- Current/previous affiliation requires an exact `@handle` and only the declared registered relation in the same
  line/punctuation-delimited clause. Bare `前` is not a marker; explicit `Prev`, `曾任`, or `前任职于` forms remain
  supported, and an explicit previous marker takes precedence over a role phrase such as `Engineer at` in that clause.
  Registered negation or recruiting contexts such as `Not Head of`, `Looking for Head of`, `从未任职于`, and
  `Not Previously` invalidate the relation proposal.
- `role_text`, its relation marker, and the sole target handle must be bound to the same punctuation clause. A
  role-bearing clause with multiple handles is ambiguous and rejected. A multi-handle statement such as `Prev @a @b`
  may still produce one handle-bound unresolved proposal per target only when `role_text=null`.
- V1.2 has no reversible output state for past-current or future/intended affiliations. `Ex-`, `Was`, `Past`,
  `Aspiring`, `Incoming`, and `Future` clauses are therefore rejected rather than mislabeled as `current`.
- Handle-only organization proposals remain `unresolved`; target platform user ID is null.
- All display-name, protected-identity, confirmation, discovery/ranking, and canonical-write claims are fixed false.

### Positive and negative grammar table

| Synthetic Bio clause | Result | Reason |
| --- | --- | --- |
| `同名小红书四万粉丝` | Xiaohongshu ecosystem lead | Registered subject-first same-name form |
| `小红书同名四万粉丝` | Xiaohongshu ecosystem lead | Registered alias-first same-name form |
| `我的小红书用户数四万` | Xiaohongshu ecosystem lead | Explicit subject claim plus bounded audience-count continuation |
| `公众号 SyntheticFounder（长文首发）` | WeChat ecosystem lead | Ecosystem-specific bare account identifier opt-in |
| `我的小红书不是我的账号` | Reject | Post-template negation |
| `同名小红书并非本人运营` | Reject | Post-template ownership negation |
| `我的小红书账号由朋友运营` | Reject | Third-party operation |
| `我的小红书关注列表` | Reject | Non-account/list object |
| `Future Head of @synthetic_hub` | Reject | Future intent has no truthful v1.2 affiliation state |
| `Head of growth @synthetic_hub @another_org` with a role | Reject | Role-bearing clause has multiple target handles |

All positive ecosystem rows produce only a `china_digital_ecosystem` verification lead. They leave
`physical_region_experience.status=not_evaluated` and every ethnicity/nationality/name inference claim false.

The runtime pins the canonical SHA-256 of the complete policy, while the policy schema pins the same complete JSON
value. Policy v1.2 records the post-template/third-party ownership guards, explicit high-recall account forms,
temporal/intent affiliation rejects, clause-bound roles, and validation traversal budgets added after adversarial
review. Any
alias, ownership template, context guard, relation marker, limit, forbidden field, or output-state change under the
same `policy_version` fails closed. Such a change requires another version bump, schema/runtime/hash/fixture/test
update, and non-author review rather than a hidden prompt change. Runtime validation is terminal-total for all
single-field nested JSON type substitutions covered by the fixture mutation corpus. Iterative traversal stops at 64
nested container levels or 4,096 scheduled nodes and returns typed validation errors before recursive equality or
hashing. The CLI also converts parser recursion failures into a bounded `input_unreadable_or_invalid` result without a
traceback.

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
