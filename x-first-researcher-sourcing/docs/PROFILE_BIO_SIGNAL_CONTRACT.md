# X profile/Bio signal contract

> Status: fixture-only deterministic baseline and regression guardrail. It performs no network, model, provider,
> discovery, ranking, identity merge, employment confirmation, canonical write, export, or outreach action. It is not
> the primary open-text recall classifier; the separately versioned Luna-native semantic-review v2 lane owns that
> role.

## Goal

Preserve the high-recall value of public X Bio text without pretending that an X display name is a verified real name
or collapsing several different facts into one ambiguous "China experience" score.

The lane distinguishes five facts:

1. `display_alias`: a raw mutable alias only;
2. `observed_chinese_content`: directly observed Han-script content, recorded as a `zh` language candidate rather than
   proof of fluency, nationality, ethnicity, or physical location;
3. `china_ecosystem_experience_lead`: an explicit subject-authored Bio claim about a registered China professional or
   publishing ecosystem, such as a Xiaohongshu account or WeChat official account;
4. `organization_mention`: a subject-claimed current or previous organization relation that remains
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
deterministic conservative champion, a source of adversarial fixtures, and a hard-regression oracle; adding aliases or
negative phrases to it is not the strategy for covering arbitrary X Bio prose. The Luna-native semantic-review v2
design instead lets a fast model
propose meanings and reasons from the complete Bio, while deterministic code validates exact source spans, hashes,
authority and prohibited outputs.

This repository does not yet have an independently verifiable native-profile
receipt that can bind a completed provider tool call, stable X account ID, handle, Bio field, and Bio content hash.
Adding more self-reported fields would not create that trust root, so the v1 runtime and schemas accept only:

- `retrieval_mode=offline_fixture`;
- a canonical lowercase-scheme/lowercase-host `.invalid` profile URL whose final path component equals
  `current_handle`;
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
| Positive grammar | `profile_bio_signal_policy.v1.json`, policy v1.5 | Canonical hash-pinned complete regex-AST: aliases, template-to-continuation mappings, continuation regex bodies/notes, affiliation regex bodies, handle binding, role extraction, and Unicode rules; there is no negative-phrase denylist |
| Grammar interpreter | Policy `grammar_runtime_binding` plus loaded runtime source digest | Policy validation fails closed when the loaded interpreter source does not match the implementation digest pinned inside the canonical policy |
| Affiliation relation | Evidence proposal | The complete normalized claim window matches exactly one positive current/previous grammar; a role is the exact parsed role span and output remains unresolved |
| Physical location experience | Region-experience evidence owner | Never derived from this lane |
| Canonical person/employment/assertion | Existing product owners | No writes or confirmation from this lane |

## Downstream use

Within a population already selected by lab, current professional affiliation, role/research evidence, and pretraining
relevance, these outputs can expand the human verification queue for China-related professional experience. V1.5
intentionally gives up open-text recall for deterministic precision; Luna v2 owns semantic recall. Neither lane is
permitted to discover or rank people by inferred ethnicity, nationality, race, citizenship, or real name.

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
- Affiliation relations: `current`, `previous`. V1.5 has no positive grammar or output for `unspecified`.
- V1 transport is only `offline_fixture`; native mode, live URLs, and non-null receipts are invalid.
- Fixture profile URLs require the exact lowercase `https://` scheme and a lowercase `.invalid` host. Proposal
  `extractor.version` is non-empty and at most 100 characters in both runtime and JSON Schema.
- Ecosystem support requires the complete NFKC-normalized proposal claim window to match one registered positive
  ownership form plus zero or one closed continuation. Prefix matching, a second clause, and unconsumed leading or
  trailing tokens are invalid.
  Xiaohongshu accepts closed forms such as `同名{alias}`, `{alias}同名`, and `我的{alias}`. The WeChat official-account
  registry additionally opts into `公众号 <bounded account identifier>` so a Bio can name a channel without an
  artificial `我的` prefix. Account identifiers, ownership nouns, audience counts, and the exact positive note
  `（长文首发）` are the only continuation classes. Each form owns its continuation-ID list: the account-ending
  `my {alias} account` form accepts no subsequent account identifier, while the WeChat-specific Chinese form can.
  Arbitrary parenthetical prose is not a positive continuation.
- Rejection is structural, not denylist-based: a negative, third-party, topic/list, past-current, future, intended, or
  otherwise unknown expression simply cannot full-match a positive grammar. Adding another negative synonym cannot
  make the champion safer and is not a supported maintenance strategy.
- Current affiliation accepts only the policy-owned positive regex-AST for
  `Head of <growth|research|engineering|AI> @handle`,
  `Founder/Researcher/Engineer at @handle`, `任职于 @handle`, and `就职于 @handle`. Previous affiliation accepts only
  `Prev/Previously/Formerly @handle...`, `Former <Researcher|Engineer|Founder|closed Head-of role> @handle`,
  `曾任 @handle...`, `前任职于 @handle`, and `此前任职于 @handle` forms. There is no open role body that can absorb a
  negative, temporal, or second clause. The complete window must match exactly one grammar for the declared relation.
- `role_text` is not independently trusted input. A role-bearing positive grammar parses one exact bounded role span;
  the proposal must equal that span exactly after NFKC normalization. A handle, handle prefix, bare preposition,
  relation-only fragment, or free substring is invalid. Role-bearing grammars permit one handle. Closed `Prev`/`曾任`
  handle lists may contain multiple handles only with `role_text=null`.
- After NFKC normalization, Unicode `Cc` control and `Cf` format characters make an ownership or affiliation claim
  invalid. Zero-width characters therefore cannot split a negative/temporal expression into a false positive.
- Handle-only organization proposals remain `unresolved`; target platform user ID is null.
- All display-name, protected-identity, confirmation, discovery/ranking, and canonical-write claims are fixed false.

### Positive and negative grammar table

| Synthetic Bio clause | Result | Reason |
| --- | --- | --- |
| `同名小红书四万粉丝` | Xiaohongshu ecosystem lead | Registered subject-first same-name form |
| `小红书同名四万粉丝` | Xiaohongshu ecosystem lead | Registered alias-first same-name form |
| `我的小红书用户数四万` | Xiaohongshu ecosystem lead | Explicit subject claim plus bounded audience-count continuation |
| `公众号 SyntheticFounder（长文首发）` | WeChat ecosystem lead | Ecosystem-specific bare account identifier opt-in |
| `我的小红书不是我的账号` | Reject | The suffix is not a closed positive continuation |
| `同名小红书并非本人运营` | Reject | The complete window does not full-match a positive form |
| `我的小红书账号由朋友运营` | Reject | Arbitrary third-party prose is not a positive continuation |
| `我的小红书关注列表` | Reject | List/topic text is outside the positive grammar |
| `my Xiaohongshu account list` | Reject | The account-ending template has no account-identifier continuation |
| `公众号 SyntheticFounder 关注列表` | Reject | Bare account identifier has an unconsumed list suffix |
| `我的小红书账号。其实不属于我` | Reject | A second clause is unconsumed; no denylist lookup is required |
| `公众号 SyntheticFounder（朋友在运营）` | Reject | Parenthetical content is not the registered positive note |
| `Future Head of @synthetic_hub` | Reject | The prefix prevents a positive current full-match |
| `Head of growth. Not employed by @synthetic_hub` | Reject | A closed role cannot absorb a negative second clause |
| `Former never employed @synth_listen` | Reject | `never employed` is not a policy-owned previous-role alternative |
| `将任职于 @synthetic_hub` | Reject | The prefix prevents an exact `任职于` full-match |
| `Head of growth @synthetic_hub` with `role_text=@synthetic` | Reject | Declared role is not the exact parsed role span |
| `Fu​ture Head of @synthetic_hub` | Reject | `Cf` zero-width format character is invalid |

All positive ecosystem rows produce only a `china_digital_ecosystem` verification lead. They leave
`physical_region_experience.status=not_evaluated` and every ethnicity/nationality/name inference claim false.

The runtime pins the canonical SHA-256 of the complete policy, while the policy schema pins the same complete JSON
value. Policy v1.5 records the complete positive grammar manifest rather than grammar IDs alone: every regex body,
template-to-continuation mapping, exact parenthetical-note registry, handle-binding rule, closed parsed-role rule,
rejected Unicode category, and validation traversal budget is canonical policy data. The same policy pins a digest of
the loaded grammar interpreter's critical functions, so code drift or monkeypatching fails policy validation before a
bundle can be accepted. It contains no negative-phrase or temporal denylist. Any alias, ownership template,
continuation mapping or regex, positive affiliation grammar, interpreter implementation, role rule, limit, forbidden
field, or output-state change under the
same `policy_version` fails closed. Such a change requires another version bump, schema/runtime/hash/fixture/test
update, and non-author review rather than a hidden prompt change. Runtime validation is terminal-total for all
single-field nested JSON type substitutions covered by the fixture mutation corpus. A shared tree preflight rejects
non-Unicode-scalar strings before hashing, normalization, deterministic comparison, or CLI rendering. Iterative
traversal stops at 64 nested container levels or 4,096 scheduled nodes and returns typed validation errors before
recursive equality or hashing. The CLI also converts parser recursion failures into a bounded
`input_unreadable_or_invalid` result without a traceback.

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
