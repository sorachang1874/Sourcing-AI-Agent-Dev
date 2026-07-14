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

This means an explicit Bio statement analogous to "同名小红书…，公众号…" may support a
`china_digital_ecosystem` experience lead. It does not by itself say that the person lived, studied, researched, or
worked in Mainland China. A statement analogous to `Head of ... @org` or `Prev @org` creates current/previous
affiliation proposals and graph edges; it does not confirm employment.

The repository fixture is synthetic and does not copy a real profile or handle.

## AI-native boundary

The contract is post-extraction rather than keyword-ranking code. A future Grok X-native adapter may propose signal
records, but every proposal must bind to:

- the stable numeric X account ID;
- an immutable profile snapshot and Bio content SHA-256;
- a canonical profile URL;
- a provider field-capability receipt in live mode;
- an exact Unicode `span_start`/`span_end`, excerpt, and excerpt SHA-256;
- the exact native profile tool-call ID in live mode.

Model prose cannot create a profile field, stable identity, organization relation, or experience lead. The executable
validator rechecks the source span and closed policy before producing deterministic output. Fixture mode uses
`offline_fixture` and a null tool receipt; it cannot access Grok, X, OAuth, or another provider.

## Owner and source-of-truth matrix

| Concern | Owner/source of truth | Rule |
| --- | --- | --- |
| External X account | X platform | Stable numeric `platform_user_id`; handle and display alias are mutable attributes |
| Provisional subject | X-first identity contract | Opaque `pp_x_<ULID>`; no name/model merge |
| Profile field availability | Versioned native profile receipt | A missing Bio field is `unavailable`, never reconstructed from model memory |
| Raw Bio | Profile snapshot | Exact text, observation time, content version, and SHA-256 |
| Extracted proposal | `x.profile.bio_evidence.bundle.v1` | Exact Bio span and source-bound extractor receipt |
| Ecosystem registry | `profile_bio_signal_policy.v1.json` | Versioned aliases and explicit subject-claim markers |
| Affiliation relation | Evidence proposal | `current|previous|unspecified`, always unresolved in this slice |
| Physical location experience | Region-experience evidence owner | Never derived from this lane |
| Canonical person/employment/assertion | Existing product owners | No writes or confirmation from this lane |

## High-recall use

Within a population already selected by lab, current professional affiliation, role/research evidence, and pretraining
relevance, these outputs can expand the human verification queue for China-related professional experience. They are
not permitted to discover or rank people by inferred ethnicity, nationality, race, citizenship, or real name.

The recall ladder is additive and explainable:

```text
raw profile field
  -> exact source-bound proposal
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
- Ecosystem support requires both a registered ecosystem alias and an explicit subject-claim marker in the same
  source-bound excerpt.
- Current/previous affiliation requires an exact `@handle` and a registered relation marker in the same excerpt.
- Handle-only organization proposals remain `unresolved`; target platform user ID is null.
- All display-name, protected-identity, confirmation, discovery/ranking, and canonical-write claims are fixed false.

The policy registry is intentionally small. Adding a platform, alias, relation marker, or output state requires a
version bump, schema/runtime/test update, and non-author review rather than a hidden prompt change.

## Current limits

This slice proves only offline field and evidence semantics. It does not prove that the installed Grok CLI returns a
Bio, that every native X result includes a numeric author ID, that organization mentions resolve to accounts, or that
the API can batch profile enrichment. Those are field-level live capability questions and remain blocked behind the
X-live runner review and a supported Responses/Batch credential decision.

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
