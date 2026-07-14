# Region-experience evidence contract

> Status: provider-neutral, deterministic, adjudication-aware evidence-classification slice. It performs no network,
> provider, model, identity merge, canonical write, discovery, ranking, export, or outreach action. The labels describe
> only directly evidenced public professional or personal experience in a location. They are not ethnicity,
> nationality, citizenship, ancestry, language, or protected-identity claims.

## Purpose and exact hierarchy

The contract exposes three evidence-backed labels:

1. `ASIA_EXPERIENCE`
2. `GREATER_CHINA_EXPERIENCE`
3. `MAINLAND_CHINA_EXPERIENCE`

The owner-approved v1 hierarchy is exact:

- Mainland China supports all three labels.
- Hong Kong, Macau, or Taiwan supports Asia and Greater China.
- Singapore supports **Asia only**.
- Other registered Asia locations support Asia only.

`GREATER_CHINA_EXPERIENCE` is an operational location-experience grouping containing Mainland China, Hong Kong,
Macau, and Taiwan. It is not a geopolitical or identity conclusion. The v1 Asia registry is deliberately closed and
non-exhaustive. A directly evidenced location outside the registry is retained as `unregistered_location`; it is not
silently converted into a negative result or guessed into a region.

## Contract owners

| Concern | Owner/source of truth | Rule |
| --- | --- | --- |
| Exact v1 policy | `configs/region_experience_policy.v1.json` plus `validate_policy()` | JSON Schema uses the complete policy as a `const`; runtime equality must agree |
| Evidence shape | `x.region_experience.evidence_bundle.v1` | Closed post-extraction/proposal/adjudication evidence bundle |
| Classification | `x.region_experience.classification.v1` | Complete deterministic recomputation from policy and input hashes |
| Opaque person identity | Existing X-first identity contract | `pp_x_<26-character ULID>` only |
| X account identity | X platform | Stable numeric `platform_user_id`; handle/display name are not identity keys or signals |
| Source/binding verification | Reviewed evidence adapter and adjudication owner | Must resolve source, author, binding, and adjudication references before admitting `eligible` evidence |
| Location matching | Exact normalized alias registry | NFKC, whitespace collapse, case-fold, then whole-value equality |
| Product/canonical state | Existing product owners | No writes, promotion, discovery, ranking, or outreach from this slice |

The Python owner is `src/x_first/region_experience.py`. It has no `sourcing_agent`, provider, model, or network
dependency. Future integration must consume the versioned artifact through an adapter and must not import product
writers into this sibling.

## Identity and evidence provenance

Names, handles, schools, language, and free text are never identity keys. Every bundle carries:

- one opaque provisional-person `subject_ref` in canonical `pp_x_<ULID>` form;
- the subject's stable numeric X `subject_platform_user_id`;
- one fixed-length opaque `evidence_id` and `source_record_id` per record;
- the stable numeric source-author X account ID;
- a public source URL, observation time, content version and content SHA-256;
- a bounded excerpt whose own SHA-256 is recomputed by the validator;
- independently structured source medium, publisher actor, statement scope, and experience relation;
- actor-to-subject relation and publisher-binding state;
- extraction, proposal, adjudication, and eligibility states;
- fixed-length binding/adjudication references where the state requires them;
- an actor-binding SHA-256 over the source/account/relation tuple and, after adjudication, an adjudication SHA-256
  over the exact binding, content, structured claim, location, status, review reference, and review time.

Repeated `source_record_id` values must have identical URL, author, medium, content hash/version, observation time,
and actor-binding digest. The classification binds both the complete evidence bundle and exact policy with SHA-256.

An opaque reference is not evidence merely because it is well-formed. A live adapter may emit an adjudicated eligible
record only after resolving the referenced source and matching binding/adjudication digests in its own reviewed
evidence ledger. This sibling recomputes both digests, checks internal closure, and refuses to promote pending
evidence; it is not an authorization to invent proof IDs or digests.

## Actor proof rules

`source_medium`, `publisher_actor`, and `experience_kind` are deliberately independent. A Bio can contain an explicit
worked/studied/researched statement, and an official organization Bio can be a verified professional record.

| Publisher actor | Required proof |
| --- | --- |
| `subject_self` | Source-author numeric platform ID equals subject platform ID; `same_external_account`; `self_account_match`; no synthetic binding ref |
| `verified_organization` | Different stable organization account ID; `verified_professional_record`; `verified_organization_binding`; non-null opaque binding ref resolved by the adapter |
| `third_party` | `unrelated_or_unknown`; `unverified`; never `eligible` |

Only `subject_explicit_experience` can be eligible. Topic discussion and third-party mention records may remain in the
bundle after adjudication as rejected audit evidence, but they support no region label.

## Evidence lifecycle

The four lifecycle fields are closed and monotonic for this artifact:

1. `extraction_status=extracted`
2. `proposal_status=proposed`
3. `adjudication_status=pending|adjudicated`
4. `eligibility_status=unresolved|eligible|rejected`

Pending evidence must be unresolved and have null adjudication reference/time. Adjudicated evidence must be eligible
or rejected and carry an opaque adjudication reference plus canonical timestamp. Only adjudicated eligible evidence
with valid actor proof and explicit subject experience reaches a location label.

## Output states and evidence-reference closure

Each evidence decision is one of:

- `eligible / eligible_explicit_experience`;
- `unregistered_location / eligible_unregistered_location`;
- `insufficient_evidence / pending_adjudication`;
- `rejected / rejected_by_adjudication`.

Every label is exactly one of:

- `supported`: at least one complete registered eligible evidence reference;
- `unregistered_location`: no supporting registered evidence, but at least one eligible unregistered location remains;
- `insufficient_evidence`: neither complete supporting evidence nor eligible unregistered evidence exists.

The result always contains exactly three labels in canonical order. `evidence_refs` and
`unregistered_evidence_refs` are complete sorted sets. The runtime validator recomputes the entire result, including
policy hash, input hash, decisions, label states, references, and fixed false claims.

## Location aliases and public-source URLs

Registered aliases use NFKC normalization, whitespace collapse, case-fold, and whole-value equality. Substrings,
zero-width insertions, and Unicode confusables do not match. Explicit Chinese location aliases are registry entries;
their script or language is not an identity signal.

Production evidence URLs must be canonical public HTTPS references on X or an exact/subdomain match of the reviewed
official-professional host registry. URLs are length bounded and reject credentials, query strings, fragments, ports,
private/localhost hosts, unknown hosts, and noncanonical X profile/post paths. Reserved `.invalid` hosts are accepted
only to support deterministic synthetic fixtures; they are never live evidence.

## Protected-identity, Bio, language, and mention boundary

The following never support a region label:

- display name, surname, name script, handle, raw alias, or handle history;
- language, bilingualism, or merely posting in Chinese;
- school/organization names without explicit experience and reviewed relation evidence;
- social graph, list membership, follows, replies, or graph neighbors;
- a third-party mention or location-topic discussion;
- a Bio statement that has not passed the actor/binding/adjudication boundary.

An explicit first-person language-capability declaration may later be modeled as a separate
`declared_language_capability` for human communication adaptation. It must not affect candidate eligibility, ranking,
region experience, nationality, or ethnicity. Observed Chinese text is only `observed_language` and does not create
that declaration.

Likewise, explicit subject-authored Bio evidence of activity in a China-specific professional/platform ecosystem may
later form a separate `china_ecosystem_lead` verification proposal. It does not prove physical worked/studied/lived/
researched experience. Bio strings such as `Head of ... @org` or `Prev @org` are subject-claimed current/previous
affiliation proposals or mention edges; they require organization or independent evidence before confirmation. These
lead/affiliation contracts are intentionally outside this region-only slice.

Region labels must never be used as a proxy to reconstruct ethnicity, nationality, citizenship, race, ancestry,
community membership, or cultural identity. They are not permitted inputs to X-first discovery or ranking. The result
fixes protected-identity inference, name/handle/language/social-graph use, discovery/ranking, canonical writes, and
outreach authorization to `false`.

## Schema/runtime parity and synthetic fixture

The deterministic fixture uses only opaque IDs and reserved `.invalid` URLs. It covers all five source media, subject
and organization proof, rejected topic/third-party evidence, all three hierarchy levels, Asia-only Singapore, and an
eligible unregistered location.

The test module includes a standard-library validator for the closed Draft 2020-12 subset used by these files. Its
bidirectional mutation corpus requires both declarative schema and runtime rejection for duplicate policy/evidence,
hierarchy drift, nonpublic URL, invalid calendar timestamp, empty/whitespace excerpt, and duplicate label order. The
runtime additionally owns cross-field identity equality, exact alias resolution, source-record consistency, complete
hash/reference recomputation, and lifecycle semantics that standard JSON Schema cannot compare across records.

## Validation

From `x-first-researcher-sourcing/`:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m unittest tests.test_region_experience_contract -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m x_first.region_experience
../sourcing-ai-agent/.venv/bin/ruff check \
  src/x_first/region_experience.py tests/test_region_experience_contract.py
```

Run full sibling unittest discovery before another batch consumes this artifact. Passing fixture tests do not
authorize live collection, discovery/ranking, product integration, or canonical writes.
