# Region-experience evidence contract

> Status: provider-neutral, deterministic evidence-classification slice. It performs no network, provider, model,
> identity merge, canonical write, ranking, export, or outreach action. The labels describe only directly evidenced
> public professional experience; they are not ethnicity, nationality, citizenship, ancestry, language, or protected
> identity claims.

## Purpose

This contract replaces the unsafe parts of legacy outreach layering with three evidence-backed labels:

1. `ASIA_EXPERIENCE`
2. `GREATER_CHINA_EXPERIENCE`
3. `MAINLAND_CHINA_EXPERIENCE`

For this product contract only, the Greater China operational grouping contains Mainland China, Hong Kong, Macau,
Taiwan, and Singapore. This grouping is an explicit location-experience taxonomy, not a geopolitical, nationality,
citizenship, ethnicity, or identity conclusion. Mainland evidence supports all three labels. Hong Kong, Macau, Taiwan,
Singapore, or explicit Greater China evidence supports the first two. Other registered Asia locations support only
`ASIA_EXPERIENCE`.

The v1 location registry is intentionally closed and non-exhaustive. An unregistered location yields no label rather
than a guessed mapping.

## Contract files and owners

| Concern | Owner/source of truth | Rule |
| --- | --- | --- |
| Policy and hierarchy | `configs/region_experience_policy.v1.json` plus executable validator | Exact v1 locations, aliases, source rules, and forbidden fields |
| Evidence bundle | `x.region_experience.evidence_bundle.v1` | Public-professional evidence proposals only |
| Classification | `x.region_experience.classification.v1` | Deterministic derivation bound to the complete input SHA-256 |
| Location matching | Exact normalized alias registry | Unicode NFKC, whitespace collapse, case-fold, then whole-value equality |
| Evidence eligibility | Source-kind rule registry | Explicit subject experience, allowed actor, allowed experience relation, exact location alias |
| Product/canonical state | Existing product owners | No writes or promotion from this slice |

The Python owner is `src/x_first/region_experience.py`. It has no `sourcing_agent` import and no provider/model/network
dependency. Future integration must consume its versioned artifact through an adapter; it must not import product
writers into the sibling.

## Eligible evidence

Every evidence item has an opaque subject reference, stable evidence ID, source URL, bounded excerpt, canonical
observation timestamp, structured source/actor/scope/experience relation, and an exact registered location value.

| `source_kind` | Allowed actor | Allowed subject experience |
| --- | --- | --- |
| `bio` | `subject_self` | `based_in`, `lived_in` |
| `employment` | `subject_self`, `verified_organization` | `based_in`, `worked_in` |
| `education` | `subject_self`, `verified_organization` | `studied_in` |
| `research` | `subject_self`, `verified_organization` | `researched_in` |
| `post` | `subject_self` only | Explicit first-person `based_in`, `lived_in`, `worked_in`, `studied_in`, or `researched_in` |

An education or research record must carry an explicit structured location. A school, lab, conference, paper title,
or organization name is not itself a location alias. This prevents the old behavior in which university names or
arbitrary profile substrings silently became regional identity signals.

## Evidence that cannot support a label

- name, display name, surname, script, or handle;
- language or bilingual signals;
- school name without an explicit location-experience relation;
- social graph position, list membership, follows, replies, or graph neighbors;
- a third-party mention of the subject;
- a post that merely discusses, links to, reports on, or performs research *about* a location;
- substring or token hits such as `Chinatown`, `Mainland-inspired`, or `Singaporean`;
- unregistered or mismatched location aliases;
- an actor/source/experience relation outside the closed policy.

Forbidden fields fail the evidence bundle closed. Topic-only and third-party-mention observations may remain in the
bundle for audit, but receive a rejected evidence decision and appear in no label's `evidence_refs`.

The bounded `excerpt` is evidence for a reviewer; the deterministic classifier never token-scans it. Changing a name,
language phrase, handle-like token, or place discussion inside an excerpt cannot change a label. Only the closed
structured fields can do so.

## Exact evidence-reference contract

The result always contains exactly three label records. Each supported label contains the complete, sorted set of
eligible evidence IDs whose registered location belongs to that label. Unsupported labels contain an empty list.

The result also records one terminal decision per input evidence item:

- `eligible_explicit_experience`;
- `rejected_non_subject_scope`;
- `rejected_source_actor`;
- `rejected_experience_kind`;
- `rejected_location_alias`.

The validator recomputes the entire result from the bound evidence bundle. Missing or extra evidence references,
altered label status, changed claims, hash drift, or a changed evidence decision fail closed.

## Protected-identity and action boundary

The result fixes all of the following claims to false:

- protected identity, ethnicity, nationality, or citizenship inferred;
- name, handle, language, social graph, or third-party mention used;
- canonical writes or outreach authorized.

Region-experience labels must not be interpreted as proof or a proxy for ethnicity, nationality, citizenship, race,
religion, gender, ancestry, community membership, cultural identity, or outreach permission. They must not be used to
reconstruct a protected population. Any later ranking or product action requires its own owner-reviewed contract.

## Synthetic fixture

The deterministic fixture uses only opaque synthetic subjects and `.invalid` URLs. It covers all five source kinds,
all three hierarchy levels, an Asia-only education record, a topic-only post, and a third-party mention. The last two
are retained as rejected audit decisions and never support a label.

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

The full sibling unittest discovery should also remain green before this contract is reused by another batch.
