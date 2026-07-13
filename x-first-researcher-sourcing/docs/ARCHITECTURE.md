# X-first sibling architecture

> Current authority: offline synthetic contracts only. This document is not a live-access approval or formal review
> `GO`.

## Goal and ownership boundary

`x-first-researcher-sourcing` is a standalone evidence-source experiment. It may produce versioned artifacts, but it
does not import the `sourcing-ai-agent` runtime and cannot write canonical person, evidence, assertion, CRM,
projection, export, or outreach state.

The sibling has two isolated offline layers:

1. **Stage 0 collection fixture** — `x.grok.collection.v1` validates synthetic account, observation, candidate-packet,
   coverage, identity, relevance, and quarantine contracts.
2. **Stage 1 capability-probe fixture** — `x.grok.capability_probe.request.v1` and
   `x.grok.capability_probe.result.v1` validate an account-level probe envelope, request hash binding, terminal-total
   states, provenance, zero-external-call accounting, evidence minimization, and fail-closed boundaries.

Neither layer executes a provider, model, network client, database, or product adapter. Stage 1 v1 is deliberately
`execution_mode=fixture_only`; it cannot represent or validate a live result and cannot emit `x_native_proven`.

```text
synthetic generators
        |
        v
strict JSON shapes ---> executable semantic validators ---> deterministic tests
        |                         |
        |                         +--- reject live URLs, credentials, unsafe fields,
        |                              budget/state drift, full bodies, and writers
        v
versioned fixture artifacts

future live probe (not implemented)
        |
        +--- requires owner decisions + new contract version + independent review
```

## Population and identity boundary

Stage 0 fixture selection is limited to lab, current professional affiliation proposal, and pretraining relevance.
Stage 1 fixture does not select a population at all: it targets one synthetic official-lab account and does not mint
provisional people, infer affiliation or relevance, traverse a graph, identify researchers, or rank anyone.

Protected identity and its proxies are prohibited from discovery, ranking, request, result, and promotion contracts.
Names, language, region, school, community, bios, posts, mentions, and graph position cannot be used to infer
ethnicity, nationality, race, citizenship, religion, gender, or another protected identity.

## Artifact ownership

| Contract | Owner/source of truth | Current consumers | Promotion rule |
| --- | --- | --- | --- |
| Stage 0 collection fixture | Stage 0 schema + executable validator | Stage 0 tests and CLI only | No direct promotion |
| Stage 1 request fixture | Stage 1 request schema + executable validator | Stage 1 generator/tests/CLI only | Cannot authorize execution |
| Stage 1 result fixture | Stage 1 result schema + executable validator | Stage 1 generator/tests/CLI only | Cannot prove X access |
| External account identity | Stable platform user ID | Fixture-local references | Handle remains mutable evidence |
| Canonical person/assertion | Existing product owners | None in this sibling | Separate adjudication and adapter gate |
| Live provider/model/runtime | Unassigned pending owner decision | None | New version, implementation, review, explicit trigger |

## Dependency direction

The sibling depends only on the Python standard library and its own contracts. A future integration must consume a
versioned artifact through a separate adapter and preserve:

```text
search -> fetch/verify -> adjudicate -> materialize
```

The current sibling stops before search in Stage 1 and before materialization in every stage. It must never become a
hidden fallback for LinkedIn-first discovery or a canonical writer.

## Change rules

- Do not add a live mode to `x.grok.collection.v1` or the fixture-only Stage 1 v1 contracts.
- A future live contract needs an explicit version bump, owner-approved account/model/access/cost/deadline/retention
  pins, a user-triggered request, and independent review.
- Keep schema and executable validator changes in the same batch with adversarial regression tests.
- Unknown state, provenance, identity, budget, or retention values fail closed.
- Before enabling a second lab fixture, replace the exact OpenAI literals with the reviewed
  `CapabilityFixtureProfile` registry defined by the extraction condition in
  `STAGE1_CAPABILITY_FIXTURE_CONTRACT.md`; v1 remains deliberately exact until then.
- Reviews block only promotion/live/manual/product signoff for their scope; unrelated offline work may continue.
