# X-first sibling architecture

> Current authority: offline contracts plus bounded author-evidence explorations. This document is not a scale,
> product-write, outreach, or formal-review `GO`.

## Goal and ownership boundary

`x-first-researcher-sourcing` is a standalone evidence-source experiment. It may produce versioned artifacts, but it
does not import the `sourcing-ai-agent` runtime and cannot write canonical person, evidence, assertion, CRM,
projection, export, or outreach state.

The sibling has isolated fixture layers plus bounded diagnostic lanes:

1. **Stage 0 collection fixture** — `x.grok.collection.v1` validates synthetic account, observation, candidate-packet,
   coverage, identity, relevance, and quarantine contracts.
2. **Stage 1 capability-probe fixture** — `x.grok.capability_probe.request.v1` and
   `x.grok.capability_probe.result.v1` validate an account-level probe envelope, request hash binding, terminal-total
   states, provenance, zero-external-call accounting, evidence minimization, and fail-closed boundaries.
3. **Grok CLI exploration diagnostic** — validates a sanitized model-mediated result against a raw-session hash/call
   receipt, computes field gaps, and emits non-executable hydration diagnostics.
4. **Profile Bio semantic lane** — proposes exact-span professional context, then applies a separate versioned China/
   Asia professional-experience proxy policy; proposals remain unverified and cannot write product state.
5. **Adaptive Grok wave runner** — prepares a closed native-X-only invocation under a preissued one-shot grant,
   isolated auth/home, process-group recovery ledger, hard deadline and technical kill ceilings. It has no
   candidate/observation/call success cap and remains unpromoted until pinned review.
6. **Recall-pool replay/merge** — reconstructs raw assistant output and native-X call ledgers, casefold-merges handles,
   quarantines invalid evidence associations, and emits a private source-replay-bound campaign artifact while keeping
   candidate evidence, Bios and platform IDs explicitly model-mediated and unverified.
7. **Reported profile-text semantic lane** — performs provider-free, terminal-total batch adjudication of
   `grok_model_mediated_unverified_text` into strong/weak/none professional-experience verification-queue proposals;
   it preserves opaque campaign/text bindings and cannot establish platform identity or physical-region experience.

Fixture validation executes no provider, model, network client, database, or product adapter. Stage 1 v1 is deliberately
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

separate adaptive live runner (implemented, not promoted)
        |
        +--- requires preissued grant + pinned independent review before a new live execution
```

## Population and identity boundary

Stage 0's frozen v1 fixture selection is limited to lab, current professional affiliation proposal, and pretraining
relevance. It is a narrower regression baseline rather than the candidate-value authority.
Stage 1 fixture does not select a population at all: it targets one synthetic official-lab account and does not mint
provisional people, infer affiliation or relevance, traverse a graph, identify researchers, or rank anyone.

Protected identity and its proxies are prohibited from discovery, ranking, request, result, and promotion contracts.
Names, language, region, school, community, bios, posts, mentions, and graph position cannot be used to infer
ethnicity, nationality, race, citizenship, religion, gender, or another protected identity.

After the base researcher population is established, a separate high-recall verification queue may consume
source-bound `strong_proxy|weak_proxy` China/Asia professional-experience proposals. These are professional-activity
signals, not protected-identity proxies or physical-region labels. Their policy/provenance remain explicit and they
cannot independently authorize final ranking, eligibility, outreach, or canonical writes.

Candidate value is a separate two-axis contract. `target_lab_affiliation_state` and
`pretraining_experience_state` each use `current|historical|ambiguous|unsupported`. The four current/historical
combinations remain in the experience-recall pool; only evidence-complete current/current is eligible for the default
precision delivery. Segment priority and hydration behavior are versioned policy, while `lab_id` comes from the
approved query policy—no lab name belongs in the evaluator algorithm.

## Artifact ownership

| Contract | Owner/source of truth | Current consumers | Promotion rule |
| --- | --- | --- | --- |
| Stage 0 collection fixture | Stage 0 schema + executable validator | Stage 0 tests and CLI only | No direct promotion |
| Stage 1 request fixture | Stage 1 request schema + executable validator | Stage 1 generator/tests/CLI only | Cannot authorize execution |
| Stage 1 result fixture | Stage 1 result schema + executable validator | Stage 1 generator/tests/CLI only | Cannot prove X access |
| Adaptive Grok live wave | Request/grant/process/receipt schemas + hardened runner | Explicit operator execution only | Pinned review plus one-shot grant; no product promotion |
| Recall-pool campaign | Raw replay receipts + campaign merger | Private evaluation only | Source trust remains field-specific; no canonical write |
| Reported profile text | Opaque campaign/text-hash observation + offline adjudicator | Verification queue proposal only | New source-bound artifact required for any trust upgrade |
| External account identity | Stable platform user ID | Fixture-local references | Handle remains mutable evidence |
| Canonical person/assertion | Existing product owners | None in this sibling | Separate adjudication and adapter gate |
| Live provider/model/runtime | Unassigned pending owner decision | None | New version, implementation, review, explicit trigger |

## Dependency direction

The sibling depends only on the Python standard library and its own contracts. A future integration must consume a
versioned artifact through a separate adapter and preserve:

```text
search -> fetch/verify -> adjudicate -> materialize
```

The fixture-only Stage 1 contract stops before search. The separately governed exploration and adaptive-runner lanes
may perform explicit native-X search, but every lane still stops before product materialization. The sibling must
never become a hidden fallback for LinkedIn-first discovery or a canonical writer.

## Change rules

- Do not add a live mode to `x.grok.collection.v1` or the fixture-only Stage 1 v1 contracts.
- A new or revised live contract needs an explicit version bump, owner-approved
  account/model/access/cost/deadline/retention pins, a user-triggered request, and independent review.
- Keep schema and executable validator changes in the same batch with adversarial regression tests.
- Unknown state, provenance, identity, budget, or retention values fail closed.
- Before enabling a second lab fixture, replace the exact OpenAI literals with the reviewed
  `CapabilityFixtureProfile` registry defined by the extraction condition in
  `STAGE1_CAPABILITY_FIXTURE_CONTRACT.md`; v1 remains deliberately exact until then.
- Reviews block only promotion/live/manual/product signoff for their scope; unrelated offline work may continue.
