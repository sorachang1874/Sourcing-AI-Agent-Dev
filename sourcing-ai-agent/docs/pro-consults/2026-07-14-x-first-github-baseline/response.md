> Status: Reference (consult transcript, 2026-07-14 era). Decisions were promoted into their owning docs; do not treat dated numbers here as current.

ADVISORY_ONLY — not an independent-review artifact or formal GO.

X-first fixture lane baseline advisory
1. Connector evidence
Commit-pinned retrieval status

Repository: sorachang1874/Sourcing-AI-Agent-Dev

Immutable content authority: commit SHA 5ab9798b5e8e1935cc1520abc82204c9261f5fef

Connector retrieval result:

Repository retrieval succeeded.

Exact full commit SHA was retrieved from the connector. The commit object reports SHA 5ab9798b5e8e1935cc1520abc82204c9261f5fef.

Commit title: docs: record owner-accepted exception — Track D design finalized at v8.

Branch verification:

branch_verification=connector_unavailable

The connector did not provide authoritative mutable branch-ref verification in this session.

No claim is made that the connector verified the branch ref.

Local git state was not used as repository evidence and was not reviewed.

Requested file retrieval evidence

All six requested files were retrieved from the connector at the pinned commit content authority:

/AGENTS.md

Retrieved content identifies workspace engineering rules and contract ownership requirements.

Contract field ownership rules require owner, source of truth, allowed values, derivation rules, consumers, fallback status, and deletion conditions before changing shared fields.

/sourcing-ai-agent/AGENTS.md

Retrieved repository-specific rules identify the product as recruiting/public-information enrichment infrastructure.

Repository rules require explicit contracts, canonical sources of truth, and avoidance of hidden fallback ladders.

/sourcing-ai-agent/docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md

Retrieved handoff states Track C C1a had not started implementation and that the handoff is not a formal review artifact.

It explicitly states author evidence is not independent review GO.

/sourcing-ai-agent/docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md

Retrieved C1 design states C1a authorizes no schema migration, production rollout, or live provider/model call.

C1a scope is transport/client correction only, with no storage change and no provider/model call.

/sourcing-ai-agent/docs/QUERY_GUARDRAILS.md

Retrieved guardrails prohibit protected-attribute targeting/ranking/filtering.

The document requires evidence-backed, career-relevant signals and rejects protected-attribute inference or proxying.

/sourcing-ai-agent/docs/MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md

Retrieved provider contract states model-native search must remain a separate provider source and cannot become hidden fallback behavior.

Current normal behavior is fail closed unless the provider contract is explicitly implemented.

2. Source-neutral contracts/modules the X-first fixture lane should reuse

The X-first fixture lane should remain a fixture/evidence lane, not a new provider execution path.

Recommended reusable contracts:

Area	Reuse boundary
Query interpretation	Existing query guardrail model: structured criteria, evidence policy, manual review boundaries
Evidence handling	Existing evidence-oriented flow: normalize evidence, preserve provenance, avoid raw-source promotion
Provider abstraction	Existing search-provider contract boundaries rather than adding X-specific runtime semantics
Entity resolution	Existing identity-resolution concepts only for dedupe/matching, not identity inference
Ranking inputs	Career-relevant signals only: lab, current affiliation, role, publications, pretraining relevance
Contract ownership	Existing owner/source-of-truth matrix pattern before adding fields

The lane should not create a parallel “X ranking system.” It should exercise existing evidence contracts with synthetic fixtures.

3. LinkedIn-specific or canonical-writer modules it must not couple to

The X-first fixture lane must not directly couple to:

LinkedIn-specific profile writers.

LinkedIn URL repair flows.

Provider-specific profile fetch implementations.

Canonical person asset writers.

CRM/projection/export writers.

Any live provider ingestion path.

Reasoning:

The repository requires shared contracts to have explicit owners and prohibits hidden fallback ladders.

Model-native or supplemental search sources must not directly promote assertions or bypass evidence adjudication.

The fixture lane may produce test evidence artifacts, but it must not become a new canonical writer.

4. Conflicts between an X-first plan and actual repository contracts

Potential conflicts:

Proposed shortcut	Contract conflict
Treat X posts as authoritative profile truth	Violates evidence provenance and adjudication ownership
Add X as a silent fallback when LinkedIn fails	Violates provider isolation and fail-closed provider contracts
Directly write candidate/person assets from X fixtures	Violates canonical writer ownership
Rank using inferred identity attributes from X content	Violates query guardrails
Add a provider-specific schema before owner matrix approval	Violates contract field ownership rules

The correct interpretation is:

X can be a future evidence source.

X cannot silently become a replacement source of truth.

X fixture work should validate evidence plumbing, not product ranking decisions.

5. Smallest fixture-only sibling folder/doc/schema/test slice

Suggested minimal slice:

sourcing-ai-agent/
  fixtures/
    x_first/
      README.md
      sample_posts.json
      sample_profiles.json
      expected_evidence.json
  docs/
    X_FIRST_FIXTURE_LANE.md
  tests/
    test_x_first_fixture_contract.py

Constraints:

No provider/model call.

No network access.

No credential usage.

No database writes.

No canonical asset writes.

No CRM/projection/export writes.

No production configuration changes.

Fixture schema should contain only:

fixture_id
source_type = "x_fixture"
public_reference
text/content sample
timestamp if present
claimed signals
expected evidence labels
adjudication expectation

It should not contain inferred identity attributes.

6. Owner/source-of-truth matrix, fail-closed rules, validation checklist
Owner/source-of-truth matrix
Field/domain	Owner	Source of truth
Fixture identity	fixture test owner	fixture manifest
Evidence provenance	evidence owner	normalized evidence record
Candidate/person identity	existing identity owner	canonical identity contracts
Ranking decision	ranking/planning owner	structured criteria + evidence
Export/publication	export/projection owners	canonical projection/export contracts
Fail-closed state rules

Unknown provider/source type: reject.

Missing evidence provenance: do not promote.

Ambiguous identity match: retain unresolved/manual-review state.

Missing owner for a new contract field: do not introduce field into normal path.

Unsupported ranking signal: exclude from ranking.

Protected-attribute proxy signal: reject.

Local validation checklist

Fixture-only validation:

 Fixture schema validates.

 Evidence provenance is preserved.

 No provider invocation occurs.

 No model invocation occurs.

 No database writes occur.

 No canonical writer is called.

 No protected attribute inference appears.

 Ranking explanation maps only to allowed career signals.

 Contract ownership is documented before any shared field addition.

7. Protected-trait guardrail

Discovery/ranking population must use only:

lab affiliation,

current affiliation,

role/function,

publication/research relevance,

pretraining relevance,

other auditable professional signals.

The system must not infer or proxy ethnicity, nationality, or protected identity through:

name,

language,

region,

school,

community,

biography,

posts,

mentions,

social graph.

Names may be used for entity resolution, but not identity inference. Language or regional experience may be used for outreach/context signals only, not as a proxy for protected identity.

8. Decisions
Accepted

X-first work may exist as a fixture/evidence experiment.

The experiment should reuse existing evidence and contract boundaries.

Initial implementation should be offline, fixture-only, and fail closed.

Deferred

Live X ingestion.

X provider integration.

Ranking changes.

Person asset promotion rules.

CRM/export integration.

Requires owner approval

Any new schema field.

Any new provider registration.

Any canonical writer change.

Any ranking criterion change.

Any live provider/model experiment.
