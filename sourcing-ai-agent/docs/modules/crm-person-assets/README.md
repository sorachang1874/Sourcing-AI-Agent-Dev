# CRM and Person Assets Documentation Route

> Status: Current Phase-1 module router. Existing CRM, person-asset, and Track D action documents at their flat paths remain canonical.

## Boundary

Owns person identity, CRM records and engagement lifecycle, person
assets/evidence/assertions, CRM-owned Public Web state, manual review resolution,
and outreach state.

Does not own company acquisition strategy, provider transport, or public
projection serving.

Code boundary examples: `src/sourcing_agent/crm_*`,
`src/sourcing_agent/person_*`, `src/sourcing_agent/manual_review*`, and
`src/sourcing_agent/outreach_layering.py`.

## Canonical Routes

| Question | Canonical document | Status/read condition |
| --- | --- | --- |
| CRM record, engagement state, audit and writer rules | [CRM_STATE_CONTRACT.md](../../CRM_STATE_CONTRACT.md) | active contract |
| Person identity, evidence and assertion boundary | [PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md](../../PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md) | active contract |
| Agent operation/control boundary | [AGENT_OPERATION_CONTRACT.md](../../AGENT_OPERATION_CONTRACT.md) | active cross-module contract |
| Outreach/user-layer behavior | [OUTREACH_LAYERING.md](../../OUTREACH_LAYERING.md) | product reference; persisted state remains CRM-contract-owned |
| Current CRM action activation/review artifacts | [INDEX.md Tier 3](../../INDEX.md) | select the exact D1f/D1h/D1j or later artifact |
| Public API/projection consumers | [FRONTEND_API_CONTRACT.md](../../FRONTEND_API_CONTRACT.md) | consumer contract; it must not repair CRM state |

## Upstream and Downstream Routes

- Company/public evidence acquisition: [planning and acquisition](../planning-acquisition/README.md)
- Agent action/binder/control: [agent runtime](../agent-runtime/README.md)
- Workflow command/UoW causality: [workflow runtime](../workflow-runtime/README.md)
- Public projection/frontend overlay: [serving and product](../serving-product/README.md)

## New Artifact Placement

- Contracts: `docs/modules/crm-person-assets/contracts/`
- Product/prototypes: `docs/modules/crm-person-assets/product/`
- Architecture/decisions: `docs/modules/crm-person-assets/architecture/` and `decisions/`
- Testing/operations: `docs/modules/crm-person-assets/testing/` and `operations/`
- Migrations/reviews: `docs/modules/crm-person-assets/migrations/` and `reviews/`

## Validation Route

Run exact-owner and zero-write negative tests before mutation-path tests. Shared
CRM/action changes must verify submit, dispatch, command owner, replay, and
public-reader consumers from the same canonical identity/owner contract.

## Route Maintenance

Owner: linked contract owners; route steward: repository maintainers. Re-audit
when person identity, workspace ownership, CRM writer UoWs, evidence/assertion
semantics, manual resolution, or CRM action activation changes.
