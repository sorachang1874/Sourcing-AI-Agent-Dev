# Planning and Acquisition Documentation Route

> Status: Current Phase-1 module router. Existing intent, acquisition, Public Web, and asset documents at their flat paths remain canonical.

## Boundary

Owns effective request semantics, plan/review, population strategy, acquisition,
company identity/assets, Public Web evidence acquisition, coverage proof, and
asset promotion/reuse.

Does not own public projection serving, CRM engagement state, or provider
transport policy.

Code boundary examples: `src/sourcing_agent/request_*`,
`src/sourcing_agent/planning.py`, `src/sourcing_agent/acquisition*`,
`src/sourcing_agent/company_*`, and `src/sourcing_agent/public_web_*`.

## Canonical Routes

| Question | Canonical document | Status/read condition |
| --- | --- | --- |
| User intent, strategy and source priority | [INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md](../../INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md) | active contract |
| Authoritative asset/coverage production and promotion | [AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md](../../AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md) | active contract |
| Discovery queue ownership | [DISCOVERY_PROVIDER_QUEUE_CONTRACT.md](../../DISCOVERY_PROVIDER_QUEUE_CONTRACT.md) | active contract; provider execution routes downstream |
| Asset snapshot/scope/promotion governance | [DATA_ASSET_GOVERNANCE.md](../../DATA_ASSET_GOVERNANCE.md) | active governance |
| Service-grade architecture and rollout ordering | [SERVICE_GRADE_ARCHITECTURE_PLAN.md](../../SERVICE_GRADE_ARCHITECTURE_PLAN.md) | active plan; confirm current track in snapshots |
| Product intent/planning background | [INTENT_PLANNING_BRIEF.md](../../INTENT_PLANNING_BRIEF.md) | reference; verify against active contracts |
| Lead discovery methods | [LEAD_DISCOVERY_METHODS.md](../../LEAD_DISCOVERY_METHODS.md) | active guidance; provider rules still come from provider contracts |
| Current company/Public Web implementation tracks | [INDEX.md Tier 3](../../INDEX.md) | select the exact Track D/C document; do not bulk-load trackers |

## Upstream and Downstream Routes

- Provider queues and cost/mode: [provider runtime](../provider-runtime/README.md)
- Durable execution/recovery: [workflow runtime](../workflow-runtime/README.md)
- Public projection/API output: [serving and product](../serving-product/README.md)
- Person/CRM asset consumers: [CRM and person assets](../crm-person-assets/README.md)

## New Artifact Placement

- Contracts: `docs/modules/planning-acquisition/contracts/`
- Product/prototypes: `docs/modules/planning-acquisition/product/`
- Architecture/decisions: `docs/modules/planning-acquisition/architecture/` and `decisions/`
- Testing/operations: `docs/modules/planning-acquisition/testing/` and `operations/`
- Migrations/reviews: `docs/modules/planning-acquisition/migrations/` and `reviews/`

## Validation Route

Start with effective-request/owner preflight and targeted planning/acquisition
tests. For shared semantics, inspect an actual normalized request and coverage
artifact. Provider-backed validation must also prove invocation mode and cost
boundaries through the provider module route.

## Route Maintenance

Owner: linked contract owners; route steward: repository maintainers. Re-audit
when effective request fields, company identity, coverage/promotion, acquisition
lane ownership, or Public Web lifecycle changes.
