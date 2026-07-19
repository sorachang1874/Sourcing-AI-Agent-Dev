# Serving and Product Documentation Route

> Status: Current Phase-1 module router. Existing projection, frontend, and product documents at their flat paths remain canonical.

## Boundary

Owns canonical serving projections, public readers, API response semantics,
frontend adapters/state/copy, and product/prototype interpretation of sourcing
results.

Does not own upstream acquisition, CRM mutation, or workflow repair.

Code boundary examples: `src/sourcing_agent/serving_projection_*`,
`src/sourcing_agent/api.py`, `contracts/`, and `frontend-demo/`.

## Canonical Routes

| Question | Canonical document | Status/read condition |
| --- | --- | --- |
| Projection identity, readiness and public reader behavior | [CANONICAL_SERVING_PROJECTION_CONTRACT.md](../../CANONICAL_SERVING_PROJECTION_CONTRACT.md) | active contract |
| Frontend/backend API fields and adapters | [FRONTEND_API_CONTRACT.md](../../FRONTEND_API_CONTRACT.md) | active contract; schema/adapter under `contracts/` |
| Cohort/filter membership semantics | [COHORT_SELECTION_CONTRACT.md](../../COHORT_SELECTION_CONTRACT.md) | active contract |
| Filter v2 publication foundation boundary | [TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md](decisions/TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md) | `foundation_only_unbound`; `64a7dfc` Ultra `NO-GO 0/10/2/2`; lineage/product-owner batches pending |
| Filter v2 S1f0a review response | [TRACK_D_D1N_S1F0A_FILTER_PROJECTION_OWNER_REVIEW_RESPONSE.md](decisions/TRACK_D_D1N_S1F0A_FILTER_PROJECTION_OWNER_REVIEW_RESPONSE.md) | finding-by-finding retraction/closure and S1f0b-S1f1 dependency graph |
| Filter v2 foundation machine contract | [filter_projection_foundation_boundary_v1.json](contracts/filter_projection_foundation_boundary_v1.json) | closed S1f0a fixed-forward status, dependency graph, writer inventory, and forbidden claims |
| Filter v2 operation-native lineage and terminal-owner decision | [TRACK_D_D1N_S1F0C_LINEAGE_TERMINAL_OWNER_DECISION.md](decisions/TRACK_D_D1N_S1F0C_LINEAGE_TERMINAL_OWNER_DECISION.md) | S1f0c0 author candidate; future start propagation, exact recompile, commit-once Cohort terminal, and final projection authority; no runtime/served delta |
| Filter v2 lineage and terminal-owner machine decision | [filter_projection_lineage_terminal_owner_decision_v1.json](contracts/filter_projection_lineage_terminal_owner_decision_v1.json) | closed decision shape and S1f0c1-S1f1 dependency graph; fresh pinned review pending |
| Filter v2 fixed-forward lineage decision | [TRACK_D_D1N_S1F0C_LINEAGE_FIXED_FORWARD_DECISION.md](decisions/TRACK_D_D1N_S1F0C_LINEAGE_FIXED_FORWARD_DECISION.md) | S1f0c FF-DI canonization author candidate; append-only literal lock, 22 full-schema contract digests with acyclic dependency DAG, two retained root/owner-ref closed-schema pins with exact-version-and-digest references, design-evidence closure map, 100 machine-executed eight-row join predicates (digest-pinned external preview validator, full requester-binding scope comparisons, per-predicate hostile mutations), six PG relations under three owners with typed descriptors and one-to-one invariant enforcement (exact digest constants/bounds, composite scoped FKs, repository-enforced classification, opaque `membership_revision` text token, lane provider-evidence UoW binding, terminal append-only transition_guard split), migration `0015` reservation, product terminal core/envelope digests, V3 exact union discrimination with closed item schemas, closed `cohort_selection` object, and complete ActionResultSpec/AgentToolSpec fingerprint decision-lock (64-KiB/8192-item/depth-10 limits, empty artifact schemes, owner-pin digest recomputation); machine contract [filter_projection_lineage_fixed_forward_decision_v1.json](contracts/filter_projection_lineage_fixed_forward_decision_v1.json); no runtime/served delta, scope-matched review pending |
| Filter v2 S1f0b foundation implementation | [TRACK_D_D1N_S1F0B_FILTER_PROJECTION_FOUNDATION_IMPLEMENTATION.md](implementation/TRACK_D_D1N_S1F0B_FILTER_PROJECTION_FOUNDATION_IMPLEMENTATION.md) | non-product carrier reservation plus generic/native preserve-or-invalidate policy in the existing projection UoW; served remains zero |
| User-visible workflow progress | [WORKFLOW_PROGRESS_CONTRACT.md](../../WORKFLOW_PROGRESS_CONTRACT.md) | active cross-module contract; writer owned by workflow runtime |
| Product requirements | [PRD.md](../../PRD.md) | background reference; active contracts win on drift |
| Backend/product prototype boundary | [BACKEND_MVP.md](../../BACKEND_MVP.md) | background reference; verify current implementation |
| Intent explanation and plan-review experience | [INTENT_PLANNING_BRIEF.md](../../INTENT_PLANNING_BRIEF.md) | product reference; route execution semantics upstream |
| Outreach/product layering | [OUTREACH_LAYERING.md](../../OUTREACH_LAYERING.md) | product reference; CRM contract owns persisted engagement state |

## Upstream Routes

- Projection writers and workflow progress: [workflow runtime](../workflow-runtime/README.md)
- Acquisition/coverage inputs: [planning and acquisition](../planning-acquisition/README.md)
- CRM/person overlays and mutations: [CRM and person assets](../crm-person-assets/README.md)
- Agent-callable query surfaces: [agent runtime](../agent-runtime/README.md)

## New Artifact Placement

- Contracts: `docs/modules/serving-product/contracts/`
- Product requirements/prototypes: `docs/modules/serving-product/product/`
- Architecture/decisions: `docs/modules/serving-product/architecture/` and `decisions/`
- Implementations: `docs/modules/serving-product/implementation/`
- Testing/operations: `docs/modules/serving-product/testing/` and `operations/`
- Migrations/reviews: `docs/modules/serving-product/migrations/` and `reviews/`

## Validation Route

Run the fast frontend/API contract preflight and targeted public-reader tests,
then `cd frontend-demo && npm run build` for frontend changes. Validate in-flight
state and cross-endpoint parity, not only terminal rendering. Public readers
must stay fail-closed and read-only.

## Route Maintenance

Owner: linked contract owners; route steward: repository maintainers. Re-audit
when projection fields/readiness, API adapters, frontend copy/state, product
prototype status, or public reader ownership changes.
