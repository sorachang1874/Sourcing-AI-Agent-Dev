# Platform Operations Documentation Route

> Status: Current Phase-1 module router. Existing storage, testing, runtime, deployment, and review documents at their flat paths remain canonical.

## Boundary

Owns PG/storage migration infrastructure, test/runtime environment isolation,
local service bootstrap, hosted deployment mechanics, asset retention, and
independent-review infrastructure.

Does not own domain field semantics, which remain with the relevant module
contract even when persisted or deployed by this layer.

Code boundary examples: `src/sourcing_agent/storage.py`,
`src/sourcing_agent/migration_runner.py`, local/runtime scripts, migrations,
and deployment configuration.

## Canonical Routes

| Question | Canonical document | Status/read condition |
| --- | --- | --- |
| Test tiers and gates | [TESTING_PLAYBOOK.md](../../TESTING_PLAYBOOK.md) | active playbook |
| Isolated test environment | [TEST_ENVIRONMENT.md](../../TEST_ENVIRONMENT.md) | active environment contract |
| Local/scripted/hosted preflight | [RUNTIME_PREFLIGHT.md](../../RUNTIME_PREFLIGHT.md) | required before runtime commands |
| Local PG control plane | [LOCAL_POSTGRES_CONTROL_PLANE.md](../../LOCAL_POSTGRES_CONTROL_PLANE.md) | active operations route |
| PG-only storage migration | [TRACK_B_PG_PURE_STORE_DESIGN.md](../../TRACK_B_PG_PURE_STORE_DESIGN.md) | active design/owner route |
| Remaining PG cutover debt | [PG_ONLY_CUTOVER_TRACKER.md](../../PG_ONLY_CUTOVER_TRACKER.md) | active tracker; confirm snapshot status |
| Runtime asset retention | [RUNTIME_ASSET_RETENTION_GOVERNANCE.md](../../RUNTIME_ASSET_RETENTION_GOVERNANCE.md) | active governance |
| Hosted deploy/GitHub scope | [HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md](../../HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md) | active operations boundary |
| Independent review gate | [INDEPENDENT_REVIEW_GATE.md](../../INDEPENDENT_REVIEW_GATE.md) | required for named gate triggers |

## Domain Routes

- Workflow/runtime semantics: [workflow runtime](../workflow-runtime/README.md)
- Provider execution/cost/mode: [provider runtime](../provider-runtime/README.md)
- API/projection consumers: [serving and product](../serving-product/README.md)
- Agent activation/review scope: [agent runtime](../agent-runtime/README.md)

## New Artifact Placement

- Contracts: `docs/modules/platform-operations/contracts/`
- Architecture/decisions: `docs/modules/platform-operations/architecture/` and `decisions/`
- Testing/operations: `docs/modules/platform-operations/testing/` and `operations/`
- Migrations/reviews: `docs/modules/platform-operations/migrations/` and `reviews/`

## Validation Route

Use repository `.venv-tests`/`.venv` interpreters and the mode-specific
preflight. Storage/migration changes require PG-backed tests and rollback/
partial-failure evidence. Review-runner changes must validate their own control
plane and cannot self-certify a product scope.

## Route Maintenance

Owner: linked document owners; route steward: repository maintainers. Re-audit
when storage authority, migration runner, environment modes, deploy topology,
retention policy, or independent-review machinery changes.
