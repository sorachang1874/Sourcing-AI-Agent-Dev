# Workflow Runtime Documentation Route

> Status: Current Phase-1 module router. Existing workflow contracts and playbooks at their flat paths remain canonical.

## Boundary

Owns durable events, commands, activities, typed causality, completion policy,
leases, retries, recovery, scheduler/daemon behavior, and workflow progress
production.

Does not own product intent, provider pricing/policy, or frontend interpretation
of a public projection.

Code boundary examples: `src/sourcing_agent/durable_runtime.py`,
`src/sourcing_agent/recovery_*`, `src/sourcing_agent/worker_*`, and
`src/sourcing_agent/workflow_*`.

## Canonical Routes

| Question | Canonical document | Status/read condition |
| --- | --- | --- |
| Event/command/activity/causality and durable owner rules | [DURABLE_EXECUTION_RUNTIME_CONTRACT.md](../../DURABLE_EXECUTION_RUNTIME_CONTRACT.md) | active top-level contract |
| Progress fields, counts, board lifecycle | [WORKFLOW_PROGRESS_CONTRACT.md](../../WORKFLOW_PROGRESS_CONTRACT.md) | active contract; pair with serving projection consumers |
| Execution invariants and forbidden fallbacks | [EXECUTION_CONTRACT_GUARDRAILS.md](../../EXECUTION_CONTRACT_GUARDRAILS.md) | active guardrails |
| Product-visible workflow behavior | [WORKFLOW_BEHAVIOR_GUARDRAILS.md](../../WORKFLOW_BEHAVIOR_GUARDRAILS.md) | active behavior contract |
| CLI/API recovery and operations | [WORKFLOW_OPERATIONS_PLAYBOOK.md](../../WORKFLOW_OPERATIONS_PLAYBOOK.md) | active operations route |
| Runtime/provider mode isolation | [RUNTIME_ENVIRONMENT_ISOLATION.md](../../RUNTIME_ENVIRONMENT_ISOLATION.md) | active boundary contract |
| Recovery takeover design | [RECOVERY_TAKEOVER_INTENT_DESIGN.md](../../RECOVERY_TAKEOVER_INTENT_DESIGN.md) | design record; verify implementation state in current tracker |

## Upstream and Downstream Routes

- Plan/acquisition intent: [planning and acquisition](../planning-acquisition/README.md)
- Agent command/control surfaces: [agent runtime](../agent-runtime/README.md)
- Provider invocation mode/effects: [provider runtime](../provider-runtime/README.md)
- Projection/API consumers: [serving and product](../serving-product/README.md)

## New Artifact Placement

- Contracts: `docs/modules/workflow-runtime/contracts/`
- Architecture/decisions: `docs/modules/workflow-runtime/architecture/` and `decisions/`
- Testing/operations: `docs/modules/workflow-runtime/testing/` and `operations/`
- Migrations/reviews: `docs/modules/workflow-runtime/migrations/` and `reviews/`

## Validation Route

Start with the exact command/activity/recovery tests named by the changed
contract, run the fast contract preflight, and use PG-backed scripted smoke when
service-level recovery or scheduler confidence is required. Do not treat a
skip-capable external-runtime lane as evidence.

## Route Maintenance

Owner: linked contract owners; route steward: repository maintainers. Re-audit
when a writer, reducer, command owner, recovery daemon, progress producer, or
causality field changes.
