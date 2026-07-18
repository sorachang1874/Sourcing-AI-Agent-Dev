# Agent Runtime Documentation Route

> Status: Current Phase-1 module router. Existing Track D and contract documents at their flat paths remain canonical.

## Boundary

Owns Agent action/tool declaration and activation, OperationRun/AgentAction
surfaces, approval/control semantics, tool result slots, model-safe projections,
and `served` activation gates.

Does not own general workflow command execution, provider transport, or public
candidate projection behavior; route those to the linked downstream modules.

Code boundary examples: `src/sourcing_agent/action_*`,
`src/sourcing_agent/agent_*`, `src/sourcing_agent/operation_runtime.py`, and
`src/sourcing_agent/model_tool_runtime.py`.

## Canonical Routes

| Question | Canonical document | Status/read condition |
| --- | --- | --- |
| Agent action, approval, budget, idempotency, external boundary | [AGENT_OPERATION_CONTRACT.md](../../AGENT_OPERATION_CONTRACT.md) | active contract; read before action/Operation changes |
| Current Agent rollout, ordering, served/live gates | [TRACK_D_AGENT_RUNTIME_PLAN.md](../../TRACK_D_AGENT_RUNTIME_PLAN.md) | active plan; confirm current batch in the project snapshots |
| Pre-Agent contract/owner matrix | [PRE_AGENT_CONTRACT_REVIEW.md](../../PRE_AGENT_CONTRACT_REVIEW.md) | active review matrix; contract-heavy changes require independent review |
| Tool/model invocation surface | [TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md](../../TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md) | design baseline; use current D0/D1 artifact for the exact slice |
| Current implementation/review artifact | [INDEX.md Tier 3](../../INDEX.md) | choose the exact Track D artifact; do not read all Track D docs by default |
| Independent review procedure | [INDEPENDENT_REVIEW_GATE.md](../../INDEPENDENT_REVIEW_GATE.md) | required for activation, contract, owner, or milestone claims |

## Downstream Routes

- Workflow command/activity/causality: [workflow runtime](../workflow-runtime/README.md)
- Public projection reads/results: [serving and product](../serving-product/README.md)
- CRM/person actions: [CRM and person assets](../crm-person-assets/README.md)
- Provider/model transport: [provider runtime](../provider-runtime/README.md)

## New Artifact Placement

- Contracts: `docs/modules/agent-runtime/contracts/`
- Architecture and decisions: `docs/modules/agent-runtime/architecture/` and `decisions/`
- Batch implementation/review: `docs/modules/agent-runtime/reviews/`
- Test guidance and operations: `docs/modules/agent-runtime/testing/` and `operations/`

Create these directories when the first new artifact is added; do not duplicate
an existing flat canonical document merely to populate them.

## Validation Route

Use the exact targeted tests named by the active Track D artifact, then the D1
contract/preflight set and Operation runtime adjacency required by its review
packet. A green author test is not an independent review verdict. Keep
`served=0` unless the exact activation gate passes.

## Route Maintenance

Owner: linked contract owners; route steward: repository maintainers. Re-audit
when action partitions, tool registry ownership, result schemas, or the Track D
serving boundary changes.
