# Provider Runtime Documentation Route

> Status: Current Phase-1 module router. Existing provider contracts and playbooks at their flat paths remain canonical.

## Boundary

Owns external provider/model execution modes, queues, invocation envelopes,
rate/cost/retry policy, webhook/result handling, and provider-specific operator
guidance.

Does not own population selection, business coverage proof, or public result
rendering.

Code boundary examples: `src/sourcing_agent/provider_*`,
`src/sourcing_agent/harvest_connectors.py`,
`src/sourcing_agent/dataforseo_client.py`, and `src/sourcing_agent/model_*`.

## Canonical Routes

| Question | Canonical document | Status/read condition |
| --- | --- | --- |
| Model-native provider behavior/fail-closed boundary | [MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md](../../MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md) | active contract |
| Provider task runtime, budgets and batching | [M2_PROVIDER_TASK_RUNTIME_DESIGN.md](../../M2_PROVIDER_TASK_RUNTIME_DESIGN.md) | design/current rollout reference; verify implementation state |
| Provider queue ownership | [DISCOVERY_PROVIDER_QUEUE_CONTRACT.md](../../DISCOVERY_PROVIDER_QUEUE_CONTRACT.md) | active cross-module contract |
| Provider/cache mode isolation | [RUNTIME_ENVIRONMENT_ISOLATION.md](../../RUNTIME_ENVIRONMENT_ISOLATION.md) | active contract |
| Query boundaries | [QUERY_GUARDRAILS.md](../../QUERY_GUARDRAILS.md) | active guardrails |
| HarvestAPI operations | [HARVESTAPI_PLAYBOOK.md](../../HARVESTAPI_PLAYBOOK.md) | provider-specific playbook |
| DataForSEO operations | [DATAFORSEO_PLAYBOOK.md](../../DATAFORSEO_PLAYBOOK.md) | provider-specific playbook |
| Apify webhook operations | [APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md](../../APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md) | provider-specific playbook |

## Upstream and Downstream Routes

- Population/coverage strategy: [planning and acquisition](../planning-acquisition/README.md)
- Durable commands/recovery: [workflow runtime](../workflow-runtime/README.md)
- Model tools/Agent result contracts: [agent runtime](../agent-runtime/README.md)
- Runtime/test/deploy environment: [platform operations](../platform-operations/README.md)

## New Artifact Placement

- Contracts: `docs/modules/provider-runtime/contracts/`
- Architecture/decisions: `docs/modules/provider-runtime/architecture/` and `decisions/`
- Testing/operations: `docs/modules/provider-runtime/testing/` and `operations/`
- Migrations/reviews: `docs/modules/provider-runtime/migrations/` and `reviews/`

## Validation Route

Prefer simulate, replay, or scripted validation. Any non-live run must prove
provider mode and cache isolation. Live validation requires a matching
independent `GO`, scoped cost/budget review, and the provider-specific preflight;
do not infer permission from a successful local test.

## Route Maintenance

Owner: linked contract owners; route steward: repository maintainers. Re-audit
when a provider, model route, invocation schema, queue owner, cost policy,
webhook, retry, or mode boundary changes.
