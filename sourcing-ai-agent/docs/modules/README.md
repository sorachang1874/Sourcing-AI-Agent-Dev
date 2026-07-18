# Module Documentation Registry

> Status: Current Phase-1 module-router registry. It classifies documentation ownership without moving existing canonical files.

Use the project [problem router](../README.md) first. This registry is the
bounded fallback when a change crosses modules or ownership is unclear.

## Active Module Indexes

| Module | Owns | Does not own | Index |
| --- | --- | --- | --- |
| Agent runtime | Agent actions/tools, Operation runtime surface, model-safe Agent results | General workflow execution and provider transport | [agent-runtime](agent-runtime/README.md) |
| Workflow runtime | Durable events/commands/activities, recovery, scheduling, progress causality | Product intent and frontend interpretation | [workflow-runtime](workflow-runtime/README.md) |
| Planning and acquisition | Effective request, planning/review, acquisition, company/public evidence | Public projection serving and CRM engagement | [planning-acquisition](planning-acquisition/README.md) |
| Serving and product | Canonical public projection/API/frontend and product surface | Upstream acquisition or CRM writes | [serving-product](serving-product/README.md) |
| CRM and person assets | Person identity, CRM state/actions, evidence/assertions, outreach | Company acquisition strategy and provider queue policy | [crm-person-assets](crm-person-assets/README.md) |
| Provider runtime | Provider/model execution, mode, queue, cost and rate boundary | Business population selection and public rendering | [provider-runtime](provider-runtime/README.md) |
| Platform operations | PG/storage, test/runtime environments, deployment, review infrastructure | Domain contract semantics owned above | [platform-operations](platform-operations/README.md) |

## Cross-Module Rule

One artifact has one canonical path. Multiple module indexes may link to it,
but no index restates its field semantics. When a change affects more than one
module, the task brief and review packet must name each producer/consumer route
and the project-level owner matrix that binds them.

If no module owns a problem, add a routing-gap row to the
[documentation migration and retirement registry](../governance/DOCUMENTATION_MIGRATION_AND_RETIREMENT.md)
before creating a new cross-module document.
