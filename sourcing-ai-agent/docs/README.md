# Documentation Router

> Status: Current problem-to-module router (three-hop chain: root AGENTS/README → this file → module index → canonical doc). Existing canonical documents keep their current paths; use [INDEX.md](INDEX.md) for status/tier authority until each migration row is closed.

```
owner: operator   last-route-audit: 2026-07-22   next-route-audit: next per-module migration pass
gates (CI, backend-ci lane): tests/test_markdown_status.py (banners) +
  tests/test_docs_routing.py (link graph via scripts/check_markdown_links.py --all,
  snapshot budgets, gap-row targets)
reorg design: HARNESS_REORG_DESIGN.md   migration registry: governance/DOCUMENTATION_MIGRATION_AND_RETIREMENT.md
```

这份入口按“问题属于哪个模块”来路由，而不是按文件名或最近修改时间来猜。
Phase 1 只增加索引，不批量移动现有文档，因此已有链接、评审 scope 和活跃分支不会被打断。

## Read Order

1. Read the applicable [`AGENTS.md`](../AGENTS.md).
2. Classify the problem in the table below.
3. Open the owning module index under [`docs/modules/`](modules/README.md).
4. Read the linked contract/product/testing/operations artifact required for the task.
5. Search all code producers and consumers of any shared symbol before editing.
6. Use [INDEX.md](INDEX.md) to check whether an existing flat document is active,
   reference-only, or archived.

The router narrows context; it does not replace code-reference searches or the
owner matrix in a canonical contract.

## Problem Routing

| Problem or change | Owning module | Read first | Then read |
| --- | --- | --- | --- |
| Agent action, tool schema, OperationRun, approval, model-safe result, served/live activation | [Agent runtime](modules/agent-runtime/README.md) | Agent module contract routes | Current Track D implementation/review artifact named by the module index |
| Workflow event/command/activity, causality, retry, recovery, scheduler, progress lifecycle | [Workflow runtime](modules/workflow-runtime/README.md) | Durable runtime contract | Recovery, progress, and operations routes |
| User intent, plan/review, acquisition strategy, company asset, Public Web, coverage/promotion | [Planning and acquisition](modules/planning-acquisition/README.md) | Intent/coverage contract | Acquisition operations and asset governance |
| Public API, projection, board/candidate state, frontend copy or product prototype | [Serving and product](modules/serving-product/README.md) | Frontend/projection contract | Product/reference artifact and frontend validation route |
| CRM record/action, person identity, evidence/assertion, outreach state | [CRM and person assets](modules/crm-person-assets/README.md) | CRM/person contract | Action activation and writer-owner routes |
| Provider queue, Harvest/DataForSEO/Apify, model transport, cost/rate policy | [Provider runtime](modules/provider-runtime/README.md) | Provider contract and mode isolation | Provider-specific playbook |
| PG/storage migration, test environment, local runtime, deploy, independent review | [Platform operations](modules/platform-operations/README.md) | Environment or storage contract | Mode-specific runbook and validation |
| Cross-module contract or unclear owner | [Module registry](modules/README.md) | Every named producer/consumer module | [Documentation status index](INDEX.md) and owner matrix |
| Current/blocked/next work | Workspace snapshot | [../../NEXT_TODO.md](../../NEXT_TODO.md) | Canonical module artifact linked from the row |
| Latest handoff/resume state | Workspace snapshot | [../../PROGRESS.md](../../PROGRESS.md) | Live lane state: gitignored `.coord/BOARD.md` (git wins) |
| X-First collection, judge contract, CSV export | X-First package | [../../x-first-researcher-sourcing/AGENTS.md](../../x-first-researcher-sourcing/AGENTS.md) | Export/live scripts: [../scripts/README.md](../scripts/README.md) (R2) |
| Live ops, paid dispatch, provider quota walls | [Provider runtime](modules/provider-runtime/README.md) | [../../CLAUDE.md](../../CLAUDE.md) critical rules | [HARVESTAPI_PLAYBOOK.md](HARVESTAPI_PLAYBOOK.md) |
| Directory organization / doc routing itself | Documentation governance | [HARNESS_REORG_DESIGN.md](HARNESS_REORG_DESIGN.md) | This file's routing-gaps table |
| Stale, duplicate, or misplaced document | Documentation governance | [Migration and retirement registry](governance/DOCUMENTATION_MIGRATION_AND_RETIREMENT.md) | Owning module index |

## Module Routes

| Module | Product responsibility | Code boundary examples | Status |
| --- | --- | --- | --- |
| [Agent runtime](modules/agent-runtime/README.md) | Safe Agent operations, tool contracts, results, approvals | `action_*`, `agent_*`, `operation_runtime.py`, `model_tool_runtime.py` | active route |
| [Workflow runtime](modules/workflow-runtime/README.md) | Durable workflow execution, causality, recovery, progress | `durable_runtime.py`, `recovery_*`, `worker_*`, `workflow_*` | active route |
| [Planning and acquisition](modules/planning-acquisition/README.md) | Intent-to-plan, acquisition, company/public evidence and coverage | `planning.py`, `acquisition*`, `company_*`, `public_web_*` | active route |
| [Serving and product](modules/serving-product/README.md) | Projection/public reads, frontend API and user experience | `serving_projection_*`, `api.py`, `frontend-demo/`, `contracts/` | active route |
| [CRM and person assets](modules/crm-person-assets/README.md) | Person-first CRM and evidence-backed engagement state | `crm_*`, `person_*`, `manual_review*`, `outreach_layering.py` | active route |
| [Provider runtime](modules/provider-runtime/README.md) | External provider/model execution policy and queues | `provider_*`, `harvest_connectors.py`, `dataforseo_client.py`, `model_*` | active route |
| [Platform operations](modules/platform-operations/README.md) | PG/storage, environments, testing, deployment, review machinery | `storage.py`, `migration_runner.py`, runtime/deploy scripts | active route |

These are documentation ownership boundaries, not a claim that each code file
already has a single runtime owner. Cross-module files must name all affected
module contracts in the task and review packet.

## Artifact Placement for New Documents

New durable documents should go directly under the owning module:

```text
docs/modules/<module>/
  contracts/
  product/
  architecture/
  testing/
  operations/
  migrations/
  decisions/
  reviews/
  archive/
```

Create a category directory when its first artifact exists. During Phase 1,
existing flat documents remain canonical and module indexes link to them. Do not
copy their normative content into a second file.

Use `docs/project/` only for a future artifact that truly has no single module
owner. Its owner matrix must still name each producer and consumer module.

## Snapshot Boundary

- Workspace [../../NEXT_TODO.md](../../NEXT_TODO.md) (≤120 lines) is the
  current/blocked/immediately-next routing snapshot; workspace
  [../../PROGRESS.md](../../PROGRESS.md) (≤200 lines) is the resume/handoff
  snapshot. Both replace-not-append. The old package-level
  [PROGRESS.md](../PROGRESS.md) / [NEXT_TODO.md](NEXT_TODO.md) are superseded
  redirect stubs (full content archived).
- Contracts, decisions, product intent, test rules, runbooks, and review
  evidence remain in their module or current canonical flat path.
- Snapshot cleanup is tracked separately so active Track D work is not mixed
  with a documentation reorganization.

See the [migration and retirement registry](governance/DOCUMENTATION_MIGRATION_AND_RETIREMENT.md)
for the cadence, deletion conditions, and current Phase-1 debt.

## Routing Gaps

| Gap | Impact | Temporary route | Resolution target |
| --- | --- | --- | --- |
| ~60 Track C/D increment docs flat at docs/ top level | active contracts drowned in increments | [INDEX.md](INDEX.md) Tier 3 (all bannered 2026-07-22) | per-module migration passes (registry-tracked) |
| module indexes still Phase-1 (link to flat paths) | third hop lands on flat files | each module README | per-module passes, one module at a time |

Closed 2026-07-22: live-script registry → [../scripts/README.md](../scripts/README.md) (R2);
deliverables manifest → [../deliverables/MANIFEST.md](../deliverables/MANIFEST.md) (R4).

## Phase-1 Validation

For changes limited to these routers:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_markdown_status.py
```

The repository-wide status test currently exposes pre-existing flat-document
debt recorded in the migration registry; every new router must be absent from
that failure list. Also check local Markdown targets and run `git diff --check`.
Behavior-level validation continues to come from the module-specific contract
and test route.
