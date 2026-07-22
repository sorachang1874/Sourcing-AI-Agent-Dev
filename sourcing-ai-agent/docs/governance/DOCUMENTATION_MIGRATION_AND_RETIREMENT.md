# Documentation Migration and Retirement Registry

> Status: Current Phase-1 documentation-governance registry. It records routing and cleanup debt; it does not change product/runtime contract status.

## Purpose

This registry lets the repository adopt module-first documentation without a
mass rename. Existing flat documents remain at their current paths until a
bounded move has checked status, backlinks, open review scopes, and unique
content.

The [problem router](../README.md), [module registry](../modules/README.md), and
module indexes are navigation artifacts. [INDEX.md](../INDEX.md) remains the
status/tier authority during Phase 1.

## Lifecycle States

| State | Meaning |
| --- | --- |
| `inventory_only` | Current path remains canonical; module index links to it |
| `move_planned` | Destination and owner are decided; backlink/open-branch audit pending |
| `migrating` | New canonical path exists; old path is a temporary forward-link stub |
| `superseded` | Old content is non-canonical and links to its replacement |
| `archived` | Retained only for audit/history and excluded from normal context |
| `delete_ready` | Retention and backlink conditions passed; deletion can be reviewed |

No document is moved merely because a destination directory now exists.

## Phase-1 Registry

> Update (2026-07-22, harness reorg R1-R3): the status-banner gate
> (`tests/test_markdown_status.py`) is now fully green — 81 files bannered by family
> (.coord ephemeral / pro-consults reference / Track C+D increment records).
> `PROGRESS.md` and `docs/NEXT_TODO.md` are superseded redirect stubs; workspace-root
> `PROGRESS.md`/`NEXT_TODO.md` are the bounded snapshots. `ARCHITECTURE.md`/`MODULES.md`
> re-bannered PENDING REFRESH. Bulk relocation of the ~60 flat Track C/D increment docs
> is deliberately DEFERRED to per-module passes (moving them wholesale would break
> pinned-review scopes and dozens of relative links in one shot — against the
> "one module at a time" adoption rule); each pass updates its row here.

| Current path/class | Owning route | Phase-1 state | Canonical replacement | Required check before move/retirement | Owner/target |
| --- | --- | --- | --- | --- | --- |
| `docs/INDEX.md` | Project documentation governance | `inventory_only` | none; remains status/tier authority | New router adoption is proven and every active doc has a module/status route | Repository maintainers; later phase |
| `docs/MODULES.md` | [Module registry](../modules/README.md) | `inventory_only` | module indexes are routing successor, not yet full content replacement | Refresh stale code boundaries, preserve unique architecture detail, update backlinks | Repository maintainers; separate bounded batch |
| Active flat contract docs in `docs/*.md` | Owning module index | `inventory_only` | future `docs/modules/<module>/contracts/` path, decided per document | Exact owner, callers/consumers, backlinks, open branches/reviews, status banner | Contract owner; opportunistic |
| Active flat product/design docs in `docs/*.md` | [Serving/product](../modules/serving-product/README.md) or owning module | `inventory_only` | future module `product/`, `architecture/`, or `decisions/` path | Decide active vs reference status and preserve unique acceptance criteria | Product/contract owner; opportunistic |
| Active Track B/C/D implementation and review docs | Owning module `reviews/` or `migrations/` | `inventory_only` | none selected in Phase 1 | Do not break pinned-review scope, standing links, or active branch handoffs | Track owner; after scope closes |
| `PROGRESS.md` | Project snapshot | `inventory_only` with cleanup debt | stays at root; durable detail routes to modules | Preserve current resume state, move unique evidence first, then rotate to declared active window | Active-work owner; after current batch handoff |
| `docs/NEXT_TODO.md` | Project snapshot | `inventory_only` with cleanup debt | stays in current location; module detail routes outward | Preserve active/blocked/next work and deletion gates; move completed narrative only after canonical links exist | Active-work owner; separate cleanup batch |
| Completed handoff/session/incident docs | Owning module archive or `docs/archive/` | `inventory_only` | chosen per artifact | No active snapshot/index/review depends on it as current truth | Artifact owner; milestone cleanup |

## Snapshot Policy

Root/project-level trackers are routing snapshots, not append-only stores:

- `PROGRESS.md` retains only current verified state and the latest handoff
  window; older windows rotate to `docs/archive/progress/` after durable module
  evidence is linked.
- `docs/NEXT_TODO.md` retains current, blocked, and immediately next items;
  completed history moves only when its unique contract, decision, evidence, or
  deletion condition has a canonical home.
- The existing `docs/INDEX.md` target of at most 300 lines for `PROGRESS.md`
  remains the current declared budget. A separate owner-led cleanup should set
  and enforce a bounded budget for `NEXT_TODO.md` rather than compressing it
  during active Track D implementation.
- Refresh at every milestone boundary and at least weekly while active work
  continues. Record the next cleanup date when each snapshot is next edited.

Do not delete detail from a snapshot until its durable destination is linked and
reviewable. Do not leave the same normative detail in both places afterward.

## Move and Retirement Procedure

For one document or one owner-bounded module at a time:

1. Confirm the document's current status in [INDEX.md](../INDEX.md).
2. Choose one module, artifact class, owner, and canonical destination.
3. Search all Markdown, scripts, tests, review prompts, and operator commands for
   the old path.
4. Check open branches and pinned-review artifacts whose scope names the path.
5. Move the file without mixing semantic rewrites into the rename when possible.
6. Update all in-repository links and the module/project routers in the same
   change.
7. Keep a forward-link stub only for a named external/open-branch dependency;
   give the stub an owner and deletion condition.
8. Run status-banner, local-link, and affected contract validation.
9. Change the registry row to `superseded`, `archived`, or `delete_ready` only
   when the evidence supports it.

## Cleanup Cadence

At each milestone and weekly during active multi-agent work:

1. Check all project/module router links.
2. Review routing gaps and documents past their verification date.
3. Replace stale snapshot narrative with links to durable module artifacts.
4. Resolve duplicate active/canonical claims.
5. Advance completed migration rows and remove expired stubs.
6. Confirm archived docs are not the only source of current behavior.

## Routing Gaps

| Gap | Impact | Temporary route | Owner | Resolution condition | Status |
| --- | --- | --- | --- | --- | --- |
| Existing flat documents do not yet carry module/artifact metadata | Agents still rely on filename/tier inference after reaching a module | Module indexes plus [INDEX.md](../INDEX.md) | Repository maintainers | Per-module inventory completed during bounded moves | open |
| `PROGRESS.md` and `NEXT_TODO.md` contain more than a minimal snapshot window | Resume files are expensive to read and easy to let grow further | Read current sections and follow module links; do not append unrelated history | Active-work owner | Owner-led rotation after current implementation handoff | open |
| Product/reference documents have mixed freshness | Prototype intent may conflict with active API/contract semantics | [Serving/product route](../modules/serving-product/README.md); active contracts win | Product/contract owners | Each document marked active/reference/superseded in its module inventory | open |
| Track documents are flat and numerous | Finding the exact current batch can require broad search | [INDEX.md Tier 3](../INDEX.md) plus owning module route | Track owners | Track-specific review/migration index adopted without breaking pinned scopes | open |
| No checked-in local Markdown link validator covers the routing graph | Broken routes are not yet CI-blocking | Run the targeted local-link check when router files change | Platform operations owner | Add a deterministic repository link check in a separate tooling batch | open |
| Repository-wide status-banner test has pre-existing violations outside this Phase-1 scope | The full Markdown governance lane is not currently green | Require every new router to pass a targeted first-eight-lines status check | Documentation owners | Existing violations are classified and repaired or explicitly excluded by policy | open |

## Phase-1 Exit Criteria

Phase 1 is complete when:

- representative Agent, workflow, acquisition, product, CRM, provider, and
  platform questions route from `docs/README.md` to a canonical artifact;
- all new router documents have status banners and valid local links;
- no existing document was moved or reclassified implicitly;
- snapshot cleanup debt and migration deletion conditions are visible; and
- future documents have a declared module-first destination.
