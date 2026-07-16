# Track D D1l Projection Read Action Schema Activation

Date: 2026-07-16

Status: implementation candidate; fresh pinned non-author review required before live/W6/manual/product signoff.

## Scope

D1l activates `search_projection` and `filter_projection` as schema-defined Operation actions.

It does not:

- serve any Agent tool to a model;
- call any provider/model/live path;
- change projection facet ownership or search-index ownership;
- close R-019, R-028, or R-029.

## Contract

- `ActionRequestSpec` owns closed request schemas:
  - `projection_search_request_v1`
  - `projection_filter_request_v1`
- The existing public projection reader is the access-scope owner. Its checked-in
  `SHARED_CANONICAL_PROJECTION_TYPES` closed set classifies both production projection types as
  `shared_canonical_read`; neither projection identity nor membership is tenant-owned. The persisted Operation
  workspace separately scopes CRM overlay reads and never changes projection membership.
- Submit accepts exactly one `projection_id` / `serving_projection_id` carrier and at most one membership-revision
  carrier, rejects duplicates even when equal, and resolves the selector through the canonical serving projection
  reader.
- The action owner mints `OwnerBoundTargetRef(owner_module="projection_search_service")` with:
  - `projection_id`
  - `membership_revision`
- `search_keyword` / `search` / `query` are exclusive compatibility carriers normalized to one stripped
  `search_keyword`; zero or multiple carriers fail before persistence.
- `filters` / `candidate_filter` are exclusive compatibility carriers normalized to one closed `filters` object.
  Employment status, location, function, layer, audit, recall, and search values use the canonical projection-reader
  normalizer; unknown keys, invalid enum values, ambiguous nested aliases, and lossy values fail instead of becoming an
  unfiltered read. Multi-select arrays are deduplicated and sorted before idempotency identity is computed.
- Persisted schema-defined action `input` and `target_ref` are revalidated as strict JSON before dispatch: only exact JSON
  containers/scalars are accepted, so tuples and other Python-only containers fail closed without transition writes.
- Dispatch acquires the Operation dispatch lock and projection publication lock against one shared monotonic **5s total
  acquisition deadline**, not 5s per lock, and holds both from pre-read revalidation through bounded-result persistence.
  A deadline already in the past fails before opening a connection. Exhausting the shared deadline
  returns `operation_dispatch_lock_busy` or `projection_publication_lock_busy`; both map to HTTP 409. Dispatch also
  compares the persisted membership revision before the read and the reader's post-read pinned revision; a
  reader-observed revision change produces reselection rather than an ordinary failure. Generic reader reasons
  `projection_membership_revision_changed_during_read`, `...changed_during_page_read`, and
  `...changed_during_search_read` normalize to `projection_membership_revision_stale` with
  `reselection_required=true`; exact replay preserves that flag and emits no duplicate event.
- Missing, non-shared, and otherwise unprovable projections share the masked `projection_not_found` / HTTP 404 result at
  submit without creating an action or operation. Other readiness failures retain an exact reason only after the row
  proves a supported shared-canonical projection type.
- A caller-supplied stale membership revision fails at submit before writer entry, with zero action/operation/evidence
  writes. Membership that becomes stale after a successful submit fails at dispatch before successful bounded
  read-result publication or any domain write; that dispatch-observed failure persists explicit Operation
  failure/reselection evidence.
- Projection read actions create no workflow command and write no projection/CRM/person/provider domain state. Their
  terminal action, Operation, and event transition commits in one dedicated PG UoW.
- Operation dispatch uses the exact persisted Operation workspace for CRM overlays. Authenticated direct projection
  reads plus job dashboard/candidate projection rows default to the server-derived workspace, while an explicit
  `default` remains the pre-auth legacy namespace selector. Open mode preserves its explicit workspace; workspace never
  changes shared projection membership. Direct `/api/projections/{projection_id}/candidates` and `/search` transport
  tests pin all three workspace modes: authenticated server-derived, authenticated explicit legacy `default`, and open
  mode explicit workspace.
- Four production generic projection-field patch callers—board-visible extension, Operation native projection admission,
  facet layering publication, and collection layering backfill—use
  `ServingProjectionRepository.patch_publication_fields_under_lock`; production raw projection `upsert` is statically
  rejected. The helper owns the projection publication session key, then the native writer locks the authoritative row
  with `SELECT ... FOR UPDATE` and merges current counts/readiness/metadata. It rejects these three search-index binding
  metadata keys: `projection_person_search_index_build_generation`,
  `projection_person_search_index_build_input_revision`, and `projection_person_search_index_input_revision`. A raw
  public counts/readiness patch therefore cannot bypass D1l read/result exclusion.

## Owner and preflight matrix

| Action | Caller selector | Owner-minted target | Canonical input | Submit preflight | Dispatch preflight |
|---|---|---|---|---|---|
| `search_projection` | exactly one projection alias; optional single revision alias | `projection_search_service`: `projection_id + membership_revision` | `search_keyword`, optional `offset/limit` | shared-canonical type, servable row, stable current revision, exact alias/type rules; persisted strict JSON | Operation + projection locks under one shared monotonic 5s deadline through result persistence; persisted target and reader-pinned revision checks; exact persisted Operation workspace for CRM overlay; terminal action+Operation+event one-PG-UoW |
| `filter_projection` | exactly one projection alias; optional single revision alias | `projection_search_service`: `projection_id + membership_revision` | closed normalized `filters`, optional `offset/limit` | same projection checks plus exclusive filter carrier, strict canonical filter validation, and persisted strict JSON | same shared monotonic deadline/revision fence/workspace scope/terminal UoW |

Missing, non-shared, and otherwise unprovable projections map to the same `projection_not_found`/HTTP 404 before writer
entry. Other readiness failures retain their exact `not_ready` reason only after supported shared-canonical type proof.
A completed projection-read dispatch maps to HTTP 200; a dispatch-time reader `not_ready` retains its exact reason and
maps to HTTP 409. Typed `operation_dispatch_lock_busy` and `projection_publication_lock_busy` also map to HTTP 409;
command-planning adapters retain HTTP 202.

## Residuals

- R-029 remains open at **6/15** schema-less production actions.
- R-019 remains open globally. D1l is a bounded commandless specialization: terminal action+Operation+event writes use
  one PG UoW and both session locks share one monotonic total deadline; there is no workflow command, so command
  generation/lease fencing is inapplicable. No direct state-sync caller is added and the ratchet remains **26**. Other
  Operation/command paths retain the global residual.
- R-028 remains open; this batch does not change CRM mutation or command terminal/effect atomicity.
- Served Agent tool population remains **0**.

## Current author evidence

Initial targeted validation before the fixed-forward audit:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_read_only_projection_action_creates_idempotent_operation_without_module_side_effects \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_projection_filter_operation_dispatch_completes_read_only_without_workflow_command

8 passed
```

The fixed-forward audit then added selector ambiguity, strict filter normalization/idempotency, persisted strict-JSON
revalidation, authenticated direct/job workspace scope including direct candidates/search three-mode coverage, masked
access-scope parity, submit-stale zero-write, one-deadline Operation/projection lock serialization through result
persistence, four-caller lock-owning publication patches, reader-change reselection/replay, exact post-type readiness
reason, commandless terminal-UoW rollback/replay, and HTTP 404/409/200 transport regressions.

Final stable-tree author evidence:

```text
adversarial projection-read nodes: 5 passed + 9 subtests
API + projection writer:           50 passed + 87 subtests
D1 contract suite:                 133 passed + 176 subtests
full Operation runtime:            139 passed
make lint:                         58 files already formatted; all checks passed
make typecheck:                    81 errors / 4 files (accepted ceiling unchanged)
changed Python py_compile:         clean
git diff --check:                  clean
```

A fresh dirty-tree non-author read-only re-audit returned P0/P1/P2/P3=`0/0/0/0`, scope-local advisory `GO`. It is not
a pinned or formal `GO`. The implementation commit and its fresh hash-bound independent-review artifact are recorded
only after those steps complete; live/W6/manual/product signoff remains fail-closed meanwhile.
