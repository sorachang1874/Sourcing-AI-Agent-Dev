# Track D D1d — Projection action owner-binder decision oracle

> Status: Decision/oracle complete; production implementation blocked by an unowned projection-scope contract
> (2026-07-15). `search_projection` and `filter_projection` deliberately remain on the R-029 schema-less bridge, and
> the served Agent tool population remains zero. This document and its executable oracle authorize no schema, binder,
> model/provider call, live path, manual/product signoff, or milestone closure.
>
> D1f follow-up: the generic submit route now derives authenticated workspace/user only for the exact three activated
> CRM existing-record actions. That conditional path does not supply a projection owner or change this decision.

## 1. Outcome and boundary

D1d attempted the first production `ActionRequestSpec` population for the two read-only projection actions. The
required owner proof does not exist in the current physical or request-boundary contracts, so this batch stops before
adding `ActionRequestSchemaBuilder`, `ActionTargetBinderRegistry`, an `ActionBindContext`, or either production schema.

This is the required fail-closed outcome, not an implementation deferral hidden behind a permissive binder. D1's
contract requires `target_ref` to contain only owner-minted resource identity. A binder cannot truthfully mint a
projection identity, membership revision, or foreign/missing decision from caller-controlled workspace and projection
fields when the serving projection aggregate has no workspace/access-scope owner.

The batch owns only:

- this decision record;
- `tests/test_d1d_projection_action_binder_decision.py`, which mechanically freezes the missing physical owner,
  writer, API bind-context, and schema-population facts.

It changes no product code, API behavior, storage schema, migration, operation state, projection reader/writer,
provider/model path, shared tracker, or residual status.

## 2. Verified blocking facts

### 2.1 No physical projection-scope owner

The canonical runtime descriptors for `serving_projections`, `run_projection_links`, and
`collection_authoritative_pointers` contain no `workspace_id`, `tenant_id`, owner identity, or explicit access-scope
column. `ServingProjectionWriter.publish_run_scope_projection(...)` and
`publish_collection_authoritative_projection(...)` likewise accept no such field.

`run_scope_projection.source_run_id` is not a substitute owner:

- the projection row does not physically exact-copy the source job's requester/tenant;
- there is no projection-to-job ownership FK or scope invariant;
- a collection-authoritative projection has no required source-run chain at all;
- the public projection GET/search/candidate readers currently treat canonical projection rows as shared reads.

Joining an optional mutable identifier to `jobs` inside a binder would therefore invent a partial owner rule and make
collection and run projections disagree.

### 2.2 No server-owned generic action bind context

For projection actions, the authenticated `POST /api/operations/actions` handler still server-overrides only `actor`.
D1f conditionally derives `workspace_id` and user owner only when `action_type` is one of the exact three CRM
existing-record actions; it does not derive operation workspace/tenant or pass an `OwnerBoundTargetRef` for either
projection action. `SourcingOrchestrator.submit_operation_action(...)` therefore still uses the projection payload
workspace (defaulting to `default`) on this branch.

There is also no active `AgentConversation.scope_ref`/session owner at this boundary from which the projection owner
could select a target. Treating `payload.target_ref.projection_id`, an input alias, or caller-supplied membership
revision as server context would violate the existing D1 owner boundary.

### 2.3 Current D1 status

Both actions still have the reviewed D1b `projection_read` adapter, but their request schema/version/digest remain
empty. This keeps them API-submittable only through the explicit R-029 compatibility path and ineligible for any
future full served predicate. The D1 OB-ID set remains empty; Plan section 6 item 3 and R-029 continue to own the loose
schema bridge. R-019 and its 26-call ratchet are untouched.

## 3. Required owner decision before implementation

The projection contract owner must choose and implement one of these explicit shapes before D1 may populate either
action:

1. **Workspace-scoped projection.** Add a physical, immutable workspace/tenant or equivalent scope owner to the
   projection aggregate and every routing row that can select it; define key/index, publication-lock, CAS, reader,
   backfill, collision, and deletion semantics; exact-copy the scope at publication; and prove same-owner,
   foreign/missing, and collection/run parity in real PG. The binder may then exact-match authenticated scope and mint
   canonical `projection_id` plus the repository-read membership revision.
2. **Intentionally shared canonical read.** Add an explicit physical access-scope classification and document that
   shared projection identity is not tenant-owned. Separately define the authenticated operation workspace and the
   server-owned conversation/session scope that selects the projection. Missing, non-shared, and unprovable rows must
   share one fail-closed result; the API must not pretend a shared projection has a foreign tenant owner.

Neither choice may be inferred from current metadata/provenance JSON. The owner decision must cover both run-scope and
collection-authoritative projections, or explicitly exclude one with a reviewed product contract and migration plan.

## 4. Activation gate for the later implementation

After the owner choice is physical and reviewed, the bounded implementation must:

1. add the centralized closed request-schema builder and target-binder registry used by later action batches;
2. define closed, strict-unknown `search_projection` and `filter_projection` input option schemas;
3. mint target identity and current membership revision only from the selected server context plus canonical
   repository reads;
4. reject raw/forged targets, workspace/revision aliases, missing/foreign/unprovable targets before the first
   action/run/event write;
5. preserve exact schema/target pins through replay, approve, retry, and dispatch revalidation;
6. cover same-owner/shared-positive (as selected), missing/foreign, forged target, collision/replay, open-mode default
   operator workspace, and zero-write matrices;
7. keep both actions non-live and `served=0` until revisioned model-safe result schemas and simulate serializer
   preflight complete the full served predicate.

Only then may these two actions leave the R-029 numerator. D1f reduced that numerator to 12 by activating three CRM
actions; D1h then reduced it to 11 and D1i to the current 10 through their own reviewed owner/schema contracts.
Projection activation would reduce it further but would not close R-029 for the remaining API-submittable actions or
authorize removal of the compatibility epoch/evidence.

## 5. Executable oracle and validation

Run from `sourcing-ai-agent/`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1d_projection_action_binder_decision.py
.venv/bin/ruff check tests/test_d1d_projection_action_binder_decision.py
.venv/bin/ruff format --check tests/test_d1d_projection_action_binder_decision.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mypy \
  --follow-imports=skip --ignore-missing-imports \
  tests/test_d1d_projection_action_binder_decision.py
git diff --check -- \
  tests/test_d1d_projection_action_binder_decision.py \
  docs/TRACK_D_D1D_PROJECTION_ACTION_BINDER_DECISION.md
```

The oracle is intentionally a tripwire. Adding a physical projection scope, extending either projection publisher,
wiring authenticated action workspace/target binding, or populating either action schema must fail this test until the
owner decision, binder behavior, migration/backfill, zero-write matrix, and this document are updated together.

Author validation on the decision-only worktree passed the D1d oracle (**8 passed**) and the combined D1d + D1a + D1b
+ D1c request-contract/characterization/adapter set (**82 passed**). Ruff check/format, focused mypy, and the exact
two-file whitespace check passed. No PG row, operation/action/event, provider/model call, full pipeline test, or live
path was exercised because this batch changes no runtime or storage behavior.
