# Track D D1n S1f0b — filter-projection publication foundation

> Status: implementation author candidate. This slice exercises and protects the existing parent/member/run-route
> PostgreSQL unit of work with a non-product `foundation_only_unbound` carrier. It does not establish receipt,
> Cohort-result, snapshot, terminal, freshness, readiness, Agent-result, provider, model, live, or served authority.

## Impact

S1f0b reserves two distinct shadow-carrier keys:

- parent metadata: `filter_projection_publication_candidate_v1`;
- member provenance: `filter_projection_membership_candidate_v1`.

Only `ServingProjectionWriter.publish_filter_projection_foundation_run_scope_projection` can introduce those
carriers. The builder accepts a closed source-run identifier and a non-empty, duplicate-free candidate set, then
canonicalizes the foundation record and member wrappers. The record intentionally excludes receipt, result-view,
snapshot, terminal-owner, freshness, readiness, and member-set authority fields.

The typed value is rebuilt and exact-compared immediately before repository and native publication. Its projection
state is forced to `draft`; its route uses `filter_projection_foundation_candidate`, never the product `result` route;
its typed source run must match the native scope key, projection row, and routing row exactly. Product and candidate
publication share one run-scoped advisory lock and symmetrically reject the opposite route, so first publication
cannot race into two routes. An existing product result route or non-foundation projection collision fails closed.
`ServingProjectionReader` also rejects the carrier explicitly, independently of state and route isolation.

Every native publication payload is bound back to the scope whose advisory lock it holds: run projections require the
exact projection type, source run, routing run, and link type; collection projections require the exact projection
type, collection, source version, and pointer active version.
Every member row and replacement predicate is likewise bound to the parent projection id. Mixed combined-table pairs
are rejected, so a caller cannot lock parent A while writing members or a route for B.

The physical writer remains `serving_projection_owner`; no second projection table, relation, or writer is added.

## Generic-writer policy

Every current generic projection surface follows the same rule inside the existing PostgreSQL transaction:

1. A caller cannot forge either reserved key through repository or native adapter entrypoints.
2. A pure semantic member no-op preserves the exact existing parent/member carriers, opaque input revision, and
   derived search/facet/readiness products. A generic run publication is not a pure member no-op because it would
   install a product result route; it is rejected while the foundation exists.
3. Any real member addition, deletion, reorder, visibility/public-summary/provenance change, or authoritative
   collection publication removes the parent carrier, every member carrier, and the exact candidate route atomically;
   later foundation rebuild or product publication is therefore not blocked by an orphan route.
4. A missing parent carrier cannot coexist with surviving member carriers.
5. Repository and native standalone parent/member writes cannot silently remove or orphan a live foundation; the
   candidate-aware publication path or an atomic combined mutation is required.
6. Generic generated-id insert/upsert, update-returning, update, and delete entrypoints are prohibited for both serving
   projection tables. Exact-scope replacement, standalone upsert, and combined publication retain their dedicated
   carrier-aware lock and policy.
7. Combined parent/member methods always apply revision/product preservation or invalidation for the serving pair;
   callers cannot opt out with a boolean. Run-scope publication keeps parent, members, input revision, and route in
   the existing locked unit of work. A route failure rolls all of them back.

The machine inventory is checked in both directions: every declared surface must exist, and every generic native
mutator discovered from the adapter's dedicated-writer guard must be represented. This prevents a newly added generic
DML method from silently falling outside the carrier review boundary.

The semantic comparison ignores only the foundation member wrapper itself. All existing public member semantics
remain part of change detection.

## Files and owners

| Surface | Responsibility |
| --- | --- |
| `filter_projection_publication_owner.py` | closed foundation value, canonical digest, reserved-key helpers, carrier-free semantic comparison |
| `serving_projection_writer.py` | single typed public entrypoint for the foundation run-scope publication |
| `repositories/serving_projection.py` | repository-level rejection, preservation, invalidation, and collection stripping |
| `control_plane_live_postgres.py` | native entrypoint rejection and same-transaction carrier policy |
| `test_d1n_s1f0b_filter_projection_publication_owner.py` | pure contract, generic/native forge matrix, no-op/change matrix, rollback, and collection invalidation |

## Release boundary

This candidate is usable only as a non-adoptable foundation and test harness. S1f0c1/S1f0c2 must first implement the
reviewed operation-native start lineage, exact planning-to-execution recompile, commit-once Cohort terminal, and
complete candidate-set commitment. S1f0d then owns the first product-eligible terminal publication. S1f1 and Agent
tool population remain downstream.

`served=0`; R-019 and R-029 remain open. A pinned non-author review is required before this slice can be treated as
accepted implementation evidence.
