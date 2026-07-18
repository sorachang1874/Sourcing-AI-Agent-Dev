# Track D D1n S1f0a — filter-projection publication foundation boundary

> Status: fixed-forward candidate after the pinned Ultra review of `64a7dfc8f84f416a166a6ded69c666c7e50e523e`
> returned `NO-GO 0/10/2/2`. The rejected proposal is not implementation authority. This document locks only the
> foundation boundary and the dependency graph needed to ratify a product owner. It changes no product writer,
> reader, migration, Agent registry population, provider/model route, or serving state. This is author evidence,
> not an independent-review verdict.

## 1. Fixed-forward outcome

S1f0a does **not** ratify an exact-start-v2 product publication owner. Current code has no durable join from the
approved start occurrence to the committed Cohort result and run projection, so a receipt, run, snapshot, result
view, execution result, and member list supplied as independent arguments cannot become owner evidence merely by
passing local shape checks.

The only ratified ownership boundary is:

```text
physical_projection_writer=serving_projection_owner
candidate_field_validator=projection_search_service.filter_projection_publication_candidate
candidate_field_validator_revision=filter_projection_publication_candidate_v1
candidate_state=foundation_only_unbound
product_owner_state=unratified
agent_reader_state=blocked
result_slot_state=blocked
served_population=0
provider_model_live_invocations=0
migration_delta=0
backfill=forbidden
```

`serving_projection_owner` remains the sole physical writer for a run-scope projection, its members, route,
Activity/Attempt/EntityDelta evidence, and recovery. A filter component may validate a future field bundle inside
that writer's existing publication UoW; it is not a second physical owner and may not open its own connection or lock
order.

The current `projection_filter_request_v2` target remains unchanged:

```text
projection_id + membership_revision + cohort_selection_registry_version +
cohort_selection_registry_digest + cohort_selection_digest
```

No caller-visible planning, execution, receipt, path, runtime, lane, or result fields are added.

## 2. Current lineage characterization

The repository currently contains three incomplete segments:

1. exact start-v2 authority: `operation_runs.action_id` joins the Action stream's exact `ActionApproved` sequence-2
   receipt; the command-acceptance owner binds Action, OperationRun, WorkflowRun, root WorkflowCommand, receipt,
   start snapshot, and result occurrence;
2. legacy Cohort publication: `jobs.job_id -> job_result_views.job_id/snapshot_id -> SearchSeedSnapshot files ->
   cohort_execution_result.v1 -> run_scope_projection(source_run_id=job_id)`;
3. operation-native acquisition: `acquisition_runs.operation_run_id/workflow_run_id` retains typed runtime lineage
   and publishes incremental projection admission with `source_run_id=workflow_run_id`, but creates no legacy Job,
   SearchSeedSnapshot, result view, or committed Cohort result.

There is no join between segments 1/3 and segment 2. In addition, the start-v2 root payload's receipt/Cohort/planning
identity is copied into `resolved_intent.source_workflow_payload` and then dropped by the current plan builder before
plan review and acquisition-run persistence. Consequently:

- a valid receipt can be paired with an unrelated run;
- a valid execution result can be paired with an unrelated snapshot/result view;
- a caller-provided publication digest does not prove the candidate-document commit marker;
- an internally consistent member subset does not prove equality with the committed candidate population.

All such inputs remain characterization or fixture values. They are not adoptable product authority.

## 3. Required dependency batches

The exact product path is operation-native. It must not create a legacy `job_id` shell bridge.

| Batch | Owner and required outcome | Promotion boundary |
| --- | --- | --- |
| `S1f0b-foundation` | `serving_projection_owner`: dedicated carrier-key reservation registry, generic membership-change atomic invalidation, collection strip, and existing parent/member/route UoW tests | foundation only; no Agent reader or product-eligible carrier |
| `S1f0c0-lineage-decision` | `acquisition_planner` + `cohort_provider_runtime`: ratify the exact operation-native start-lineage carrier and the commit-once Cohort terminal lineage owner, fields, CAS, and migration status | decision only; no inferred aliases |
| `S1f0c1-start-propagation` | preserve the exact start authority through intent, plan, review session, execution bundle, and `acquisition_runs` without a second writable source | must have scope-matched review before product adoption |
| `S1f0c2-cohort-terminal` | persist one immutable terminal lineage record binding acquisition/workflow source run, receipt/root authority, SearchSeedSnapshot/result view, exact execution result, candidate-document marker, and full candidate-set commitment | no partial/mutable/latest-file authority |
| `S1f0d-owner-v2` | `serving_projection_owner` loads the typed terminal owner and publishes the final closed parent/member set inside the existing UoW | first product-eligible physical carrier candidate |
| `S1f1-reader-result` | owner-only read snapshot plus commandless shared result-slot acceptance | depends on reviewed `S1f0d`; remains unserved until release gates |

Implementation and review may run asynchronously where exact write sets are disjoint. The only promotion edges are
shown in the table; a review request does not freeze unrelated Track D work.

## 4. S1f0c0 decisions that must be frozen before product code

The lineage decision must structurally define, with unknown-key rejection:

1. the operation-native source-run identity and its exact join through acquisition run, workflow root command,
   OperationRun, Action, and the unique confirmation receipt;
2. the immutable start-authority ref, including receipt id/digest, Action/Operation/Workflow/root-command identities,
   command-acceptance digest, start-snapshot digest, workspace/requester binding, and result occurrence;
3. exact receipt -> retained capability-free planning manifest -> exact recompile -> capability-bearing execution
   manifest -> committed result validation, including the only permitted capability difference;
4. a commit-once terminal lineage record and CAS that binds snapshot id, result-view id, execution/result/publication
   digests, candidate-document commit marker, and the exact start authority;
5. complete source-to-projection population equality: nonzero source count, `<=1000`, visible count, excluded count,
   exclusion rule, canonical member-set digest, and duplicate/extra/omitted/cross-run rejection;
6. a versioned opaque runtime namespace ref. The canonical filesystem namespace remains private; the Agent-visible
   identifier must be server-minted and cryptographically bound without exposing a path;
7. exact freshness/readiness owners and the complete private-to-model-safe lane transformation;
8. the final projection-publication terminal owner and every shared result-slot owner target/ref/digest/token field.

S1f0c0 must publish a machine-readable closed decision manifest. Prose or local tuples are not a substitute.

## 5. Foundation-only writer behavior

S1f0b may improve the existing generic writer safely before S1f0c0 completes:

- parent and member carrier keys use dedicated reservation registries; they are not search-index binding keys;
- direct generic creation or overwrite of a reserved key fails before writes;
- if an owner-unaware writer changes membership, the same existing UoW removes the parent carrier and every member
  carrier before committing the changed membership;
- a semantic no-op preserves a valid carrier and the existing opaque membership revision;
- collection-authoritative publication always strips both carriers;
- full run publication, incremental board/facet paths, repair/migration paths, direct member upsert/replace, combined
  parent/member writes, facade methods, repository delegates, and native PG adapters are all in the enumerated writer
  inventory;
- no new `_connect_with_transaction_lock` caller or lock order is introduced.

Until S1f0d, any experimental carrier must be marked `foundation_only_unbound`, must use an identity/version distinct
from the eventual product owner, and must be rejected by every Agent/product reader. It is disposable shadow evidence,
not backfill material.

## 6. Terminal/result-slot boundary

`filter_projection` remains `result_link_policy=no_command_v1`. The shared result-slot transport needs no new DDL,
but no success/deferred terminal owner is ratified in this batch.

The current `cohort_execution_result.v1` is audit provenance, not the projection result authority. The eventual
terminal authority is the reviewed exact projection publication from S1f0d. Missing/foreign may reuse the existing
slot-generation masked-absence family. Malformed owner state, lineage drift, prepare/accept races, or membership-token
drift keep the slot pending and write zero result attempt/slot/journal rows. S1f0c0/S1f1 must separately define the
permitted shared result-slot writes after successful revalidation; domain, projection, repair, provider, and model
writes remain zero on the read path.

No `workflow_command_id`, ActivityAttempt, command fence, or mutable succeeded-command projection is invented for
this commandless action.

## 7. Tests and evidence boundary

S1f0a fixed-forward tests must parse the closed machine manifest, reject unknown keys, and assert exact Markdown
routes/status digests. Lexical substring counts are characterization only.

S1f0b foundation tests may prove:

- dedicated reservation registries cover every enumerated mutation entrypoint;
- owner-unaware semantic membership changes atomically invalidate every carrier;
- semantic no-op preserves the carrier and membership revision;
- collection/legacy/repair paths never adopt a carrier;
- fault injection rolls back parent/member/route writes;
- identical and divergent concurrent publications serialize under the existing lock.

They may not prove exact-start lineage, product eligibility, Agent result acceptance, E2E, served readiness, or live
readiness. Those claims require S1f0c0-S1f1 in dependency order.

## 8. Release and residual boundary

`served=0`; provider/model/live invocation count remains zero. R-019 and R-029 remain open. The S1f0a Ultra
`NO-GO 0/10/2/2` remains the controlling verdict until a fresh pinned non-author review accepts this fixed-forward
scope. No W6/nightly, paid provider, founder/manual signoff, or milestone claim may use the rejected `64a7dfc`
decision.
