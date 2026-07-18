# Track D D1n S1f0c0 — operation-native lineage and projection-terminal owner decision

> Status: decision-locked author candidate. This batch defines the future S1f0c1/S1f0c2/S1f0d/S1f1 contracts but
> implements no runtime writer, relation, migration, reader, result acceptance, Agent population, provider/model call,
> or served state. It is author evidence until a fresh pinned non-author review accepts the scope.

## 1. Decision

The product path remains operation-native. Its canonical `source_run_id` is exactly
`acquisition_runs.workflow_run_id`; a legacy `job_id` shell, a `job_id` alias, and caller-authored source-run aliases
are forbidden.

S1f0c0 ratifies two future authorities without implementing them:

1. `cohort_provider_runtime` will own one immutable, commit-once
   `cohort_projection_terminal_lineage.v1` record that proves one exact start occurrence produced one complete Cohort
   candidate set; and
2. `serving_projection_owner` will remain the sole physical projection writer. In S1f0d it may invoke
   `projection_search_service.filter_projection_publication_owner_v2` only as an in-UoW field validator and publish
   one `filter_projection_publication_terminal.v1` record with the parent/member/route projection.

The final reviewed projection publication is the `filter_projection` result authority. The existing
`cohort_execution_result.v1` remains an audit predecessor and cannot win an Agent result slot directly.

The closed machine decision is
[filter_projection_lineage_terminal_owner_decision_v1.json](../contracts/filter_projection_lineage_terminal_owner_decision_v1.json).
Unknown keys, inferred aliases, path-shaped identifiers, partial lane output, and independent caller arguments fail
closed.

The obligation source is Track D Plan §6, not an invented OB mapping. The invariant sweep assigns D1 the empty
OB-ID set (`∅`). Plan §6 item 3 remains represented by R-029 and the Track D TODO bookkeeping; this decision neither
closes that residual nor borrows an OB-ID from D0/D2/D3. Plan §6 item 4, R-019, and R-029 remain open.

## 2. Current-state evidence and missing cut vertex

The decision is grounded in the current tree, not in the rejected S1f0a product-owner claim:

- `AcquisitionStartCommandAcceptanceOwnerRef` already closes the 18-field command-acceptance identity across
  workspace, Action, OperationRun, WorkflowRun, root WorkflowCommand, `ActionApproved` sequence 2 receipt,
  `OperationCommandPlanned` sequence 1 winner, start snapshot, parent budget, and result occurrence.
- `AcquisitionStartV2RootCommandPayload` already validates `acquisition_root_command_payload.v2` and binds its exact
  receipt and start-snapshot digests.
- `AcquisitionCommandOwner._start_v2_root_workflow_payload` copies receipt/Cohort/planning facts into the compatibility
  workflow payload. `_execute_acquisition_intent_resolve_command_payload` retains that payload at
  `resolved_intent.source_workflow_payload`.
- `AcquisitionCommandOwner._build_acquisition_plan_from_resolved_intent` then drops that exact source payload. The
  plan, review session, execution bundle, and `acquisition_runs` therefore cannot prove their origin from the exact
  approved start occurrence.
- `CohortProviderCompiler.compile_planning_manifest` and `validate_planning_manifest` already own the closed,
  capability-free `cohort_provider_manifest.v2` planning identity. The current runtime has no exact v2
  plan-to-capability-bearing execution recompile that preserves every non-capability field.
- `_build_cohort_execution_result` writes a useful Cohort audit envelope and a candidate-document publication marker,
  but it contains private artifact paths and does not commit the Action/Operation/Workflow/acquisition lineage or the
  complete canonical member set.
- `WorkflowRuntimeRepository.upsert_acquisition_run` makes `(acquisition_run_id, workspace_id, workflow_run_id,
  operation_run_id, idempotency_key)` immutable, while its request/plan/execution JSON is not presently an immutable
  terminal owner.
- the operation-native projection admission already calls
  `ServingProjectionWriter.publish_run_scope_projection(run_id=workflow_run_id, ...)`; it is incremental and is not
  the final complete Cohort publication.

Thus three locally valid objects remain unsafe when supplied independently: a start receipt, a Cohort result, and a
projection member list. Only a durable exact join may authorize S1f0d.

## 3. Exact start authority and propagation

The sole input is `acquisition_start_command_acceptance_owner_result_ref.v1`, reloaded from the committed start
aggregate. Its field set must equal `ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS`; a subset, superset, JSON
alias, or reconstructed value is invalid.

The required equalities are:

```text
receipt.action_id
  == action.action_id
  == operation.action_id
  == start_authority_ref.action_id

operation.operation_run_id
  == acquisition_run.operation_run_id
  == start_authority_ref.operation_run_id

root.workflow_run_id
  == acquisition_run.workflow_run_id
  == start_authority_ref.workflow_run_id
  == future source_run_id

root.command_id == start_authority_ref.workflow_command_id
root.payload_digest == start_authority_ref.root_command_payload_digest
root.start_snapshot_digest
  == receipt.start_snapshot_digest
  == start_authority_ref.start_snapshot_digest
root.confirmation_receipt_ref == start_authority_ref.confirmation_receipt_ref
action.metadata.result_occurrence_ref == start_authority_ref.result_occurrence_ref
```

S1f0c1 must add one closed `acquisition_start_lineage_ref.v1` carrier and exact-copy it through this ordered chain:

```text
root command
  -> resolved intent
  -> acquisition plan
  -> plan review execution bundle
  -> plan-commit execution bundle
  -> acquisition_runs.execution_bundle.start_lineage_ref
```

Every stage reloads its predecessor, constructs the typed value, and compares canonical bytes before its own write.
No stage may read an ambient latest receipt, preview, plan, result, or file to repair a missing carrier. The final
acquisition-run carrier is still non-terminal; it authorizes S1f0c2 to validate work, not S1f0d to publish.

## 4. Planning-to-execution exact recompile

The retained planning authority is `cohort_provider_manifest.v2`. The future execution identity is the distinct
`cohort_provider_execution_manifest.v2`, created only by the compiler-owned `recompile_execution_manifest` entrypoint.
That entrypoint does not exist yet and is an S1f0c1 implementation obligation, not evidence claimed by this decision.

The only permitted differences are:

```text
schema_version
execution_ready
execution_blocker
compiler_inputs.execution_capability
manifest_digest
```

All company, Cohort, filters, lanes and lane digests, aggregation, budgets, physical query, schema pins, and compiler
inputs other than the capability must be exactly equal. The execution capability is server-issued, binds the exact
provider mode and opaque runtime namespace reference, and may narrow but never expand the planning ceiling. A recompile
from v1, a caller capability, a changed provider payload, lane reorder, normalized alias, or silent budget rewrite is
invalid.

The committed `cohort_execution_result.v1` must exact-match the execution provider, Cohort/manifest identities, closed
lane summaries, capability, and result digest. `missing_required_lane_count` must be zero. Partial lane success,
unverified equality, or a marker without matching candidate-document bytes cannot terminalize.

## 5. Commit-once Cohort terminal record and CAS

S1f0c2 must introduce the PG-only `cohort_projection_terminal_lineage` relation through a separately reviewed
migration. S1f0c0 creates no DDL. One logical identity is:

```text
(runtime_namespace_ref_id, provider_mode, workspace_id, acquisition_run_id)
```

The writer is only `cohort_provider_runtime`. It locks the exact start aggregate, then `acquisition_runs`, then the
terminal identity; reloads every stored predicate; and performs `insert_once_or_exact_replay`. The only outcomes are
`inserted`, `exact_replay`, or `conflict_zero_write`. A committed row is immutable: retry cannot update it, a later
file cannot retarget it, and a different terminal requires a different acquisition run.

The closed record binds:

- opaque runtime namespace, provider mode, workspace/requester, Action, OperationRun, WorkflowRun, root command, and
  acquisition run;
- exact start-authority, planning-manifest, and execution-manifest references;
- path-free SearchSeedSnapshot and result-view references;
- the exact Cohort execution-result digest;
- candidate-document bytes, commit-marker digest, and Cohort publication digest; and
- the complete candidate-set commitment plus future freshness/readiness inputs.

`terminal_lineage_id` is the CAS winner and `terminal_digest` hashes the canonical record excluding that digest.
Artifact paths, raw runtime paths, `job_id`, latest-file lookup, legacy terminal adoption, and historical backfill are
forbidden.

## 6. Complete candidate-set commitment

The first product slice uses `cohort_candidate_set_commitment.v1` with `no_exclusions_v1`. Therefore:

```text
1 <= source_candidate_count <= 1000
source_candidate_count == visible_candidate_count + excluded_candidate_count
excluded_candidate_count == 0
```

The canonical set contains exactly one UTF-8-byte-sorted record per source candidate:

```text
candidate_identity_key
candidate_document_digest
cohort_lane_membership_digest
visibility_state=visible
```

The source commitment and final projection must have the same canonical member-set digest. The candidate-document
digest and lane-membership-set digest are separately committed. Duplicate, extra, omitted, hidden, path-derived, or
cross-run members fail before publication. A future nonzero exclusion policy requires a new reviewed schema revision;
it cannot be smuggled into `v1` as a reason string.

## 7. Runtime namespace, freshness, and readiness owners

Filesystem namespaces remain private. The Agent-visible value is the closed
`runtime_namespace_ref.v1 = {schema_version, runtime_namespace_ref_id, runtime_namespace_binding_digest}`. Only
`runtime_environment.runtime_namespace_ref_registry` may mint it. The private registry binding includes the namespace
digest, provider mode, workspace, issuer revision, and binding digest; it never retargets and cannot be reused across
workspace or mode. A path or URL in the result is invalid, even if it points inside the expected runtime.

The final publication has two field-owner pairs while retaining one physical writer:

| Meaning | Physical source of truth | Model-safe reader owner |
| --- | --- | --- |
| freshness | `serving_projection_owner.final_publication_identity` | `projection_search_service.publication_identity` |
| readiness | `serving_projection_owner.final_projection_readiness` | `projection_search_service.readiness` |

`fresh` means the current projection terminal exact-matches terminal lineage id/digest and membership revision.
`ready` additionally requires complete lane coverage, candidate-set equality, committed parent/member/route UoW, and
the reviewed product carrier. Readers may expose these values but cannot repair or recompute owner state by scanning
files.

## 8. Projection terminal and result-slot contract

S1f0d must load the typed Cohort terminal inside the existing serving-projection publication UoW. The final parent,
members, run route, Cohort identity, complete-set digest, opaque namespace ref, freshness/readiness, and
`filter_projection_publication_terminal.v1` either commit together or roll back together. No second connection, lock
order, projection writer, or Cohort-result terminal shortcut is permitted.

`filter_projection` remains `effect_class=read_only` and `result_link_policy=no_command_v1`. Every result has an exact
Action/Operation pair, empty workflow-command/activity identities, and zero command attempt/generation/control epoch.
The variants are:

1. **success** — target kind `filter_projection_publication_v1`, target id `projection_id`, zero numeric revision and
   generation, revision token `membership_revision`, winner `publication_terminal_id`, and the complete closed
   `filter_projection_owner_result_ref.v1` enumerated in the machine decision;
2. **deferred stale/not-ready** — target kind `filter_projection_publication_observation_v1`, target id `projection_id`,
   observed membership token and publication winner, plus exact requested/observed refs and freshness/readiness; and
3. **masked missing/foreign** — target kind `filter_projection_masked_error_v1`, target id `result_slot_id`, generation
   `slot_generation`, and a slot-occurrence-bound non-enumerating `projection_not_found` ref.

The owner-result digest is always the SHA-256 of canonical `owner_result_ref`; the named serializer separately owns the
serialized-result digest. Malformed owner bytes, lineage/digest drift, a prepare/accept race, or membership-token drift
leaves the slot `pending` with zero attempt/slot/journal writes. On a valid revalidated proposal, the shared result-slot
owner may write only `agent_tool_result_attempts`, `agent_tool_result_slots`, and `agent_tool_result_journal`; domain,
projection, repair, provider, and model writes remain zero.

## 9. Mechanism-by-invariant audit

The machine decision carries seven mechanism rows and every required invariant column. The compact routing matrix is:

| Mechanism | 1 writer | 2 tenant | 3 fence | 4 lifecycle | 5 partial/late | 6 cost | 7 physical identity | 8 provenance | 9 consistency | 10 isolation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| start authority carrier | start acceptance | workspace/requester | slot generation + winner | immutable exact-copy | missing carrier blocks | budget ref retained | receipt/action/op/root/slot | typed owner ref | closed 18 fields | mode + namespace |
| plan/execution recompile | compiler | start-bound target | two immutable digests | plan -> one execution | partial lanes block | cannot expand ceiling | query + capability | server capability | non-capability equality | opaque runtime ref |
| Cohort terminal | Cohort runtime | workspace in CAS | insert once | no update/resurrection | incomplete bytes zero-write | exact execution evidence | run/receipt/result/marker | path-free typed refs | closed record digest | ref + mode + workspace |
| namespace ref | runtime registry | private workspace binding | immutable ref digest | never retarget | missing row blocks | N/A | ref/private digest/issuer | server registry | closed public/private records | no cross-mode reuse |
| candidate set | Cohort -> projection exact-copy | terminal identity | terminal + membership revision | complete once | duplicate/extra/omit block | <=1000 | candidate/doc/lane digests | committed docs | count and set equality | no cross-terminal rows |
| projection terminal | serving projection | lineage workspace | membership token | new terminal per revision | UoW rollback | zero provider/model | projection/lineage/publication | result is audit predecessor | sole existing UoW | exact opaque ref |
| result slots | shared slot owner | occurrence workspace | member token or slot generation | pending -> accepted once | malformed/race stays pending | read-only | publication/slot winner | closed owner refs | no-command shape | opaque ref only |

Stable implementation obligations are:

- `S1F-LIN-01`: S1f0c1 adds and exact-propagates `acquisition_start_lineage_ref.v1`.
- `S1F-LIN-02`: S1f0c1 implements compiler-owned exact execution recompile and opaque namespace capability binding.
- `S1F-LIN-03`: S1f0c2 installs and implements the commit-once terminal relation/CAS with no legacy backfill.
- `S1F-LIN-04`: S1f0c2 proves candidate-document marker and complete-set equality with bounded adversarial PG tests.
- `S1F-LIN-05`: S1f0d publishes the terminal/carrier only inside the existing parent/member/route UoW.
- `S1F-LIN-06`: S1f1 prepares all three result variants from locked owner state and accepts through the shared slot UoW.

## 10. Dependency and release boundary

```text
S1f0c0 decision
  -> S1f0c1 start propagation + exact recompile
  -> S1f0c2 Cohort terminal + complete-set commit
  -> S1f0d product projection terminal
  -> S1f1 owner-only read + commandless result acceptance
```

`S1f0b-foundation` may proceed independently, but S1f0d requires both its reviewed output and reviewed S1f0c2.
Each implementation batch has its own pinned review promotion edge; a pending review does not block disjoint work.

This decision does not claim implementation, a PG relation, product carrier, reader, result acceptance, E2E, migration,
backfill, served readiness, local-live readiness, or hosted readiness. `served=0`, provider/model/live invocation counts
are zero, R-019/R-029 remain open, and the existing S1f0a `NO-GO 0/10/2/2` remains controlling for the rejected
candidate.
