# Track D D1n S1f0a — filter-projection publication owner decision

> Status: non-live, zero-DDL owner decision lock (2026-07-18). This batch changes no product writer, reader,
> migration, Agent registry population, provider/model route, or serving state. S1f0b must implement the physical
> carrier exactly; S1f1 must then add the owner-only reader and connect the already-defined v2 adapter/result-slot
> path. This is
> author evidence, not an independent-review verdict.

## 1. Outcome and bounded scope

S1f0a ratifies one physical owner for `filter_projection` v2 without adding request fields or inventing a second
population selector:

```text
owner=projection_search_service.filter_projection_publication_owner
owner_revision=filter_projection_publication_owner_v1
eligible_projection_type=run_scope_projection
eligible_start_lineage=start_acquisition_run v2 exact receipt lineage
eligible_provider_modes=simulate,scripted
parent_carrier=serving_projections.metadata.filter_projection_owner_v1
member_carrier=serving_projection_members.provenance.filter_projection_membership_v1
result_link_policy=no_command_v1
migration_delta=0
backfill=forbidden
collection_authoritative_adoption=forbidden
default_public_agent_served_population=0
```

The current `projection_filter_request_v2` target remains exactly
`projection_id + membership_revision + cohort_selection_registry_version +
cohort_selection_registry_digest + cohort_selection_digest`. Planning, execution, result, publication, runtime, and
lane evidence remain server-owned publication state; they are not added to caller-visible request arguments.

This decision does not activate the current canary registry, change the historical v1 filter action, authorize a
provider/model call, or claim live/hosted readiness. R-019 and R-029 remain open.

## 2. Eligibility and rejection boundary

An owner record may be published only when every item below is true before the first projection/member write:

1. the target is one `run_scope_projection` with one nonempty `source_run_id`; `collection_authoritative_projection`
   is ineligible because it may merge multiple runs and cannot claim one planning/execution lineage;
2. the run descends from an exact start-v2 `ActionApproved` confirmation receipt, and the receipt's
   `provider_manifest_identity.manifest_digest` is the planning digest;
3. the exact committed `cohort_execution_result.v1` and candidate-document commit marker agree on the publication
   digest, and its execution-manifest/result digests are nonempty lowercase SHA-256 values;
4. receipt Cohort registry/version/selection identity exact-matches the execution manifest and the committed result;
5. `CohortExecutionCapability` exact-validates and carries `provider_mode=simulate|scripted` plus the canonical
   nonempty isolated `runtime_namespace`;
6. the execution manifest's lane ids/digests and terminal lane summaries form an exact one-to-one set; every lane is
   terminal-complete and every persisted candidate membership names one of those lanes with the same role/status;
7. every visible candidate can be projected to the already-closed owner-candidate schema without raw/private fields,
   path-shaped display data, duplicate identity, or a count above the existing 1,000-candidate bound.

Missing, malformed, mixed-lineage, legacy, replay, live, partial-lane, collection-merged, or equality-alias evidence
is ineligible. S1f0b must fail before parent/member/link/route writes; it must not publish an empty success or a
partially owned projection. `replay` is not normalized to `simulate`, and `live` remains blocked until L1/CS6 provides
the separately reviewed durable live capability.

## 3. Parent carrier and exact field ownership

`serving_projections.metadata.filter_projection_owner_v1` is a closed wrapper:

```text
schema_version=filter_projection_publication_owner_wrapper.v1
owner_record=<closed record below>
owner_record_digest=sha256(canonical_json(owner_record))
```

The closed `owner_record` contains exactly:

```text
schema_version
owner
owner_revision
source_run_id
result_view_id
snapshot_id
cohort_selection_registry_version
cohort_selection_registry_digest
cohort_selection_digest
selection_digest
planning_digest
execution_digest
result_digest
publication_digest
provider_mode
runtime_namespace
cache_provenance
requested_lane_coverage
lane_summaries
terminal_owner_ref
terminal_owner_digest
```

Every scalar string is an exact plain JSON string; booleans/numbers/string subclasses are rejected before string
equality checks. `cache_provenance`, `requested_lane_coverage`, `lane_summaries`, and `terminal_owner_ref` are the
closed object/array values defined below, never string aliases. The canonical JSON form is UTF-8, sorted keys,
compact separators, `allow_nan=false`.

| Owner-record field | Physical source of truth | Derivation / fallback |
| --- | --- | --- |
| `source_run_id` | `serving_projections.source_run_id` and the exact run/result-view lineage | exact-copy; missing or disagreement is ineligible |
| `result_view_id`, `snapshot_id` | locked publication inputs and committed SearchSeedSnapshot/result view | exact-copy; no latest-file lookup or path fallback |
| registry/version/selection fields | start-v2 `ActionApproved` receipt `cohort_identity` plus exact execution-manifest parity | `selection_digest == cohort_selection_digest`; mismatch is ineligible |
| `planning_digest` | receipt `provider_manifest_identity.manifest_digest` | capability-free v2 planning identity; never taken from mutable Job summary |
| `execution_digest` | committed result `cohort_provider_manifest_digest`, exact-equal to the validated capability-bearing execution manifest | no planning/execution aliasing |
| `result_digest` | committed `cohort_execution_result.v1.result_digest` | exact-copy of compiler combine result digest |
| `publication_digest` | committed `cohort_publication_digest` and candidate-document commit marker | both must exact-match |
| `provider_mode`, `runtime_namespace` | exact validated `CohortExecutionCapability` embedded in the committed result | only `simulate|scripted`; no request/ambient fallback |
| `cache_provenance` | cohort runtime binding plus per-lane request-manifest namespace validation | `{cache_scope: isolated_non_live, source_of_truth: projection_search_service.cache_provenance}` only |
| `requested_lane_coverage`, `lane_summaries` | exact execution manifest lanes joined one-to-one to committed lane summaries | committed S1f0 owner is `complete`; absent/extra/duplicate/mismatched lane is ineligible |
| `terminal_owner_ref/digest` | closed ref in section 5 | digest recomputed before publication/read |

The current projection membership revision remains the opaque
`PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY` value minted by the atomic publication owner. It is not copied into the
owner record because the repository mints it during the same publication UoW; the S1f0b reader binds the parent
record digest to the pre/post-fenced revision when S1f1 adds the owner-only reader.

## 4. Member carrier

Each visible member in an eligible owner publication carries
`serving_projection_members.provenance.filter_projection_membership_v1`:

```text
schema_version
owner
owner_revision
parent_owner_digest
candidate_identity_key
memberships
membership_digest
```

Here `schema_version=filter_projection_membership_wrapper.v1`,
`owner=projection_search_service.cohort_lane_membership`, and
`owner_revision=filter_projection_membership_v1`. `membership_digest` is SHA-256 over the canonical object containing
all prior fields.

Memberships preserve execution-manifest lane order, are nonempty and duplicate-free, and each item is a closed plain
string `{lane_id,employment_status,role_bucket_id}` object. Empty `role_bucket_id` represents the compiler's explicit
all-roles lane; it is not inferred from job-title/function heuristics. The model-safe reader emits only the
already-defined public candidate fields and derives the opaque `candidate_ref` from projection id, membership
revision, and private candidate identity. It never exposes the carrier, candidate identity key, raw cache path,
evidence, contact data, or private metadata.

Any membership-changing writer that does not supply a complete valid S1f0 owner set must atomically remove the parent
owner wrapper and every member wrapper. It may not preserve a stale parent digest over new members. An owner-aware
republish recomputes the complete parent/member set; it never patches one candidate in place under the old digest.

## 5. Terminal owner and Agent result link

The closed `terminal_owner_ref` contains exactly:

```text
schema_version
owner
owner_revision
source_run_id
result_view_id
snapshot_id
planning_manifest_digest
execution_manifest_digest
result_digest
publication_digest
```

Here `schema_version=filter_projection_terminal_owner_ref.v1`,
`owner=cohort_provider_runtime.cohort_execution_result`, and `owner_revision=cohort_execution_result.v1`.
`terminal_owner_digest` is SHA-256 over the canonical ref. The ref binds the planning receipt to the committed
execution and publication; a file path or newest-result lookup is never terminal authority.

`filter_projection` remains a read-only Agent action with `result_link_policy=no_command_v1`. S1f1 prepares one exact
result slot, reads the owner snapshot without writes, and passes the recomputed terminal ref/digest as the accepted
owner result. No `workflow_command_id` or `activity_attempt_id` is invented. Missing/stale/not-ready results use the
already-closed error/deferred variants and the same commandless result-slot aggregate.

## 6. S1f1 read algorithm and zero-write behavior

S1f1 must add an owner-only reader; it must not overload the generic public projection endpoint:

1. resolve exact shared-canonical access and require `run_scope_projection`;
2. take the existing finite-deadline publication lock/read snapshot and load the parent wrapper plus opaque membership
   revision;
3. validate/recompute the parent and terminal digests before member reads;
4. read all visible members needed by the bounded result and validate every member wrapper against the parent digest,
   candidate identity, lane manifest, and closed membership shape;
5. reread the projection and require the same owner digest, membership revision, projection state, and visible count;
6. construct the existing `_FILTER_OWNER_SNAPSHOT_TOOL_SPEC` record in memory and call
   `execute_filter_projection_v2`;
7. only the later shared result-slot acceptance UoW may persist the model-safe result.

Missing and foreign projection identity remain the same `projection_not_found` result. Owner/schema/digest/revision
drift is stale/not-ready or fail-closed according to the existing adapter contract. The read path performs zero
projection/member/link/route/domain writes and no repair/backfill.

## 7. JSON carrier decision, reservation, and migration

No DDL is required: both tables already have non-null JSON metadata/provenance columns, the repository already owns a
single atomic parent/member/route publication, and the existing publication lock/revision fences the read. Adding
columns would duplicate the closed owner record and create a mixed-source migration with no current durable consumer.

S1f0b must centrally reserve both carrier keys. Generic publication-field patching must reject the parent key;
generic member patch/upsert must reject direct creation or overwrite of the member key and preserve/remove it only
through the owner-aware full-publication rules above. Collection-authoritative merge must strip both carriers.

There is no backfill. Existing rows remain valid for current public readers but are ineligible for filter v2. The
owner record is additive shadow state until S1f1 and the exact canary tool/result path receive scope-matched review.
Removal of v1 carriers requires a separately reviewed successor, historical tool/result lookup for all durable
references, a zero-reference inventory, and an atomic migration; deleting the keys because a newer writer exists is
forbidden.

## 8. Required S1f0b/S1f1 acceptance matrix

S1f0b physical propagation must prove:

- missing/malformed/uncommitted/digest-mismatched/lane-invalid evidence writes zero parent/member/link/route rows;
- fault injection rolls back the complete owner-aware parent/member/route publication;
- identical replay preserves the same semantic owner digest and existing membership revision;
- concurrent identical/divergent publications serialize under the existing lock;
- generic parent/member mutation rejects reserved-key overwrite, and unrelated patches cannot erase a valid owner;
- collection-authoritative merge and legacy run publication carry no S1f0 owner;
- no new `_connect_with_transaction_lock` caller or independent lock order is introduced.

S1f1 then proves same-owner success, missing/foreign masking, no-ref compatibility, simulate/scripted fixtures,
prepare-to-publication races with zero terminal write, commandless result acceptance/replay/late quarantine, and
model-safe serialization. Both batches keep
`served=0`, provider/model/live invocation count zero, and R-019/R-029 open.
