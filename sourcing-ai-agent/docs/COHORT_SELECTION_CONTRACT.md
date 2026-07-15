# Cohort Selection Contract

> Status: CS1 contract and CS2 deterministic provider compiler implemented. Normal workflow execution remains deliberately fail-closed until the acquisition runtime delegates to the compiled Harvest lane boundary; frontend controls, result projection, and live validation remain separate batches.

## Product behavior

`cohort_selection` is the optional user-owned population boundary for a sourcing request. It supports multi-select role buckets, multi-select current/former employment status, and explicit `any`/`all` role matching without binding the product contract to one provider's query syntax.

The v1 wire object is:

```json
{
  "schema_version": "cohort_selection.v1",
  "role_bucket_ids": ["research", "engineering"],
  "employment_statuses": ["current", "former"],
  "role_match": "any",
  "source": "user_explicit"
}
```

- `role_bucket_ids` is an ordered, duplicate-free list from `ROLE_BUCKET_KNOWLEDGE`. An empty list means all roles.
- `employment_statuses` is a non-empty ordered subset of `current` and `former`.
- `role_match` is `any` or `all`.
- External API requests may only use `source=user_explicit`. `legacy_adapter` and `inferred` are server-owned provenance values.
- Unknown fields, unknown values, duplicate values, unsupported versions, and invalid sources fail closed.
- A present flat mirror may not be `null`; canonical and singular role aliases may not both be present. If `intent_axes`, `population_boundary`, or `thematic_constraints` is present alongside a cohort, each must have object shape and every present cohort mirror must be non-null and exact.
- Callers do not supply registry or selection digest fields. The server derives the immutable registry pin (`cohort_selection.registry.v1` plus SHA-256) and a source-insensitive selection digest from the canonical object.

## Ownership matrix

| Contract | Owner / source of truth | Normal consumers | Forbidden consumers | Fallback / migration | Deletion condition |
|---|---|---|---|---|---|
| v1 validation and canonical ordering | `src/sourcing_agent/cohort_selection.py` | `JobRequest`, request normalization, API ingress, future provider compiler | provider-local validators and UI hard-coded enums | none | not applicable |
| selectable role ids, labels, order | `ROLE_BUCKET_KNOWLEDGE` in `query_signal_knowledge.py` | options endpoint and cohort validator | a second UI/provider role registry | none | not applicable |
| registry and selection identity | `COHORT_SELECTION_REGISTRY_VERSION`, `cohort_selection_registry_digest()`, and `cohort_selection_digest()` | options endpoint, request/matching reuse identity, provider manifest, and future result audit metadata | caller-authored digest fields or provider-local hashes | private hash input includes role aliases, provider role hints, and function ids; the public options projection remains only id/label/order | version bump is required for a breaking registry interpretation |
| employment status and role-match options | `cohort_selection.py` | options endpoint and cohort validator | ad hoc endpoint lists | none | not applicable |
| explicit request authority | canonical `cohort_selection` object | normalized/effective request and later compiler | model normalization and refinement model patches | flat fields remain exact compatibility mirrors | flat mirrors may be removed only after every planner, matcher, retrieval path, artifact, and stored request reader consumes the canonical object |
| effective role targeting | `resolve_effective_role_targeting()` in `cohort_provider_compiler.py` | acquisition strategy and provider compiler | raw text, categories, role-like facets, model patches, and provider-local inference when `source=user_explicit` | absent canonical object retains the legacy resolver unchanged | remove legacy branch only after old request retirement |
| physical provider lanes and merge semantics | `CohortProviderCompiler` | planning manifest and `HarvestProfileSearchConnector.search_profiles_for_cohort_manifest()` | multi-role or multi-status assumptions inside one Harvest call | one lane per `(employment status, role)` or status-only lane for all roles | not applicable |
| execution readiness and provider budget | typed `CohortExecutionCapability`, owned by the cohort runtime cutover | compiler exact-copy plus connector preflight | caller booleans or manifest-authored readiness | absent capability produces `cohort_selection_execution_not_ready`; one manifest-wide call/item/output budget is allocated before the first lane | not applicable |
| all-role proof | versioned `CohortRoleProofVerifier` matching the capability's exact verifier id/revision | compiler combiner after connector preflight | provider-authored `normalized_role_bucket_ids`, metadata flags, or a caller boolean | absent/mismatched verifier fails before the first provider call | not applicable |
| plan-review cohort merge | `merge_plan_review_cohort_selection()` | review approval and approved-review execution override | ad hoc field overlay or swallowed validation errors | stored explicit permits absent/exact replay; stored legacy can atomically upgrade to explicit | not applicable |
| request-family reuse fence | registry-pinned explicit selection digest in `request_matching.py` plus `source_request_covers_explicit_cohort()` | criteria rerun baselines, idempotency candidates, asset-reuse plans, snapshot/authoritative projection reuse, and feedback-family weighting | similarity score, company fallback, or latest-company fallback across a different/absent explicit selection | no explicit object preserves legacy matching; stale bundles are rebuilt from their request payload; malformed historical requests are ignored rather than aborting the current request | remove only with a replacement execution-identity owner |
| legacy request adaptation | `effective_cohort_selection(...)` | future consumers that explicitly request an effective object | normal request serialization and signatures | lazy `source=legacy_adapter`; no write-back | remove after persisted legacy requests and all old clients are retired |

## Mirror and precedence rules

When `cohort_selection` is absent, `JobRequest.cohort_selection` remains `None`, `JobRequest.to_record()` omits the key, and existing flat/NLP request semantics and signatures remain unchanged.

When it is present:

1. `must_have_primary_role_buckets` mirrors `role_bucket_ids`.
2. `employment_statuses` mirrors the canonical employment list.
3. If either flat mirror or its existing `intent_axes` mirror is present, it must be equal after canonical normalization; otherwise ingress returns HTTP 400 before model, history, plan-review, job, criteria, or provider writes.
4. Missing flat mirrors are installed from the canonical object.
5. Model request normalization and model-assisted post-acquisition refinement cannot override a `user_explicit` selection. A direct external flat refinement conflict is rejected rather than silently changing the cohort. Server-owned `inferred` and `legacy_adapter` objects are provenance snapshots, not user authority locks: if a later internal patch changes their mirrors, the stale object is retired and can be re-derived by its owning server path.
6. `source` is provenance, not matching semantics. Every `user_explicit` selection is bound to request/matching reuse identity by its registry-pinned digest; an absent object retains the legacy signature byte-for-byte.

## Role authority and provider compilation

For `source=user_explicit`, `role_bucket_ids` is the complete role authority. Raw request text, categories, role-like facets, normalization models, refinement models, and legacy flat fields cannot add another role. An empty role list means all roles and suppresses role inference, technical-function defaults, job-title filters, and role-like provider keywords.

`CohortProviderCompiler` emits registry-ordered physical lanes. A selection with two statuses and three roles produces six lanes; an empty role list with two statuses produces two status-only lanes. Every lane has a stable lane id/digest and exactly one role's `function_ids`/`job_titles`. Caller/model role filters are removed before compilation; role-like `keywords` and `scope_keywords` are removed while non-role thematic constraints remain.

- `role_match=any` uses deterministic lane-order union plus candidate-identity dedupe.
- `role_match=all` uses per-status lane intersection followed by a deduplicated union across statuses. It ignores/removes provider-authored proof fields and accepts role evidence only from a versioned execution-owned verifier. Preflight requires the exact id/revision and a callable verifier before any provider call; a later verifier exception becomes stable whole-manifest failure rather than a partial result.
- Every accepted row carries server-derived `cohort_lane_membership` entries for the exact qualifying lane id, employment status, and role bucket. Input rows cannot author that field. This preserves current/former and multi-role provenance after identity dedupe for the runtime snapshot/result projection.
- The manifest records canonical compiler inputs. The Harvest boundary deep-copies one request-local snapshot, exact-recompiles and compares the entire manifest, then exact-matches the separately supplied execution capability; a changed readiness bit, lane, filter, registry pin, budget, or recomputed ordinary digest fails before directory/cache/provider work.
- Provider item limits are allocated across the complete lane set from one global result budget; planned calls/items and the deterministic output cap are manifest fields. The connector derives the minimum exact page count for each lane allocation, uses `auto_probe=false`, and selects one async run-id dispatch path so a failed/ambiguous sync request cannot submit a second paid run inside the same call. It requires every declared lane exactly once and never treats unavailable/malformed/missing results as an empty success. Cross-process exact replay still requires the runtime batch to durably bind manifest/lane identity to submission state and provider run id before live validation.
- Candidate rows must resolve to the canonical person identity owner before union/intersection. Missing identity fails the whole manifest. Same canonical LinkedIn identity dedupes even when one Harvest lane supplies a LinkedIn URL and another supplies only `publicIdentifier`; non-LinkedIn URLs are never promoted to LinkedIn identity and are ignored in favor of a valid public identifier or fail closed.
- Cohort execution enables strict raw-result validation before the legacy parser can discard malformed items. Non-object/mixed envelopes and null async dataset pages fail the whole lane, while a genuine empty array remains a successful zero-result lane; a malformed suffix can never publish a parsed partial prefix.
- A later-lane failure returns a stable whole-manifest error with completed lane ids; it publishes no combined result. Per-lane stable ids/directories and provider cache make retry bounded and inspectable, but prior external calls cannot be rolled back and remain explicit partial-attempt evidence.

The current general acquisition runtime has not yet been cut over to that boundary. Therefore `queue_workflow`, blocking workflow, synchronous `run_job`, and acquisition resume return or raise stable `cohort_selection_execution_not_ready` before provider execution. Planning/explain/review remain usable and expose the exact compiled manifest; this gate must be removed only in the runtime cutover that delegates every cohort acquisition lane to the compiler boundary.

## Plan review

A stored `user_explicit` selection permits no incoming cohort override or an exact canonical replay. Any conflicting object or flat/axis mirror returns stable invalid/HTTP 400 before the review/job write. A stored legacy request may receive one externally valid `user_explicit` object; the canonical object, ordered mirrors, statuses, and `role_match` are installed atomically. Cohort validation exceptions are never converted into a stale execution-bundle fallback.

An approved review whose request or plan changed must also rebuild a coherent execution bundle before the sole review write. If that rebuild fails, `plan_review_execution_bundle_rebuild_failed` is returned with zero review/job writes; a prior legacy bundle cannot be persisted beside the new explicit request.

The explicit selection digest is a hard request-family identity fence, not a similarity weight. Different digests and present-versus-absent selections score zero and are ineligible for baseline/snapshot/authoritative projection reuse, latest-company fallback, company-only feedback fallback, or related-family feedback weighting even when company, flat mirrors, roles, and statuses otherwise overlap. Historical feedback or source jobs with missing, legacy, different, or malformed cohort identity are ignored for an explicit request; exact same-identity history remains eligible.

An explicit criteria rerun baseline is owner-checked first and then cohort-checked before baseline-result reads or policy execution; an explicit idempotency-key hit is likewise only a candidate when its stored request has the same cohort identity. Asset-reuse compilation filters registry rows through their source jobs and emits `baseline_source_job_id` for explicit requests. Inherited force-fresh suppression requires that provenance to resolve to the same exact cohort, so a stale plan or registry pointer cannot silently install a delta/reuse baseline.

## Read API

`GET /api/cohort-selection/options` returns the stable role, employment, and role-match option catalog plus `registry_version` and `registry_digest`. Role options are derived from `ROLE_BUCKET_KNOWLEDGE`; the endpoint does not maintain another registry. Provider compilation binds `cohort_selection_digest()` today, and future result projection must copy it without adding caller-controlled fields to the v1 object.

## Bounded CS1 exclusions

This batch does not make live provider/model calls, add SQL, render frontend controls, or project cohort/result audit fields. The general acquisition-runtime cutover is also still gated. That cutover must add a durable per-lane submission checkpoint (manifest/lane identity, submission state, provider run id, ambiguous-submit handling, resume, exact replay, and terminal reuse) before paid live validation. Those follow-ups must consume the canonical object, compiler manifest, and digests rather than re-derive role or employment semantics.

## Foundation validation evidence

- Focused selection/compiler/matching/confidence/owner-fencing regression: `125 passed + 64 subtests`.
- Extended planning/refinement/Cohort/owner-fencing regression: `215 passed + 64 subtests`, with one failure reproduced identically on clean `e04faf8` (`test_acquisition_strategy_prefers_intent_view_over_conflicting_flat_fields`).
- Harvest connector regression: `121 passed`, with one failure reproduced identically on clean `e04faf8` (`test_profile_match_accepts_requested_opaque_identifier_even_when_profile_name_is_blank`).
- Ruff check/format passed on all 20 changed Python files; focused mypy passed on five Cohort/matching/confidence/asset-reuse owners. The repository ratchet remains exactly `81 errors / 4 files` under `make typecheck`.
- The adjacent PG organization-profile class produced `1 passed / 14 failed` on both this tree and clean `aa53ba7`; every failure occurs during repeated schema bootstrap because the shared test fixture truncates migration history, before Cohort/asset-reuse code runs. The exact affected node passes when run alone on clean `aa53ba7`; this pre-existing harness defect remains a separate Track D test-infrastructure repair.
- No full `tests/test_pipeline.py`, paid provider/model, or live environment was used. This is author evidence only; fresh pinned independent review remains required before runtime/live signoff.
