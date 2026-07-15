# Cohort Selection Contract

> Status: CS1 contract, CS2 deterministic provider compiler, CS3 isolated non-live runtime adapter, CS4 user interaction, the CS5 scripted service E2E, and the criteria-write provenance fixed-forward are implemented. The CS3/CS5 formal review of `60e7e67` was `NO-GO 0/6/4/0`; `17e1607` plus `c6c0fcd` are the author fixed-forward and require a fresh pinned non-author review. `simulate`/`replay`/`scripted` workflows delegate to the compiled Harvest lane boundary and persist one canonical search-seed/result audit contract; the default-off frontend picker round-trips explicit selections through submit/revision/recovery/review. Paid live execution and the retrieval-only `run_job` helper remain fail-closed.

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
| execution readiness, runtime identity, and provider budget | typed `CohortExecutionCapability`, issued by `cohort_execution_capability_for_runtime()` only for an existing validated isolated runtime directory | compiler exact-copy, workflow gate, acquisition adapter, and connector preflight | caller booleans, request fields, manifest-authored readiness, empty namespaces, production+non-live, or mode/namespace drift | capability binds exact `provider_mode` and canonical `runtime_namespace`; the connector re-derives both before directory/cache/provider work; one manifest-wide call/item/output budget is allocated before the first lane | replace the non-live-only policy only after durable live lane checkpoint acceptance |
| provider plan authority | capability-free `provider_execution_manifest` emitted by the planner and exact-recompiled by the acquisition adapter | plan/explain/review payloads and workflow acquisition state | missing, edited, or capability-bearing stored manifests | stored manifest must exist and equal the exact capability-free recompilation before any provider call; runtime then recompiles from the same canonical inputs plus the server capability | no alternate manifest source |
| snapshot-local Cohort raw cache | `HarvestProfileSearchConnector` runtime binding plus request-manifest provenance | exact retry inside the same provider mode/runtime namespace | another mode, runtime, unproven legacy raw file, or shared-cache provenance | Cohort cache path is mode+runtime namespaced and reuse requires exact payload, response path, mode, namespace, and cache namespace | not applicable |
| runtime result projection and publication readiness | `_build_cohort_execution_result()` plus `SearchSeedSnapshot`/candidate-document projection; candidate documents carry the final publication digest marker | guarded search-seed loaders, acquisition state, downstream candidate projection, result audit | a parallel Cohort store, provider row shape, an uncommitted summary/result artifact, or direct artifact existence as readiness | zero/all-rejected returns blocked before result/snapshot/projection/reawaken writes; success is readable only when candidate documents contain the same `cohort_publication_digest` | not applicable |
| all-role proof | versioned `CohortRoleProofVerifier` matching the capability's exact verifier id/revision; non-live owner is `CohortHeadlineRoleProofVerifier` | compiler combiner after connector preflight | provider-authored `normalized_role_bucket_ids`, occupation/position/current-position synthesis, metadata flags, or a caller boolean | parser exposes exact `public_headline` plus `public_headline_source`; absent/mismatched proof fails or excludes the row | not applicable |
| canonical Cohort person URL | `canonical_cohort_profile_url()` and server-owned `cohort_canonical_profile_url` emitted after lane validation | search-seed adapter, candidate documents, profile prefetch | raw non-LinkedIn `url|profile_url`, caller-authored canonical field, or a second adapter normalization | valid LinkedIn URL wins; otherwise a valid public identifier becomes the canonical LinkedIn URL; missing identity fails the lane | not applicable |
| plan-review cohort merge | `merge_plan_review_cohort_selection()` | review approval and approved-review execution override | ad hoc field overlay or swallowed validation errors | stored explicit permits absent/exact replay; stored legacy can atomically upgrade to explicit | not applicable |
| request-family reuse fence | registry-pinned explicit selection digest in `request_matching.py` plus `source_request_covers_explicit_cohort()` | criteria rerun baselines, idempotency candidates, asset-reuse plans, snapshot/authoritative projection reuse, and feedback-family weighting | similarity score, company fallback, or latest-company fallback across a different/absent explicit selection | no explicit object preserves legacy matching; stale bundles are rebuilt from their request payload; malformed historical requests are ignored rather than aborting the current request | remove only with a replacement execution-identity owner |
| external criteria request aliases | `prepare_external_criteria_request_payload()` in `cohort_selection.py` | `/api/criteria/feedback`, `/api/criteria/confidence-policy`, `/api/criteria/recompile`, and the orchestrator write boundary | endpoint-local fallback ladders across `request`, `request_payload`, or `metadata.request_payload`; caller-authored `inferred`/`legacy_adapter` objects | every present alias is independently validated and must canonicalize identically; an absent request remains absent | remove aliases only after all clients send the single canonical field |
| criteria mutation request provenance | `prepare_criteria_write_payload()` in `criteria_request_provenance.py`, using the exact-owner-checked stored job request/plan and server-derived `request_signature_context()` | feedback, confidence-policy, recompile, and their repository metadata | caller-selected request/signature/matching metadata after a job reference; repository `setdefault` authority; endpoint-local authorization exceptions | every explicit `job_id`, `baseline_job_id`, or `source_job_id` uses the same exact-owner binding; no-job-reference writes retain their legacy path; open-mode jobs may bind a server-stored `inferred`/`legacy_adapter` request; exact caller replays are accepted but replaced with server-derived values | not applicable |
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

The workflow acquisition runtime now delegates explicit cohorts to that boundary only when the server-owned runtime mode is `simulate`, `replay`, or `scripted`. The explicit-Cohort fence runs before legacy full-roster, scoped, former, investor, or caller-mutated strategy dispatch. New plans emit one full manifest task and no second former task. A hydrated legacy former task may only reuse a committed full-manifest result whose exact compiler inputs, lane digests, result digest, and complete former-lane set match; it never recompiles a narrower provider plan.

The adapter exact-recompiles the capability-free planning manifest from the frozen request and plan-owned acquisition filter hints and compares it with the stored plan manifest before provider/cache/artifact work. It then recompiles with the exact runtime capability, executes the lane set, and adapts the combined rows once into the existing durable `SearchSeedSnapshot` and candidate-document projection. `cohort_execution_result.v1` records the selection/manifest/result digests, capability, lane summaries, counts, artifact path, commit-marker path, and publication digest; raw lane results remain in their normal provider assets.

A genuine zero-result or all-proof-rejected execution returns a blocked attempt payload before any result, snapshot, candidate-document, reawaken, profile-prefetch, or acquisition-state write. Successful publication uses `candidate_documents.json` as the final commit marker: search-seed summary/result files may be staged first, but all normal loaders reject them unless the candidate-document source contains the exact same `cohort_publication_digest`. A projector/process failure therefore leaves inspectable partial files but no readable canonical Cohort snapshot.

Paid `live` receives no capability until manifest/lane identity, provider submission state, run id, ambiguous submission, resume, exact replay, and terminal reuse have a durable owner. The retrieval-only `run_job` helper also remains unavailable because it cannot execute provider lanes. Non-live capabilities bind the exact `cohort_headline_role_classifier.v1` proof owner: it classifies only the normalized public headline through the central role registry, hashes that evidence, and rejects an `all` candidate unless the required roles are proven across the qualifying rows. Missing or ambiguous headline evidence is exclusion, never provider-authored proof. Planning/explain/review continue to expose the capability-free manifest in every mode.

## Runtime result and publication field ownership

The persisted result is an audit object, not an independent readiness source. `cohort_execution_result.json` is canonical only as part of a publication whose candidate-document commit marker exact-matches its digest. The post-commit in-memory `publication_committed=true` convenience field is never persisted and cannot replace that check.

| Field(s) | Owner and exact derivation | Allowed values / fallback | Consumers and forbidden use | Fast preflight |
|---|---|---|---|---|
| `schema_version` | `_build_cohort_execution_result()` literal | exactly `cohort_execution_result.v1`; no fallback | serializers and audit readers; callers cannot author it | `CohortAcquisitionRuntimeTest.test_non_live_manifest_flows_into_one_durable_search_seed_contract` |
| `provider` | exact compiled manifest `provider` | current value `harvest_profile_search`; blank is invalid for promotion | audit/result readers; never infer from artifact path | same runtime-contract test |
| `cohort_selection_digest` | exact compiled manifest selection digest | 64-char SHA-256 | request/result family audit; never recompute from provider rows | same runtime-contract test plus selection registry tests |
| `cohort_provider_manifest_digest` | exact execution manifest digest | 64-char SHA-256 | compatibility reuse, audit, lane binding; never use a capability-free preview digest as executed identity | runtime-contract and former-compatibility assertions |
| `result_digest` | `CohortProviderCompiler.combine_lane_results()` over the accepted combined result | non-empty digest on a publishable result | exact replay/audit and legacy former reuse; never substitute candidate count | runtime-contract test |
| `candidate_count` | `len(entries)` after canonical identity and role-proof filtering | positive integer for a persisted publication; zero returns blocked and is not persisted | result/projection audit; not a provider-reported total | fresh/stale zero and all-rejected matrix in `test_cohort_provider_compiler.py` |
| `truncated_count`, `rejected_unverified_count`, `missing_required_lane_count` | exact compiler combine counters | non-negative integers; publishable result requires `missing_required_lane_count=0` | result quality and reuse proof; no count may be derived from another | compiler combination and runtime-contract tests |
| `lane_summaries[*]` | connector execution owner copies exact `lane_id`, `lane_digest`, employment status, role bucket, provider item limit, accepted row count, and namespaced raw path | every declared lane exactly once; no missing/extra lane | audit and exact full-manifest reuse; raw paths are evidence only, never candidate identity | compiler boundary plus runtime-safety matrix |
| `execution_capability` | exact `CohortExecutionCapability.to_record()` issued by the validated runtime | exact schema/owner/policy, non-live mode, canonical non-empty runtime namespace, positive call/item/output budgets, matched proof owner | compiler/connector preflight and audit; caller/request/stored preview cannot mint or edit it | `tests/test_cohort_provider_runtime_safety.py` |
| `artifact_path` | builder receives the canonical result path | exact `.../cohort_provider_discovery/cohort_execution_result.json` | audit/location only; existence is not readiness | runtime-contract serialization preflight |
| `commit_marker_path` | builder receives the canonical candidate-document path | exact snapshot `candidate_documents.json` | publication verifier; must not point to a raw or summary asset | runtime-contract serialization preflight |
| `cohort_publication_digest` | SHA-256 of the path-independent result core: schema/provider/selection+manifest+result identity, all counters, lane summaries, and capability | 64-char SHA-256; missing/mismatch fails closed | search-seed summary, candidate-document source, guarded loaders; never caller-authored | injected projector-failure and committed-loader assertions |
| `publication_contract` | builder-owned literals | schema `cohort_publication_commit.v1`, state `requires_candidate_documents_digest_match`, digest field `cohort_publication_digest` | audit/readers; no alternate marker or implicit file-order readiness | runtime-contract serialization preflight |
| `publication_committed` | acquisition adapter adds `true` only after reading the matching candidate-document marker | ephemeral `true` in the returned execution payload; absent from the persisted result | immediate caller convenience only; durable readers must revalidate the digest | runtime-contract test plus mandatory-PG Cohort E2E |
| candidate `metadata.cohort_lane_membership` and `cohort_canonical_profile_url` | compiler lane combiner and canonical person-identity owner | exact qualifying lanes and canonical LinkedIn URL | candidate documents/profile prefetch; raw row claims cannot author either field | real scripted connector identity/proof regression |

## Plan review

A stored `user_explicit` selection permits no incoming cohort override or an exact canonical replay. Any conflicting object or flat/axis mirror returns stable invalid/HTTP 400 before the review/job write. A stored legacy request may receive one externally valid `user_explicit` object; the canonical object, ordered mirrors, statuses, and `role_match` are installed atomically. Cohort validation exceptions are never converted into a stale execution-bundle fallback.

An approved review whose request or plan changed must also rebuild a coherent execution bundle before the sole review write. If that rebuild fails, `plan_review_execution_bundle_rebuild_failed` is returned with zero review/job writes; a prior legacy bundle cannot be persisted beside the new explicit request.

The explicit selection digest is a hard request-family identity fence, not a similarity weight. Different digests and present-versus-absent selections score zero and are ineligible for baseline/snapshot/authoritative projection reuse, latest-company fallback, company-only feedback fallback, or related-family feedback weighting even when company, flat mirrors, roles, and statuses otherwise overlap. Historical feedback or source jobs with missing, legacy, different, or malformed cohort identity are ignored for an explicit request; exact same-identity history remains eligible.

An explicit criteria rerun baseline is owner-checked first and then cohort-checked before baseline-result reads or policy execution; an explicit idempotency-key hit is likewise only a candidate when its stored request has the same cohort identity. Asset-reuse compilation filters registry rows through their source jobs and emits `baseline_source_job_id` for explicit requests. Inherited force-fresh suppression requires that provenance to resolve to the same exact cohort, so a stale plan or registry pointer cannot silently install a delta/reuse baseline.

## Criteria write provenance

The three external criteria mutation endpoints validate every present nested request alias before invoking an orchestrator handler. A caller may supply only `source=user_explicit`; malformed mirrors, server-owned provenance values, non-object aliases, and canonical disagreement return HTTP 400 with zero feedback, policy, compiler, result, or derived-job writes. The same validation runs again at the orchestrator boundary so direct callers cannot bypass HTTP ingress.

For feedback, confidence-policy, and recompile, every explicit `job_id`, `baseline_job_id`, and `source_job_id` is exact-owner checked before any main-domain write. The selected job is re-read and its stored request is the sole request-family authority. A caller-provided request must be an exact canonical replay; target, request, matching projection, or signature disagreement fails closed before repository/compiler execution. The server then replaces all provenance metadata with values derived from that stored request. A foreign or missing reference remains the indistinguishable `job_not_found` response; the HTTP boundary returns the same 404 body for both.

This binding is independent of `rerun_retrieval`: the flag controls only whether retrieval is launched after a valid write, never whether authorization or provenance validation runs. Automatic baseline selection remains requester-and-tenant scoped. A valid mutation without a job reference keeps the pre-existing company-only behavior, and open-mode operation may consume a server-stored `inferred` or `legacy_adapter` request because that provenance was not authored by the external caller.

## Read API

`GET /api/cohort-selection/options` returns the stable role, employment, and role-match option catalog plus `registry_version` and `registry_digest`. Role options are derived from `ROLE_BUCKET_KNOWLEDGE`; the endpoint does not maintain another registry. Provider compilation binds `cohort_selection_digest()`, and the non-live acquisition result copies that identity into `cohort_execution_result.v1` without adding caller-controlled fields to the v1 request object.

## Frontend interaction

The optional picker is default-off, so a legacy user action omits `cohort_selection` rather than sending an inferred or empty compatibility object. Once enabled, the frontend consumes only the public options projection for ids, labels, ordering, employment values, and the default role-match value. It may select multiple roles and both current/former statuses; the explicit `All roles` control serializes the canonical empty role list without introducing a frontend-owned registry.

The same canonical object is copied without re-derivation through initial submit, revision submit, history recovery, and plan review. An already explicit reviewed plan displays the frozen selection read-only; a legacy plan may opt in and atomically upgrade through the backend merge contract. Frontend model, recovery, or display helpers cannot add role/status values that were not in the canonical object.

## Bounded CS3 exclusions

These batches do not make live provider/model calls, add SQL, or activate the retrieval-only helper. The next live-runtime batch must add a durable per-lane submission checkpoint (manifest/lane identity, submission state, provider run id, ambiguous-submit handling, resume, exact replay, and terminal reuse) before paid validation. Every follow-up must consume the canonical object, compiler manifest, and digests rather than re-derive role or employment semantics.

## Foundation validation evidence

- Focused selection/compiler/matching/confidence/owner-fencing regression: `125 passed + 64 subtests`.
- Extended planning/refinement/Cohort/owner-fencing regression: `215 passed + 64 subtests`, with one failure reproduced identically on clean `e04faf8` (`test_acquisition_strategy_prefers_intent_view_over_conflicting_flat_fields`).
- Harvest connector regression: `121 passed`, with one failure reproduced identically on clean `e04faf8` (`test_profile_match_accepts_requested_opaque_identifier_even_when_profile_name_is_blank`).
- Ruff check/format passed on all 20 changed Python files; focused mypy passed on five Cohort/matching/confidence/asset-reuse owners. The repository ratchet remains exactly `81 errors / 4 files` under `make typecheck`.
- At the CS1/CS2 foundation anchor, the adjacent PG organization-profile class produced `1 passed / 14 failed` on both that tree and clean `aa53ba7`; every failure occurred during repeated schema bootstrap because the shared test fixture truncated migration history before Cohort/asset-reuse code ran. The independent fixture repair is recorded in the CS3 evidence below.
- No full `tests/test_pipeline.py`, paid provider/model, or live environment was used. This is author evidence only; fresh pinned independent review remains required before runtime/live signoff.

## CS3 non-live runtime validation evidence

- Compiler/gate/acquisition adapter plus full Cohort ingress regression: `64 passed + 55 subtests`.
- The adapter regression proves a four-lane current/former × research/engineering manifest becomes one durable search-seed snapshot, candidate-document projection, and `cohort_execution_result.v1`; a forged stored planning manifest performs zero connector calls.
- The adapter also executes the real Harvest boundary under a temporary two-role `role_match=all` scripted scenario, proves the versioned headline evidence, and proves no live submit function is reached.
- Invalid candidate identity is now validated inside each lane before the next call; a first-lane failure records its lane with zero completed lanes, and a verifier failure records the exact completed-lane tuple.
- Ruff format/check and Python compilation passed on the runtime/compiler/orchestrator/test scope. Broader acquisition/transport and scripted product E2E are required before this batch is promoted.
- The PG fixture defect cited above was repaired independently in `ccd704a`: `schema_migrations` is preserved while domain tables are reset, with `2` dedicated and `8` adjacent PG tests passing.
- No full `tests/test_pipeline.py`, paid provider/model, or live environment was used. This is author evidence, and the fresh pinned review of the prior CS1/CS2 commit remains asynchronous rather than being represented as a review of this new runtime diff.

## CS4 frontend validation evidence

- Frontend production build passed with `84 modules`; frontend plan contract passed `7 tests`.
- Backend options/ingress and exact registry/review-copy nodes passed `5 tests + 3 subtests`.
- A Playwright transport smoke proved the legacy action omits the field and an explicit action sends the exact canonical research+engineering/current/any object.
- Ruff format/check passed on the transport contract scope. This is author evidence; fresh pinned non-author review remains required before manual/live signoff.

## CS5 scripted service E2E validation evidence

- Mandatory-PG `make ci-cohort-scripted-e2e PYTHON_BIN=.venv/bin/python TEST_PYTHON_BIN=.venv/bin/python`: `1 passed` against an isolated, version-migrated PG schema.
- The test enters through `SourcingOrchestrator.run_workflow_blocking()`, uses the real plan/compiler/acquisition/materialization/result path, executes two scripted Harvest profile-search lanes plus one scripted profile-enrichment batch, and finishes `completed/completed` with one `current` candidate in the public asset population.
- The public card carries the selected employment outcome, while the durable candidate document retains the exact `research+engineering` lane membership, employment selection, manifest digest, and `cohort_headline_role_classifier.v1` proof. `cohort_execution_result.v1` records two one-row lanes and one deduplicated candidate.
- The live Harvest submit function is a hard-failing sentinel through all joined background work. The public `get_job_results_api()` projection is linked through `run_projection_link`, has complete profile readiness, reports clean contamination, and records scripted mode for every provider invocation. Durable events prove both queued and terminal outreach reconcile; the adjacent-reconcile retry regression passes `2` exact pipeline nodes without running the full file.
- Fixed-forward fast scope: `85 passed + 69 subtests`; adjacent planning scope: `164 passed + 69 subtests` with its sole non-Cohort failure reproduced identically on clean `af4db41`; Ruff check/format, Python compilation, and diff checks are green. Global mypy remains exactly `81 errors / 4 files`.
- The valid pinned Ultra review of `7d54a23..60e7e67` was formal `NO-GO` with `P0/P1/P2/P3=0/6/4/0`. The author fixed-forward is `17e1607` (runtime/mode/cache/headline/identity) plus `c6c0fcd` (dispatch/publication/field contract/mandatory E2E). This mapping is not a closure verdict: a fresh pinned non-author artifact must cover the new enclosing commit. The reviewer-exclusive home proved `gpt-5.6-sol/ultra/priority`, but later attempts hit `usageLimitExceeded`; no unavailable review is represented as `GO`.
- No full `tests/test_pipeline.py`, paid provider/model, or live environment was used. Paid canary and product signoff remain blocked by CS6 plus a scope-matched valid review or an explicitly recorded founder exception.

## CS5a criteria-write provenance validation evidence

- Canonical provenance, owner-fencing, and Cohort ingress regression: `106 passed + 60 subtests`; adjacent API transport/confidence/evolution regression: `50 passed + 66 subtests`.
- PG request-scope owner-fencing adjacency: `12 passed`. The matrix covers conflicting job/request Cohorts, forged signatures, external server-owned sources and malformed mirrors, plus same-owner, no-reference, and open-mode positive paths before any feedback/compiler/policy write.
- Ruff check/format, Python compilation, and diff checks passed on the complete implementation/test scope. Global mypy remains exactly `81 errors / 4 files`.
- The exact regression-matrix inventory node reports the same pre-existing 20 unmapped modules on this tree and clean `48c43e1`; the new `criteria_request_provenance.py` owner is mapped by its focused paired test and does not appear in that failure.
- No full `tests/test_pipeline.py`, paid provider/model, or live environment was used. This is author evidence only; a fresh pinned non-author review must cover the fixed-forward before it can close the prior advisory findings.
- Confidence-policy owner fixed-forward: the authenticated API now supplies exact requester+tenant expectations, all three job-reference spellings use the shared canonical provenance owner, and missing/foreign references return `job_not_found` before policy/version/compiler/result/derived-job writes. Focused runtime/API/confidence/evolution coverage is `174 passed + 126 subtests`; the PG owner adjacency is `13 passed`. A reviewer-exclusive app-server attempt verified `gpt-5.6-sol / ultra / priority` at `thread/start` but then failed closed on `usageLimitExceeded`, so it is not formal review evidence and does not unblock live signoff.
