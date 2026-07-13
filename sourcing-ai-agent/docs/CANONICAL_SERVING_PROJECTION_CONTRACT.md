# Canonical Serving Projection Contract

> Status: Top-level contract under implementation. Drafted 2026-05-18 to guide the next post-ECS implementation slice. Implementation started 2026-05-19 with the Testcontainers PG harness, ServingProjection storage/writer foundation, event-time run projection publication, collection-authoritative merge queue, projection public reader API v1, and frontend `/projections/:projection_id` cutover. Read with `JOB_RESULT_LIFECYCLE_DESIGN.md`, `WORKFLOW_PROGRESS_CONTRACT.md`, `AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md`, `DATA_ASSET_GOVERNANCE.md`, `FRONTEND_API_CONTRACT.md`, and `NEXT_TODO.md` before changing public readers, result routes, asset reuse, target-candidate flows, or workflow/result-page coupling.
> D-3 semantic revision (owner approved 2026-07-13): exact canonical visible membership owns board population;
> profile readiness, card readiness, and explicit profile capture are independent projections over that population.

## Purpose

The current result-serving model still carries too much historical coupling:

- result pages are addressed by `job_id`, even though the user is really viewing a result set
- public readers can still derive board state from job summaries, lifecycle rows, overlays, stage files, and artifact sidecars
- large baseline assets can leak into a scoped run board and make a single acquisition job pay the cost of serving a full local collection
- local asset consumption, acquisition workflow execution, candidate board viewing, and target-candidate operations are still too tightly coupled

The durable model is: **a run executes work; a projection serves candidates**.

Every user-visible result board must be backed by a persisted `ServingProjection` addressed by `projection_id`. A run may create or update a projection, but public result APIs must not treat the run itself as the serving source of truth.

## Implementation Status

Current foundation implemented on 2026-05-19:

- `serving_projections`, `serving_projection_members`, and `projection_manifest_shards` exist in the control-plane schema.
- `run_projection_links` and `collection_authoritative_pointers` exist in the control-plane schema as foundation records for `/runs/{run_id}` to `/projections/{projection_id}` linking and collection-first local asset entry resolution.
- `store.repos.serving_projection` owns projection upsert/list/get, member upsert/replace/list/count, manifest shard upsert/list/get, run/projection link upsert/list/get, and collection authoritative pointer upsert/get; the retired `ControlPlaneStore` domain facades must not reappear.
- `ServingProjectionWriter` is the first owner-facing writer facade for publishing run-scope projections and collection-authoritative projections through the storage foundation instead of scattering low-level table writes.
- PG-only and mirror control-plane modes know these tables, and member counting uses a PG `COUNT(*)` path instead of scanning all rows.
- A missing authoritative PG table is an availability/integrity failure, not an empty-set result. PG-only generic reads raise; projection member count/readiness/page failures become fail-closed `status=not_ready` / `reason=projection_members_unavailable` at public readers (HTTP 409 at the API boundary), while internal builders abort before terminal or index-finalization writes.
- `ServingProjectionWriter` publishes projection metadata, members, and the run-link or collection-pointer route through `publish_serving_projection`, a single-connection PG domain unit of work. It acquires the logical scope lock before rereading/selecting the stable random projection identity, then acquires the schema-namespaced projection lock before assigning publication time and writing parent, members, and route in one transaction. Empty replacement sets atomically clear membership, and any member chunk or route failure rolls back the complete publication without an orphan projection or an advanced route.
- `tests/test_serving_projection_storage.py` covers local storage semantics, member dedupe, field visibility defaults, and the rule that manifest shards are audit refs rather than online membership.
- `tests/test_serving_projection_writer.py` covers writer-owned run link creation, member publication, idempotent run projection reuse, and collection pointer switching.
- `tests/testcontainers_pg_contract.py` provides the first disposable PG contract harness.
- Workflow event-time result-view publication now writes a linked `run_scope_projection` and persists the projection summary in `job_result_views.metadata.run_scope_projection`.
- Stage 1 row-shell publication creates a canonical run-scope projection before profile/card completion, so a result page can serve candidate rows while profile detail continues.
- Completed run-scope projection publication enqueues a background `collection_authoritative_merge` durable item. The recovery tick drains this as a separate background phase through `ServingProjectionWriter.publish_collection_authoritative_projection(...)`.
- Public reader API v1 exists for `GET /api/runs/{run_id}/projection-link`, `GET /api/projections/{projection_id}`, and `GET /api/projections/{projection_id}/candidates`. It fails closed on missing/unservable projection and reads online membership only from `serving_projection_members`.
- Projection candidate pages support exact backend filtering/paging over projection membership using the shared public candidate filter contract. The reader returns `filtered_candidate_count`, `filter_signature`, `filter_contract`, `field_visibility`, and `read_contract.fallback_used=false`.
- Frontend result routing can resolve `/results?job=...` through the run/projection link and navigates to `/projections/{projection_id}`. Projection pages hydrate/paginate through the projection API and keep projection-only pages read-only for job-bound CRM/review/profile-completion actions until source-projection CRM writers exist.
- Profile actor provider envelopes have begun decoupling from post-profile durable-unit caps: provider envelope default is `HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS=300`, while local durable units remain separately capped by `HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS=200`.

Not implemented yet:

- field-level permission APIs beyond default row-shape storage
- backend projection facet/search APIs backed by raw/evidence indexes
- collection-first local asset entry
- PersonSummaryView / PersonAsset / CRM source-projection writers
- historical job-result migration and legacy endpoint retirement
- complete containerized workflow signoff gates for projection cutover

Do not interpret public reader API v1 as full cutover completion. The normal new frontend path can serve canonical projections, but legacy job-result endpoints still exist until offline migration, route cutover, and signoff gates retire them.
## Non-Negotiable Rules

1. Result URLs use `/projections/{projection_id}`. Run/execution URLs use `/runs/{run_id}`.
2. `projection_id` is a stable random identifier such as `proj_...`; semantic labels live in metadata.
3. Any acquisition run that produces a candidate set must create a `run_scope_projection`, even while profile/card detail is still streaming.
4. Public readers consume canonical projection state only. Missing canonical projection is fail-closed, not an invitation to rebuild from overlay, job summary, stage files, or lifecycle fallbacks.
5. Local asset consumption does not require an acquisition run. It resolves a company collection to its active authoritative projection.
6. A run board defaults to the current run scope, not the full baseline. Baseline/full collection merge is explicit, not the default.
7. Raw provider rows can remain in audit artifacts, but frontend-visible board counts are canonical deduped candidate counts.
8. User-saved candidate selections are not projections. They belong to target-candidate / CRM / export modules and reference source projections for provenance.
9. Migration is offline backfill plus cutover. Legacy job-result endpoints cannot remain a normal dual-track serving path after cutover.
10. Projection membership is not CRM membership. Local assets and projection rows do not enter CRM unless a user, import, migration, or approved Agent action explicitly adds the person to CRM.
11. Projection APIs must expose field visibility and readiness scopes. They must not leak contact fields, CRM notes, raw evidence, or restricted assets merely because the person is in the projection.
12. Exact canonical visible membership owns the board population `N`. Main candidate sync, pagination totals, projection-sourced export totals, and projection-sourced CRM selection totals use `N/N`; card readiness does not gate or redefine them.
13. Exact card readiness owns card-detail progress `C/N`. Profile readiness and card readiness are independent: each may be ahead of the other, and there is no `card_ready <= profile_ready` invariant.
14. Explicit profile capture is an independent evidence fact. It must not be inferred from `profile_ready`, `card_ready`, fetched counts, or row visibility.
15. An authoritative exact membership publication may correct `N` and revision-bound readiness aggregates upward, downward, or to zero. `projection.membership_revision` is an opaque equality token, not a sortable version; readers obtain the current publication through the projection/run-link owner and never choose by count magnitude or token order.
16. Non-exact, inconsistent, or fallback-backed membership reads fail closed. They do not become an empty projection, a partial global total, or permission to read legacy summaries, overlays, patch logs, or frontend-loaded rows as the replacement source.

## Core Objects

### AssetCollection

An `AssetCollection` is the durable local asset namespace for a company or another future entity type.

V1 company collection ids use the LinkedIn company URL slug as the stable key:

```text
company:{linkedin_company_slug}
```

Examples:

```text
company:google
company:lovable-dev
company:openai
```

Display names are metadata only. They are not stable collection keys because they can collide or change.

### AcquisitionRun

An `AcquisitionRun` is the execution record for provider search, profile fetch, local apply, board publication, and finalization.

Run pages answer execution questions:

- what intent was planned
- which provider/search/profile workers ran
- what is pending, failed, or complete
- what budgets, retries, and recovery actions were used
- which projection was produced

Run pages do not own result serving after projection creation.

### ServingProjection

A `ServingProjection` is a persisted candidate-set read model.

It stores membership, scope, counts, readiness, provenance, source watermarks, and serving metadata. It must not copy large raw profile payloads. Candidate detail and raw/evidence data are referenced through identity keys and asset/index layers.

### Projection Manifest Storage

Projection membership should use a hybrid storage model:

- PG membership rows are the online source of truth for public readers.
- Sharded sidecar/object-store manifests are audit, migration, rebuild, and cold-path artifacts.
- Public APIs must not scan sidecar/object manifests on request to serve normal candidate pages.

Recommended logical tables:

| table/object | purpose |
| --- | --- |
| `serving_projections` | projection metadata, type, scope, counts, readiness, provenance, watermarks |
| `serving_projection_members` | online membership index keyed by `projection_id + candidate_identity_key` |
| `projection_manifest_shards` | object/sidecar refs for audit, migration, offline rebuild, and compaction |

`serving_projection_members` should hold the fields needed for stable online reads:

- `projection_id`
- `candidate_identity_key`
- `person_identity_key`
- stable rank/order keys
- source shard/lane provenance
- row/profile/card readiness flags
- explicit profile-capture evidence flag or owned reference when the capture owner has published one
- visibility flags
- lightweight summary reference or denormalized bounded row cache
- created/updated/published timestamps

The public projection parent exposes `projection.membership_revision`, owned by the member-publication UoW and sourced
from the existing member semantic-input revision. It is an opaque equality token used to bind summary/page/readiness
snapshots and invalidate caches; this contract does not require a duplicate token column on every member row.

Rules:

- `/api/projections/{projection_id}/candidates` reads the PG membership index for pagination/filter membership and joins compact summary/overlay data through service APIs.
- Large projections such as Google 8k+ boards must not re-read or rebuild full overlays on the request path.
- Sidecar/object manifests may be used by offline builders, migration, backfill, audit, and compaction. They are not public-reader fallbacks.
- If PG membership is missing or invalid while a sidecar exists, public readers fail closed with a projection repair diagnostic. They do not silently serve from the sidecar.
- A full member replacement must use `upsert_row_and_replace_rows` to upsert `serving_projections`, delete the existing `serving_projection_members` scope, and merge every replacement chunk in one PG transaction guarded by a schema-namespaced advisory lock for the `projection_id`. `members=[]` is a valid atomic clear.
- An incremental member publication must use `upsert_row_and_upsert_rows` to upsert the same projection parent and merge all supplied members in one PG transaction. Direct repository member merges use that same publication-lock key. Incremental and replacement writers therefore serialize on one projection identity rather than racing a delete against an unlocked merge.
- Normal run-scope and collection-authoritative publication must use `publish_serving_projection`: the parent, incremental or replacement members, and run-link or collection-pointer route commit atomically on one connection. A failed member merge, replacement, or route write rolls back the complete publication, so routing metadata cannot advance independently and a newly generated projection cannot be left orphaned.
- A publication may expose a global visible-member total only when its member set, visible count, count scope, and publication revision are one exact atomic product. An exact empty replacement is `N=0`; a missing/partial read is `unavailable`, never an inferred zero.
- The member-publication UoW is the only owner allowed to set `projection.membership_revision`. Summary, page, readiness, export, and projection-to-CRM reads must echo the same non-empty token. The token supports equality/inequality only; it must not be lexically, numerically, or chronologically sorted. Token mismatch invalidates caches and makes a direct response merge fail closed until the consumer re-resolves and re-reads one pinned projection snapshot.
- `created_at`, `updated_at`, `published_at`, row sequence, and count magnitude are not membership revision substitutes. In particular, `updated_at` cannot disambiguate two publications in the same timestamp resolution and must not be used as a membership CAS or merge key.
- Card/profile readiness aggregates are independently reduced from their owned per-member facts for the same membership revision. They are each bounded by `N`, but neither is derived from or ordered against the other.
- Initial membership publication must not compute public facets by scanning the just-published members. It publishes facet/index state as `pending` or `unavailable`; only the revision-fenced `projection_person_search_index` owner may publish exact public facet counts.

### CollectionWriter

The `CollectionWriter` is the only owner of collection-authoritative merges. A run projection can request a collection update, but it must not mutate the authoritative collection projection directly.

This boundary exists to keep dedupe, provenance, coverage, manual overlays, collection versioning, rollback, and audit behavior centralized.

### RawProfileIndex / CandidateEvidenceIndex

Raw profile search and evidence-backed filters must be served from backend indexes derived from raw profile assets, not from frontend scans and not from lightweight card fields alone.

The board can remain fast by paging lightweight candidate rows while backend search/filter APIs return candidate ids from indexed profile/evidence fields.

Implementation slice added 2026-05-19:

- `projection_person_search_index` is the projection-scoped online search membership index.
- `PersonAssetWriter.rebuild_projection_person_search_index(...)` is the current paged builder entrypoint. It reads `serving_projection_members`, `person_assets`, `person_evidence`, and `person_assertions`, then writes index rows and projection watermarks without loading the full projection into the public request path.
- `/api/projections/{projection_id}/search` serves keyword search from this index and returns only public projection rows plus index readiness. It must not expose raw profile payloads, raw evidence bodies, or restricted contacts.
- `/api/projections/{projection_id}/candidates?search=...` and structured projection filters use the same index. If the index is missing, the normal public reader response is fail-closed with `status=not_ready`, `reason=projection_person_search_index_unavailable`, and `read_contract.fallback_used=false`; it must not scan projection membership rows on the request path. A legacy membership-scan fallback is allowed only under explicit operator/migration opt-in (`SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK=1`) and must report `filter_contract.fallback_used=true`.
- An authoritative index count or row read failure has the same fail-closed public reason as a missing index; it must not be converted to an empty result. A later authoritative membership read failure is classified separately as `projection_members_unavailable`.
- Index results are membership claims, not best-effort enrichment hints. Public readers batch-load visible members, require the requested page length and unique key set to match exactly, reject missing/hidden/cross-projection rows, and restore index order before CRM/media enrichment. Any mismatch fails the entire page with `projection_person_search_index_unavailable`; it must not reduce the row count while retaining the old `matched_count`/offset.
- `/api/jobs/{job_id}/candidates` propagates canonical projection-index `not_ready` without recomputing counts or pagination and returns HTTP `409`. Both job and projection frontend adapters preserve an explicit `filtered_candidate_count=0`; they must not replace it with the total count through truthiness fallback.
- Projection-index fencing uses three reserved metadata keys with separate ownership:
  `projection_person_search_index_input_revision` is the current semantic member-input revision,
  `projection_person_search_index_build_input_revision` binds a build to that input, and
  `projection_person_search_index_build_generation` identifies the individual build. Ordinary member publication owns
  the input revision and advances it only when index-relevant member semantics change; a semantically identical replay
  preserves it. A same-count member replacement is still a semantic change and must advance it.
- A reset rebuild keeps the previous index until the replacement first page is ready. Under one projection-specific
  advisory-lock transaction, it compares the previously observed build generation and semantic input revision, writes
  the new generation, binds `projection_person_search_index_build_input_revision` to the current input revision, and
  performs the scoped replace. Empty projections use the same fenced atomic replace with zero rows. `updated_at` is not
  an index-input identity or CAS token; a delayed reset is obsolete even when the projection timestamp has not changed.
- Continuation row writes, partial-state publication, finalization, and public facet publication must all compare the
  same build generation and require the build-bound input revision to equal the current semantic input revision in the
  transaction that applies the write. A delayed page or state write returns obsolete and leaves the current index and
  projection metadata intact. Once a generation is `completed`, the same generation cannot downgrade it to `building`
  or `partial`.
- Public index search, filter, summary, and list reads require a non-empty build generation and
  `projection_person_search_index_build_input_revision == projection_person_search_index_input_revision`. Readers
  validate this state before and after the indexed read; stale input or a revision change during the read fails closed
  as index unavailable rather than serving mixed-generation rows.
- Index-derived public facet counts and index readiness are completion products, not independent mirrors. Finalization
  binds both products to the same generation, build-input revision, and current input revision in the generation-fenced
  UoW that marks the build `completed`. A semantic member change atomically advances the input revision and invalidates
  the old facet/readiness product; a reset atomically invalidates it again before replacing the first index page.
  Unfiltered public readers require `completed`, equal build/current revisions, and matching product bindings before
  reporting facet or index readiness. Empty completed projections publish the same verified product with exact zero
  counts; they are not treated as a missing product.
- Projection-row publication/upsert, including bulk conflict metadata updates, must preserve all three reserved keys.
  Only the fixed semantic member-publication UoW may advance the input revision, and only the generation-fenced index
  writer may bind build revision/generation or publish build state.
- Event-time index scheduling is durable: run-scope projection publication, collection-authoritative projection publication, and Public Web assertion promotion enqueue `projection_person_search_index_build`; recovery drains it through `projection_person_search_index_queue`.
- Projection global facet counts are an index build product. When `projection_person_search_index` finalizes, the writer pages persisted public `filter_record` rows and publishes `counts.public_facet_counts` onto `serving_projections`. Public projection readers may render those counts, but they must not compute global facets by scanning job overlays, raw profile JSON, stage files, or frontend-loaded candidate rows.
- `projection_facet_layering_build` is projection-owned work. The builder must resolve classifier input from canonical `serving_projection_members`, even when `overlay_info` still carries a legacy overlay path. Its completion updates projection member layer fields and enqueues `projection_person_search_index_build`; the old overlay is an artifact compatibility surface, not the canonical layer/filter source for projection reads. Jobs without a canonical projection link fail closed with `canonical_projection_link_required_for_projection_facet_layering`; they must be backfilled into a run projection before layering can run.
- Production backfill entrypoints are `/api/projections/backfill-person-summary-views` and `/api/projections/backfill-person-search-indexes`. They are migration/operator paths, not public reader repair paths.
- This is still projection-scoped. A broader collection/person-level RawProfileIndex/CandidateEvidenceIndex can be added later if projection-scoped indexes prove insufficient for cross-collection search.

### Target Candidate / CRM

Target candidates are a separate product module. They may be sourced from any projection, but they should not be tied to a job/run lifetime.

Target-candidate records should store source provenance such as:

- `source_projection_id`
- `source_run_id` when applicable
- `candidate_identity_key`
- selected/exported timestamp
- operator action and reason

Manual exclude, status changes, edits, and future CRM operations are overlay/state events. They should not physically delete raw local assets.

Current cutover status:

- Target-candidate page normal state is CRM-owned: list/add/update uses `/api/crm/records` and `CRMWriter`, not legacy job-bound target rows.
- Candidate-level Public Web normal frontend routes are CRM-owned: `/api/crm/records/public-web-search`, `/api/crm/records/{crm_record_id}/public-web-search`, `/api/crm/records/{crm_record_id}/public-web-promotions`, and `/api/crm/records/public-web-export`.
- CRM Public Web read/write/export/execution ownership is `crm_public_web_v1`: CRM routes must require real `crm_records` ids, must fail closed for legacy target-candidate-only ids, and must report `public_web_storage_owner=crm_public_web_v1` / `public_web_execution_backend=crm_public_web_v1` / `legacy_target_candidate_state_owner=false`. Normal CRM Public Web execution and promotion must not create target-candidate bridge rows or write legacy Public Web promotion rows. Any `target_candidate_public_web_v1` execution backend in a normal CRM Public Web action is a blocking signoff failure, not an acceptable warning.
- CRM Public Web public reads must not repair or synthesize owner state. If `crm_public_web_runs` lacks a matching owner row, detail/export reports no CRM Public Web result even if `person_public_web_assets.latest_run_id` points at a legacy target-candidate run.
- Legacy `/api/target-candidates/public-web...` HTTP aliases are permanently retired and return `410` with canonical CRM endpoint pointers. `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS` is ignored after W7e and must not re-enable frontend-normal, smoke-normal, migration-test, or internal execution paths. Historical target-to-CRM sync, if still needed from cold evidence, requires explicit reviewed migration tooling and `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC=1`; it is not a serving fallback.

## Identity Contract

The registry-backed LinkedIn URL normalization remains the v1 identity foundation.

Required fields:

| field | meaning |
| --- | --- |
| `raw_profile_url` | provider-returned URL, preserved for audit |
| `canonical_profile_url` | cleaned user-facing LinkedIn URL |
| `profile_url_key` | normalized/sanity key used for registry dedupe and projection membership |
| `person_identity_key` | v1 stable person key, `linkedin:{profile_url_key}` |
| `candidate_identity_key` | projection membership key; v1 equals `person_identity_key` when LinkedIn identity is present |

Rules:

- Two rows with the same `profile_url_key` represent the same candidate unless an explicit alias/conflict table says otherwise.
- Projection writers must dedupe by `candidate_identity_key` before publishing frontend-visible counts.
- Raw source rows that collapse into one candidate remain audit provenance, not separate board rows.
- Future non-LinkedIn identities may enter a pending alias table, but they must not silently merge into a LinkedIn candidate without explicit evidence.

## Projection Types

### `run_scope_projection`

A `run_scope_projection` is the default result for an acquisition run.

It represents the candidate population produced or selected by that run scope:

- OpenAI Agent baseline+delta should serve the Agent delta/run scope by default.
- no-baseline scoped search should serve only the scoped acquisition result.
- live roster should serve the deduped roster result for that run.
- Google vision-language large shard should not make the run board pay the full 8k baseline serving cost unless the planner explicitly chose a merged baseline mode.

Properties:

- `source_run_id` is required.
- `collection_id` is required when the run is company-scoped.
- `scope_spec` is the normalized business scope from the effective request, such as keywords, lanes, employment status, provider lanes, and baseline policy.
- Candidate membership is stable within one opaque membership token, but a newly authoritative exact publication may add, remove, replace, or clear visible members. Consumers re-resolve authority rather than ordering unequal tokens. Profile/card/capture readiness may also change only through their owned token-bound facts; none is a monotonic substitute for membership.
- It can become row-shell serving before all profiles/cards are fetched, as long as counts and readiness states are explicit.

### `collection_authoritative_projection`

A `collection_authoritative_projection` is the current company-level local asset view.

It is used by the local asset consumption entry, not by every run board by default.

Properties:

- It is created by the `CollectionWriter`.
- It dedupes and merges candidates across completed runs and imported assets.
- It carries coverage/freshness metadata by shard, employment status, provider source, profile completeness, and manual overlay state.
- It is versioned. New authoritative versions are published by creating a new projection and switching the collection pointer atomically.

### Not A Projection: User-Saved Candidate Sets

User-saved selections should be modeled as target-candidate / CRM / export objects, not as `ServingProjection` types.

Acceptable future names include `candidate_list`, `shortlist`, `export_snapshot`, or CRM-specific records. They may reference a projection and selected candidate ids, but they do not participate in the canonical serving projection state machine.

## Collection Authoritative Pointer

Each collection has a `collection_authoritative_pointer`.

The pointer stores:

- `collection_id`
- `active_projection_id`
- `active_collection_version`
- `previous_projection_id` for rollback
- publication timestamp
- writer/audit metadata

This pointer is not just a convenience alias. It prevents readers from guessing "latest by updated_at", supports atomic switch/rollback, and creates a governance point for compaction, retention, and migration.

Retention rules:

- keep the latest N full authoritative manifests per collection
- keep older projection metadata/audit records even when bulky manifests are compacted
- never copy large raw profile payloads into projection versions
- allow offline compaction to consolidate old versions once a newer projection subsumes them

The exact N is an implementation parameter, not a contract blocker.

## Persisted Projection Shape

The logical projection record should include at least:

| field | purpose |
| --- | --- |
| `projection_id` | primary id, `proj_...` |
| `projection_type` | `run_scope_projection` or `collection_authoritative_projection` |
| `collection_id` | company/local asset namespace |
| `source_run_id` | nullable for collection-only projections |
| `projection_version` | schema/version for membership and summary shape |
| `membership_revision` | opaque member semantic-input equality token owned by the member-publication UoW; not sortable |
| `state` | projection serving state |
| `scope_label` | display label only |
| `scope_spec_json` | normalized request/scope semantics |
| `candidate_identity_manifest_ref` | sharded candidate id/key manifest, not raw profile payload |
| `source_collection_version` | collection version used to build this projection, if applicable |
| `raw_profile_index_watermark` | index freshness boundary used for search/filter |
| `evidence_index_watermark` | evidence/filter freshness boundary |
| `counts_json` | canonical counts and count scopes |
| `readiness_json` | row/profile/card/index/compaction readiness |
| `provenance_json` | source runs, shard coverage, provider lanes, raw-row audit references |
| `manual_overlay_version` | exclude/status/edit overlay watermark |
| `created_at`, `updated_at`, `published_at` | timestamps |

The candidate manifest can be stored as PG rows, sharded sidecar, or object-store object. The contract is that public reads resolve it through the projection service, not by scanning arbitrary job artifacts.

Derived global summaries such as facet counts and outreach layering are projection-owned build products. They should be produced by bounded durable builders keyed by `projection_id`, with chunk/time budgets, progress checkpoints, and readiness fields. Normal projection-ready runs target persisted `ServingProjection` membership directly. The job-scoped serving overlay target is migration input only; it cannot execute facet/layering work unless first backfilled into a canonical run projection.

## Permission And Visibility Contract

Projection membership answers "which people are in this result set." It does not by itself authorize every field about those people.

Public projection APIs must apply field-level visibility before returning rows:

| field group | default visibility |
| --- | --- |
| `public_summary` | default projection row fields: name, headline, company/title, location, LinkedIn URL, lightweight experience/education summary, avatar media ref, profile fetched/indexed timestamps |
| `projection_metrics` | projection counts/readiness: row/profile/card readiness, count scope, facet/layering status |
| `crm_overlay_summary` | compact read-only CRM chip: in-CRM flag, stage, owner, last event timestamp |
| `restricted_contact` | email/phone/contact assertions and exportability metadata; not returned in default board rows |
| `internal_evidence` | Public Web/DataForSEO evidence excerpts, source URLs, confidence, identity match; evidence/detail APIs only |
| `raw_private` | raw profile JSON, raw HTML/PDF, provider payloads, LLM intermediate reasoning; never returned by default public APIs |
| `debug_internal` | worker ids, artifact paths, provider run ids, recovery metadata; diagnostics/operator APIs only |

Rules:

- `PersonSummaryView` should be the compact row source for projection candidates.
- `PersonAsset`, `PersonEvidence`, and `PersonAssertion` remain separate stores with their own visibility policy.
- `/api/projections/{projection_id}/crm-state` may return compact CRM overlay for visible candidates, but it must not create CRM records and must not expose CRM notes by default.
- `/api/projections/{projection_id}/persons/{candidate_identity_key}` and `/api/persons/{person_identity_key}` may return public-safe person detail summaries, assertion summaries, asset summaries, evidence summaries, and compact CRM overlay. They must scrub default rows and must not return raw profile payloads, CRM notes, or unreviewed restricted contact values as ordinary public fields.
- Projection search/filter may use raw/evidence indexes, but returned rows must still be bounded summaries plus explicit count/readiness scope.
- Export APIs must re-check assertion and CRM visibility. They cannot rely on a field being visible in the browser row.
- `/api/projections/export` is the v1 projection export surface. Default export includes public summary fields and human-promoted active assertions, records assertion provenance, supports selected `candidate_identity_keys`, and records skip reasons for unreviewed or non-exportable assertions. CRM notes and restricted contact export require explicit approval/policy.
- `/api/target-candidates/export` is not a normal export path after cutover. It may be enabled only with `SOURCING_ALLOW_LEGACY_TARGET_CANDIDATE_EXPORT=1` for explicit migration/test coverage; otherwise it returns `410` with the canonical projection export path.
- Default projection row APIs return only `public_summary`, `projection_metrics`, and optional `crm_overlay_summary`.
- `restricted_contact`, `internal_evidence`, `raw_private`, and `debug_internal` must use explicit APIs, permissions, and export policies.
- Frontend candidate-board code must not merge multiple data sources to reconstruct hidden fields or global counts. If a field is not in the projection row response, it must be requested through the owned detail/evidence/CRM/export API.

## Projection Build Product Ownership

Projection-owned derived products include:

- candidate identity manifest
- row shell publication state
- profile/card readiness counters
- global facet counts
- outreach/layering summaries
- raw/evidence index watermarks
- CRM overlay read watermark
- compaction readiness

Each derived product must have:

- a writer owner
- a readiness field
- a count scope
- a bounded builder or event-time writer
- a fail-closed public-read behavior when missing or stale

Public reads may report stale/missing build products, but they must not rebuild them synchronously from job artifacts, overlays, or raw profile JSON.

## Readiness State

Projection readiness is not one scalar.

V1 should expose separate readiness dimensions:

| dimension | examples |
| --- | --- |
| `row_readiness` | `not_serving`, `row_shell_serving`, `complete` |
| `profile_readiness` | independently owned required/ready/failed-or-not-required counts and latest profile revision; not derived from card readiness |
| `card_readiness` | exact materialized/display-ready `C/N` for the bound membership revision; not derived from profile readiness |
| `explicit_profile_capture` | independently evidenced capture count/status; unavailable when the capture owner has not published exact evidence |
| `index_readiness` | raw/evidence index watermark and partial/full status |
| `compaction_readiness` | background full artifact/index compaction status |

For an exact membership revision, let `N` be visible members, `C` be card-ready members, and `P` be profile-ready members. `0 <= C <= N` and `0 <= P <= N`, but there is no ordering between `C` and `P`. Explicit profile capture is a fourth independently evidenced dimension and has no inferred equality with either count.

User-visible result readiness is anchored to exact visible membership and `row_readiness`, so a board with `N>0` and `C=0` can render row shells while card details remain `0/N`. Exact `N=0` renders a real empty state. Missing/non-exact membership renders `not_ready`, not an empty state. Background card/profile work and full snapshot compaction are not allowed to block the result page once exact membership rows are serving.

## Write Ownership And State Transitions

### Run Projection Writer

The run projection writer creates `run_scope_projection` as soon as a run has durable candidate-set evidence.

It owns:

- run/projection link creation
- row-shell publication
- exact visible-membership publication and its canonical revision
- projection of independently owned profile/card/capture readiness facts for that same revision
- final projection serving state for the run result

It does not own collection-authoritative merge.

### Collection Writer

After a run finishes, it may enqueue a collection merge by default. That merge:

- reads the run projection and source assets
- dedupes by `candidate_identity_key`
- updates coverage/provenance/manual overlay state
- builds a new `collection_authoritative_projection` version
- switches `collection_authoritative_pointer` atomically

The run board must not wait for this merge.

### Raw/Evidence Indexer

Profile local apply should enqueue raw/evidence indexing work. The indexer updates watermarks used by projection search/filter APIs.

If the index lags raw profiles, the frontend should show indexed freshness using China time fields such as `profile_fetched_at` and `profile_indexed_at`. V1 does not automatically expire profiles; users may trigger force-fresh acquisition manually.

### Manual Overlay Writer

Manual exclude/status/edit operations are state overlays. They update collection/projection visibility through versioned overlay watermarks, not by physically deleting raw assets.

## Public API Contract

### Routes

Required result routes:

```text
/runs/{run_id}
/projections/{projection_id}
```

Expected API shape:

```text
GET /api/runs/{run_id}
GET /api/runs/{run_id}/progress
GET /api/runs/{run_id}/projection-link
GET /api/projections/{projection_id}
GET /api/projections/{projection_id}/candidates
GET /api/projections/{projection_id}/facets
GET /api/projections/{projection_id}/search
GET /api/projections/{projection_id}/crm-state
GET /api/collections/{collection_id}/authoritative-pointer
```

Exact endpoint names can change during implementation, but the ownership cannot:

- run endpoints serve execution state
- projection endpoints serve result/candidate state
- collection endpoints resolve local asset entry state

### Legacy Endpoints

After migration cutover, legacy job-result endpoints must not compose normal result payloads.

Allowed post-cutover behavior:

- redirect to the linked projection
- return migration-required diagnostic
- return `410 Gone` for unbackfilled legacy data

Not allowed:

- rebuilding lifecycle from old job summary
- serving candidates from overlay paths directly
- mixing job result view, lifecycle, stage files, and candidate sidecars in a fallback ladder

## Public Reader Rules

Public readers must:

- resolve `projection_id`
- load the persisted projection record
- load candidate membership through `serving_projection_members`
- apply projection-row filters through the shared backend candidate filter contract
- return explicit readiness and count scope

Projection reader v1 rules:

- `serving_projection_members` is the online source of truth for pagination and projection membership.
- Only a successful authoritative PG count of zero means an empty projection. A missing authoritative table and count/readiness/page query failures must not become `0`, `[]`, or `{}`: public projection routes return `not_ready` with `projection_members_unavailable`, while internal consumers propagate the failure and perform no empty-set completion/finalization side effects.
- A public total is canonical only when `counts.count_scope=exact_projection`, `read_contract.source=serving_projection_members`, `read_contract.fallback_used=false`, `read_contract.fail_closed=true`, and projection summary/page/readiness carry the same non-empty `projection.membership_revision`. Any missing, non-exact, token-mismatched, or fallback-backed product fails closed.
- Direct projection response merging compares `membership_revision` for equality only. A mismatch does not mean either token is greater; the consumer discards the mixed snapshot, invalidates the affected page/cache, re-resolves the authoritative projection/run link, and re-reads summary and page under one pinned token.
- Exact canonical visible membership `N` owns the main candidate sync `N/N`, unfiltered pagination total, projection export input total, and projection-to-CRM source selection total. Export policy/permission skips and actual CRM record membership remain separately reported outcomes; they must not rewrite source projection `N`.
- `card_ready_count` / the board compatibility alias `display_ready_candidate_count` owns card-detail progress `C/N`. It never owns page membership or the main sync numerator, and `C=0` does not prevent exact visible rows from rendering.
- `profile_ready_count`, `card_ready_count`, and explicit profile-capture count are independently sourced. Readers validate each against `0..N` for the bound revision but must not impose a cross-dimension inequality or synthesize one from another.
- `GET /api/projections/{projection_id}/candidates` may page membership rows for unfiltered candidate windows. Active search/filter requests must use `projection_person_search_index`; if the index is unavailable, the reader fails closed rather than scanning all membership rows. It must not read overlay files, job summaries, stage files, candidate sidecars, raw profile JSON, or manifest sidecars as normal fallback sources.
- The response must include `candidate_count` / `total_candidates` for the whole projection and `filtered_candidate_count` for the requested filter.
- `filter_contract.backend_filtered_paging_supported=true` means the backend, not the frontend loaded-row window, owns filtered paging for this projection. If `facet_count_scope` / `index_filter_readiness.count_scope` is `unavailable`, the frontend must fail closed by disabling search/facet controls instead of issuing active filter requests that the projection reader will reject.
- Facet counts remain `pending` / `unavailable` after initial membership publication until the revision-fenced `projection_person_search_index` owner publishes exact projection-wide counts. Membership rows, current pages, overlays, and frontend caches are not facet fallbacks.
- Projection-only result pages are read-only for job-bound actions such as manual-review enqueue and profile-completion. Adding a person to CRM/target candidates is allowed only through the projection-aware CRM writer path with source projection/person identity provenance; the frontend must not silently call old job-bound target-candidate APIs with an empty `job_id`.
- Implemented foundation: `/api/projections/{projection_id}/crm-state` and projection candidate rows may expose compact read-only CRM overlay from `crm_records`, but this path must not create CRM records. Creating CRM records from a projection is an explicit write through `/api/crm/records` and `CRMWriter`.
- Projection-sourced CRM and export mutations require the non-empty membership revision displayed to the user. The server does not bind a missing token to whatever revision is current when the request arrives. A stale token returns `not_ready`/HTTP 409 and performs no domain write; an explicit idempotency key reused with different immutable input returns a conflict rather than replaying the old action.
- Projection-bound dispatch serializes revision validation and command planning with `operation_dispatch:{operation_run_id}` followed by `serving_projection_publication:{projection_id}` session locks. Every transaction that contends on either namespace must acquire its transaction lock through the pool-safe try-lock path: a busy attempt rolls back and returns its connection to the pool before backoff, while the successful attempt keeps the same connection and lock for the complete transaction. A blocking advisory-lock wait while holding a pooled connection is forbidden because it deadlocks at `POOL_MAX=1` when the session-lock owner needs the pool. The contract preflight must exercise real concurrent publication and cancel waiters at `POOL_MAX=1`, not only probe lock availability.
- A cancel request that waited behind an in-flight dispatch may observe one non-terminal status advance before its fixed UoW obtains the dispatch lock. The high-level cancel writer retries that structured CAS conflict once with the committed non-terminal status; terminal winners and a second conflict remain fail-closed. This retry does not cancel an already queued workflow command, so command cancellation/effect fencing remains in R-019 rather than being implied by an `OperationRun` status.
- A projection-bound operation that discovers a stale membership revision before dispatch uses the fixed PG `stale_input_failure` transition: lock the operation event stream, CAS the locked `operation_run`, lock and validate its linked action/workspace, update both rows to `failed`, and append exactly one revision-bound `OperationInputRevisionStale` event in the same transaction. Exact replay must preserve the event identity; an event failure, linked-action mismatch, or terminal-state conflict rolls back the whole transition. This UoW does not include a workflow command or CRM domain mutation, so the broader R-019/R-028 boundary remains explicit.
- Projection-to-CRM selection uses the fixed PG `projection_crm_selection_uow`: acquire the projection publication advisory lock, re-read exact revision/`N`/selected visible members, acquire sorted workspace-person identity locks, then write `crm_records`, `crm_engagements`, and `crm_events` in one transaction. A revision/count/member mismatch, duplicate selected candidates for one person, or any table-write exception leaves the complete batch unchanged. Same revision/event replay is `idempotent`; a new revision for an existing person is `reselected` and appends a revision-bound event without creating a second engagement.
- CRM rows expose one atomic `metadata.last_source_selection` tuple containing `projection_id`, `membership_revision`, `source_candidate_count`, `candidate_identity_key`, and `person_identity_key`. Target-candidate export must accept a selection only when every selected row has a complete tuple and all tuples share the same projection/revision. It must not combine top-level projection fields with independently updated metadata fields.
- The fixed selection UoW does not claim that every legacy CRM edit or workflow-command completion is in the same transaction. Legacy `add_person_to_crm`/`update_crm_record`, command cancellation, Activity/entity-delta recording, and command terminal CAS retain the explicit R-019/R-028 recovery boundary until the CRM repository/command-completion migration unifies them. No signoff document may describe the current scope as global CRM exactly-once.
- `ControlPlaneStore.apply_projection_crm_selection` is a report-visible temporary adapter facade required while `CRMWriter` has no dedicated Repository owner. The CRM repository migration must move this fixed UoW behind that repository and delete the Store facade in the same batch; it is not a second implementation or fallback.
- Implemented foundation: `/api/collections/{collection_id}/authoritative-projection` resolves the active `collection_authoritative_pointer` to a projection id and fails closed when the pointer/projection is missing. Local asset entry must use this pointer rather than searching for "latest" projections by timestamp.
- Implemented foundation: `/api/projections/{projection_id}/export-policy` exposes field visibility/export groups. It is policy metadata only; export services still must re-check person assertion and CRM permissions at export time.

Public readers must not:

- mutate projection state
- repair missing projection state on request
- scan full baseline assets to answer a scoped run page
- compute counts from frontend-loaded candidate rows
- mix card-only filters with raw-profile-derived counts without exposing scope
- fall back to old overlay/job summary/stage file reconstruction

## Source Ownership Matrix

The repeated pressure-test failures came from treating several migration-era
artifacts as equally valid serving sources. Small jobs hide this because all
sources converge quickly; large/long-latency jobs expose the phase ordering.
The matrix below is the contract reviewers and tests must use before running a
nightly pressure suite.

| source | owner writer | authority | allowed normal readers | forbidden normal use | fallback rule |
| --- | --- | --- | --- | --- | --- |
| `serving_projections` | `ServingProjectionWriter` | projection metadata, readiness, counts, field visibility | projection APIs, collection asset entry, smoke/signoff | mutating state from public readers | missing/invalid projection fails closed |
| `serving_projection_members` | `ServingProjectionWriter` / bounded projection builders | exact visible membership revision; main sync, pagination, projection export/CRM source totals | `/api/projections/{projection_id}/candidates`, board-runtime adapter, projection export/CRM selection owners | card readiness gating membership; scanning job overlays or sidecars to replace membership | no fallback in normal reads; non-exact/mixed revision fails closed |
| `projection.membership_revision` | serving-projection member-publication UoW | opaque equality token sourced from the current member semantic-input revision | summary/page snapshot binding, cache invalidation, export/CRM input pinning, parity gates | sorting tokens; using timestamps, sequence, or counts as a substitute; merging unequal tokens | missing/mismatch fails closed and requires authoritative re-resolution/re-read |
| per-member profile/card readiness | profile asset owner and card materialization owner; projected by `ServingProjectionWriter` | independent revision-bound `profile_ready_count` and `card_ready_count` | profile/card status lines, detail availability, quality diagnostics | deriving card from profile, profile from card, or either from visible row count | unavailable until the owning facts are exact for the membership revision |
| explicit profile-capture evidence | profile ingest/capture owner; projected by `ServingProjectionWriter` | explicit capture fact/count only | capture audit and quality diagnostics | substituting profile-ready, card-ready, fetched, or display-ready counts | unavailable when owned evidence is absent; no derivation fallback |
| `projection_person_search_index` | `PersonAssetWriter` / recovery index builder | search/filter membership and exact public facet count product | projection search/filter/facet APIs | membership scan for active filters or initial facets, frontend-loaded-row filtering as global truth | explicit migration flag only, report `filter_contract.fallback_used=true`; normal initial state pending/unavailable |
| `board_runtime_state` | event-time projection/board publication writers | user-visible projection of exact `N/N` plus independent profile/card/capture readiness | `/progress`, `/dashboard`, `/candidates`, `/board-patches` as comparable fields | max-merging counts across unequal tokens; sorting revision tokens; using display-ready as sync/page/render gate | parity/revision drift blocks smoke/signoff |
| `job_result_lifecycle` | lifecycle/event-time writer | execution lifecycle and progress diagnostics | run/progress diagnostics, projection readiness comparison | becoming candidate membership source or overriding projection membership | diagnostic only after projection exists |
| `job_result_view` summary | workflow result writer | run summary and projection link metadata | run page, migration/backfill evidence | terminalizing baseline+delta from partial candidate counts | must not decide final board count |
| board patch log | board-visible writer | replay/audit of board-visible publication sequence | service metrics, replayability checks, debugging | serving final page membership independently from projection members | missing replayability is a signoff failure |
| legacy overlay / `asset_population_overlay_path` | migration-era materialization writer | compatibility artifact, audit, old-job migration input | offline migration, explicit repair, legacy unbackfilled jobs | normal projection-ready public serving, final count authority | report-visible migration only; delete after historical cutover |
| snapshot/candidate sidecars | snapshot/materialization builders | cold artifact, audit, compaction, import/export input | offline builders, migration, governance | request-time full-board serving or global facet computation | public readers fail closed and require repair |
| `latest_snapshot.json` | compatibility pointer repair tools | legacy pointer only | migration/repair diagnostics | authoritative snapshot selection in hosted/public reads | registry/pointer tables are authoritative |

Any new source that can influence user-visible counts, rows, filters, status
text, or exportable fields must be added to this table before it is consumed by
a public endpoint. If it is not in the matrix, it is not a normal public-reader
source.

If a projection is missing or unvalidated, the API response is fail-closed with a structured error such as:

```json
{
  "error": "projection_not_ready",
  "projection_id": "proj_...",
  "repair_required": true
}
```

## Count And Filter Contract

Frontend-visible counts must be explicit about scope.

Required count fields:

| field | meaning |
| --- | --- |
| `visible_member_count` (`result_count` / `candidate_count` / `total_candidates` compatibility views) | exact canonical visible membership `N` for one publication revision; owner of main sync, pagination, projection export/CRM source totals |
| `profile_fetch_required_count` | unique profile URLs/persons requiring LinkedIn profile detail |
| `profile_fetched_count` | required profiles fetched or available locally |
| `profile_ready_count` | visible members with independently owned exact profile readiness for the bound membership revision |
| `card_ready_count` (`card_materialized_count` / `display_ready_candidate_count` compatibility views) | visible members with independently owned exact board card detail, `C` in `C/N` |
| `explicit_profile_capture_candidate_count` | visible members with explicit owned profile-capture evidence; unavailable rather than inferred when that evidence is not exact |
| `needs_profile_completion_candidate_count` / `low_profile_richness_candidate_count` | optional exact member-quality classifications; published only when every visible member in the revision carries the corresponding owned `projection_metrics` fact |
| `row_shell_count` | candidates with lightweight row-shell visibility |
| `raw_source_row_count` | audit-only provider/source row count before candidate dedupe |

Required `count_scope` values:

| value | meaning |
| --- | --- |
| `exact_projection` | count covers the whole projection |
| `partial_served` | count covers only currently served rows |
| `index_partial` | raw/evidence index has not caught up to all projection candidates |
| `unavailable` | count should not be displayed as a global count |

Rules:

- `visible_member_count`, `result_count`, `candidate_count`, `/projections/{projection_id}/candidates.total_candidates`, and the board `expected_candidate_count` compatibility view must agree exactly for one canonical revision.
- `raw_source_row_count` may be larger than `result_count`, but it is audit metadata, not board population.
- Finalization must publish the serving projection/overlay member count as `result_count` / `candidate_count`. If legacy candidate-source or lifecycle evidence carries a larger raw expected count, preserve it only as diagnostics such as `raw_expected_candidate_count`; do not promote it to a frontend-visible board total.
- A current authoritative exact membership publication may change the visible population and all revision-bound aggregates upward, downward, or to zero. Readers obtain that publication from the projection/run-link owner and pin its opaque `membership_revision`; they do not select between unequal tokens by order, timestamp, or `max(old, new)`. Within one token, all aliases and page totals must remain equal.
- Card detail text is `C/N` from exact `card_ready_count`; main sync is `N/N`. `profile_ready_count` and `card_ready_count` are each bounded by `N`, but neither bounds the other. A card may be ready from non-profile materialized evidence, and a fetched/ready profile may still lack a card.
- Explicit profile capture must be counted only from explicit capture-owner evidence. It is not equal to `profile_ready_count`, `profile_fetched_count`, or `card_ready_count` by default.
- If membership is non-exact, unreadable, mixed-revision, or fallback-backed, its public count scope is `unavailable` and pagination/export/CRM source totals fail closed. If membership is exact but card/profile/capture facts are not, main `N/N` remains usable while only those independent readiness fields are unavailable.
- Facets shown as global must have `count_scope=exact_projection`.
- If a filter depends on raw profile/evidence fields and the index is partial, the UI must label it as partial or unavailable rather than showing it as a complete global count.
- The long-term card filter should be migrated to backend raw/evidence index semantics where possible. It should not remain limited to lossy card fields when raw profiles contain richer evidence.

### Public Field Owner Matrix

Projection/public-reader fields must not be re-derived independently per endpoint.

| field | owner/source of truth | allowed values / shape | normal consumers | forbidden derivation | preflight |
| --- | --- | --- | --- | --- | --- |
| `projection.membership_revision` | serving-projection member-publication UoW from the existing member semantic-input revision | non-empty opaque equality token; equality/inequality only | summary/page binding, board parity, cache invalidation, export/CRM input pin | ordering tokens; using `updated_at`, publication sequence, or count as a revision | same-token happy path, mismatch fail-closed, same-timestamp distinct-token fixture |
| `visible_member_count` and public total aliases | `ServingProjectionWriter` from exact visible `serving_projection_members` at one publication revision | non-negative integer with `count_scope=exact_projection`; exact zero is valid | main sync, pagination, projection export and projection-to-CRM source selection | display/card/profile counts, lifecycle expected, overlays, patch max, loaded rows | exact up/down/zero revision fixtures plus cross-endpoint total parity |
| `card_ready_count` / `display_ready_candidate_count` | card materialization owner facts projected for the same membership revision | non-negative `C <= N`, or unavailable when facts/revision are not exact | `C/N` card detail line, detail availability, quality diagnostics | profile-ready/fetched counts, visible membership, patch or overlay max | exact card text/count parity including `C=0`, `C<N`, and revision correction |
| `profile_ready_count` | profile asset/readiness owner facts projected for the same membership revision | non-negative `P <= N`, or unavailable | profile readiness/status and detail diagnostics | card-ready, display-ready, row membership | fixtures with `P<C`, `P>C`, and independent revision changes |
| `explicit_profile_capture_candidate_count` | profile ingest/capture owner from explicit per-member capture evidence | independently evidenced count/scope, or unavailable | capture audit/quality diagnostics | profile-ready, profile-fetched, card-ready, display-ready, visible membership | poison fixture where profile/card readiness exists without capture evidence |
| `filter_contract.facet_count_scope` | projection reader from canonical projection facet counts / `projection_person_search_index` readiness | `exact_projection`, `index_partial`, `unavailable`; legacy migration may report fallback explicitly | `/api/projections/*`, job projection-backed `/candidates`, board-runtime parity smoke | deriving from `facet_summary_scope`, dashboard local rows, overlay sidecars, frontend cache | projection reader tests plus cross-endpoint `board_runtime_state.filter_contract` parity |
| `asset_population.facet_summary_scope` / top-level `facet_summary_scope` | projection summary reader from revision-fenced `projection_person_search_index` public facet product | `exact_projection`, `global_full_population`, `current_served_partial`, `raw_profile_partial`, `unavailable`; initial membership publication is pending/unavailable | frontend facet visibility and count display | deriving from membership rows, `filter_contract.facet_count_scope`, dashboard local rows, overlay sidecars, frontend cache | initial-membership unavailable fixture plus index-finalization and cross-endpoint parity smoke |
| `filtered_candidate_count` | projection/candidate row reader after backend filtering | integer over complete served/projection population for the active filter | candidate board header, pagination, export preview | frontend-loaded offset window count | candidate-page backend filtering tests |
| `media_summary` | projection reader from canonical `PersonAsset.avatar_media` through `media.asset.cache` / `media_asset_owner` | `avatar_status=available` with stable `avatar_asset_id`/`avatar_url`, or `avatar_unavailable`; `fallback_used=false` in normal path | projection person detail, person summary, projection candidate page rows, candidate avatar display | provider `avatar_url` / `photo_url` hotlinks, raw profile media, frontend URL guessing | person asset projection contract tests plus frontend source contract preflight |
| `read_contract.fallback_used` | public reader adapter | boolean plus explicit fallback reason when true | smoke/signoff, frontend diagnostics | silently reading overlay/job summary/stage files | strict signoff blocks normal fallback |
| `field_visibility` | projection/export policy + permission layer | visibility groups such as `public_summary`, `crm_overlay_summary`, restricted contact/evidence groups by dedicated APIs only | frontend display/export policy | leaking raw/evidence/contact fields because projection membership exists | export policy and API schema tests |

`facet_summary_scope` and `filter_contract.facet_count_scope` are intentionally not synonyms:

- `facet_summary_scope` describes coverage of the facet summary object that a board may display.
- `filter_contract.facet_count_scope` describes the count source/readiness for filter counts in the row API.
- They may both indicate a complete projection/global state, but one must not be computed from the other unless a future contract explicitly says so.
- Local asset projection boards treat `facet_summary_scope=exact_projection` as a complete canonical facet summary. Frontend consumers must not wait for the older job-board `global_full_population` scope before enabling projection-scoped search and filters.
- Exact membership does not make either facet field exact. The initial membership publication exposes pending/unavailable facets until `projection_person_search_index` finalizes against the same semantic input revision.

## Local Asset Consumption Entry

The local asset entry is collection-first, not run-first.

Default flow:

1. User chooses a company collection.
2. Frontend resolves `collection_authoritative_pointer`.
3. Frontend opens `/projections/{active_projection_id}`.
4. User can search/filter existing assets and inspect coverage/freshness.
5. The collection asset view does not launch acquisition and does not render a direct "new acquisition" CTA in v1. Future Agent actions or the separate new-job surface may explain and prefill acquisition intent, but the asset-consumption page remains read-first.

API shape:

- `GET /api/collections` returns the company asset overview list from `collection_authoritative_pointers + serving_projections`, with registry coverage as read-only summary metadata. It does not compose candidate rows from legacy jobs or runtime artifacts.
- `GET /api/collections/{collection_id}/asset-entry` returns the active authoritative projection, exact readiness counts, index watermarks, and `acquisition_handoff.available=false`.
- `GET /api/collections/{collection_id}/coverage` returns coverage/shard/readiness metadata for the active authoritative projection. It may include `organization_asset_registry` summary fields, but result serving remains owned by `serving_projection_members`.
- Missing local company assets may be published only by an explicit maintenance owner such as `ServingProjectionMigrationBackfill.backfill_collection_authoritative_local_asset_snapshots(...)`. That path reads `runtime/company_assets`, writes `collection_authoritative_projection` plus `collection_authoritative_pointer`, and records `normal_reader_repair=false`. Collection overview/detail/public readers must never scan runtime directories or synthesize pointers on request.
- The local asset maintenance owner must select a publishable snapshot from canonical artifact evidence, not from `latest_snapshot.json` or lexicographic latest directory alone. Snapshot selection must verify that manifest shard paths exist and that the loaded source payload actually contains projectable profile/detail fields before using profile counts. Legacy shard payloads without `projection_version` are acceptable only when `candidate_id`, `fingerprint`, `materialized_candidate`, `normalized_candidate`, and `reusable_document` are present; otherwise the owner must fail closed or choose another snapshot. `candidate_documents.json` fallback is migration-era low-confidence input and must not satisfy a profile/detail repair claim.
- Company logo/media is an explicit read contract on collection overview and asset-entry payloads. Responses read `CompanyAsset(asset_type='logo_media')` through `CompanyAssetWriter`-owned PG state. If stable logo media exists, responses return `company_media.logo_status=available`, `logo_url`, `logo_asset_id`, and `media_contract.fallback_used=false`. If it does not exist, responses must return `logo_status=logo_unavailable`, empty `logo_url` / `logo_asset_id`, and `media_contract.fallback_used=true` with `fallback_source=placeholder_initials`. Frontend may render that placeholder, but it must not treat initials as a real logo asset.
- `/collections` is the frontend overview route. It should be a product-facing company selector and must not expose internal read-contract terms such as canonical projection, collection authoritative projection, raw/evidence index internals, or acquisition workflow ownership as primary content.
- `/collections/{collection_id}` is the single-company asset home. It may show concise asset readiness, version, and search-index status, then link to `/projections/{projection_id}?collection={collection_id}` and `/targets?collection={collection_id}`. It should not duplicate the overview card or imply that asset browsing starts acquisition work.
- `/projections/{projection_id}` remains the candidate-board route. When opened from a local asset, it should keep local-asset tabs visible and treat missing facet/index build products as an explicit disabled state, not a broken filter UI.

Coverage display should reuse the existing candidate-board/product semantics where possible:

- shard keyword/scope
- employment status
- provider source
- profile completeness
- profile fetched/indexed timestamps in China time
- manual exclude/status summary

## Run Completion And Collection Merge

Run completion behavior:

1. Create or finalize `run_scope_projection`.
2. Mark run execution complete when workflow contracts are satisfied.
3. Enqueue collection merge by default.
4. Let `CollectionWriter` build a new authoritative projection version in the background.
5. Atomically switch `collection_authoritative_pointer` when the merge is validated.

The run result page does not wait for the authoritative merge.

If collection merge fails, the run projection remains valid. The collection pointer stays on the previous authoritative projection and exposes merge failure diagnostics to operators.

## Migration Plan

Migration should be staged, but not dual-track in production behavior.

### Phase 1: Schema And Writers

- Add projection storage.
- Add run/projection link storage.
- Add collection authoritative pointer storage.
- Add writer APIs for run scope and collection authoritative projections.

### Phase 2: Offline Backfill

- Backfill completed historical jobs into `run_scope_projection`.
- Backfill collection authoritative projections from validated company assets.
- Write run/projection links.
- Produce a migration report: migrated, skipped, missing evidence, repair required.

### Phase 3: Route And API Cutover

- Frontend result links switch to `/projections/{projection_id}`.
- Run pages switch to `/runs/{run_id}` and link to projection.
- Projection APIs become the only normal candidate-board source.

### Phase 4: Legacy Endpoint Retirement

- Legacy job-result endpoints return redirect, migration notice, or `410`.
- Remove public-read composition from legacy job summary/result-view/stage files.
- Add smoke/signoff gates proving legacy fallbacks are absent in normal scripted cases.
- V1 cutover retires legacy job-result endpoints by default. Legacy `/api/jobs/{job_id}/results`, `/dashboard`, `/candidates`, and `/candidates/{candidate_id}` return `410` instead of composing normal public reads. If the run has a ready `run_scope_projection` link, the `410` payload includes the projection pointer. If the run is not migrated, the `410` payload is `migration_required` with no fallback payload. `SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS` can force global on/off for controlled tests, and `SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS=1` is migration/test-only override. Runs without projection evidence must be backfilled or rerun; they are not served by legacy composition in normal mode.

### Phase 5: Compatibility Code Deletion

- Delete temporary migration-only readers once backfill and ECS cutover are complete.
- Keep operator repair tools explicit and fail-closed.

## Test And Signoff Requirements

Implementation is not complete until tests prove:

- a completed run always has a linked `run_scope_projection`
- result APIs cannot serve without a valid projection
- `/runs/{run_id}` and `/projections/{projection_id}` have separated responsibilities
- OpenAI baseline+delta serves run delta by default and does not silently serve the full baseline
- Lovable live-roster publishes deduped board rows, not raw source rows
- Google large-baseline/large-shard can serve the run projection without scanning or materializing the full baseline on the hot path
- local asset entry resolves through `collection_authoritative_pointer`
- target-candidate add/export can source from a projection without depending on job lifetime
- projection row APIs enforce field visibility and do not expose raw profile, restricted evidence, CRM notes, or contact assertions without the correct API/policy
- projection CRM overlay reads are read-only and do not auto-create CRM records
- projection CRM/export mutations require the displayed membership revision; stale/missing tokens produce zero writes
- projection-bound dispatch excludes concurrent membership publication and operation cancel through the documented lock order; real `POOL_MAX=1` publication/cancel waiters complete without pool/advisory-lock deadlock, duplicate commands, or duplicate events
- projection-to-CRM batch selection rechecks revision/`N`/members under the publication lock and atomically commits record, engagement, and event rows; injected table failure and duplicate-person selection roll back the whole batch
- exact selection replay writes nothing, while a new revision for an existing person records one `projection_member_reselected` event and preserves one engagement
- target-candidate export consumes one complete `last_source_selection` tuple per row and rejects mixed projection/revision provenance
- raw/evidence filter counts expose `count_scope`
- exact visible membership drives the same `N/N` through main sync, pagination, projection export, and projection-to-CRM source totals even when `card_ready_count` is below `N` or zero
- profile readiness and card readiness are independently testable in both directions; no test or reader imposes `card_ready <= profile_ready`
- explicit profile capture and optional member-quality classifications are aggregated only from per-member owned `projection_metrics` on the same revision; a legacy summary or any member missing the fact makes that aggregate unavailable rather than an inferred zero
- a newly authoritative exact membership publication can correct counts upward, downward, and to exact zero, while non-exact/token-mismatched/fallback reads fail closed
- summary/page/readiness/export/CRM inputs bind to one opaque `projection.membership_revision`; the reader compares that token before and after member-count/readiness aggregation so a concurrent publication cannot pair new counts with an old token (or the reverse). Missing/unequal/changing tokens fail closed, inequality invalidates caches, token ordering is never attempted, and same-timestamp distinct publications remain distinguishable
- initial membership publication leaves public facets pending/unavailable until `projection_person_search_index` publishes a revision-matched exact facet product
- legacy job-result endpoints do not participate in normal serving after cutover
- Pre-Manual Scripted Signoff blocks missing projection reports, fallback usage, cross-endpoint drift, and candidate page timeouts

Before a nightly long-latency matrix is run as pressure validation, the
non-pressure contract gates must already be green:

- `scripts/check_service_gate_coverage.py --json` must report no missing tags,
  unknown expectations, or unknown coverage tags.
- All matrix cases that exercise public reads must declare
  `require_projection_cutover_report=true`,
  `require_board_visible_projection_report=true`,
  `require_board_runtime_state_cross_endpoint_parity=true`, and
  `require_no_progress_contract_violation=true`.
- `review_scripted_smoke_run.py` must have projection cutover evidence for the
  same cases; missing projection-cutover reports are blocking, not warnings.
- Legacy readers, overlays, sidecars, and compatibility pointers may appear only
  as migration evidence with explicit metrics. A normal scripted case hitting
  them is a product contract failure, not a nightly performance finding.
- Poison-fixture/unit gates must cover partial source precedence before
  nightly: stale/partial overlay, partial `result_view`, partial lifecycle, and
  partial projection visible counts must not lower a canonical projection or
  terminalize a baseline+delta run.

The preceding partial-source guard does not make exact membership monotonic. A current canonical exact publication may
lower or clear the population. Its opaque token does not say "newer" by itself; authority comes from the pinned projection
resolution, and only stale, partial, fallback-backed, or token-mismatched evidence is forbidden from replacing that snapshot.

Required scripted coverage should include:

- OpenAI baseline+delta
- OpenAI no-baseline scoped search
- Lovable live roster
- Google large baseline + large shard
- Google large baseline + small former shard
- at least one local asset consumption read-only case
- at least one target-candidate source-from-projection case

## Future Agent Interaction Track

This contract deliberately keeps deterministic serving separate from future Agent UX. The projection model is the foundation that makes richer Agent interactions safe.

Future Agent work should be specified in a separate contract after projection decoupling starts. It should cover:

- a persistent frontend Agent entry
- natural-language operations over local assets, projections, and target candidates
- an operation registry with deterministic permissions, cost budgets, provider budgets, and audit trails
- graph/node workflow execution for long-running acquisition and enrichment
- staged acquisition: discover candidate list first, fetch a sample of 50/100 profiles, let the user decide whether to continue
- multi-turn intent recognition and continuation of an existing projection/run context
- external intake paths beyond Excel
- update/insert/delete-style candidate operations implemented as auditable overlays

The Agent layer may use LangGraph, OpenClaw-style orchestration, or another explicit state-machine framework if it improves long-running interaction clarity. It must not weaken the storage/queue/projection contracts: provider calls, registry ownership, projection writes, and CRM mutations remain deterministic and auditable.

## Anti-Patterns

Do not reintroduce:

- result pages keyed by `job_id` as the primary serving identity
- public readers that rebuild state from job summary, lifecycle mirrors, overlays, and stage files
- frontend counts computed from loaded page rows
- full baseline merge as the default for every scoped run
- saved user filters as canonical projections
- raw profile JSON scans in request hot paths
- physical deletion of local assets for ordinary manual exclude/edit
- hidden fallback ladders that pass scripted signoff without structured fallback metrics
