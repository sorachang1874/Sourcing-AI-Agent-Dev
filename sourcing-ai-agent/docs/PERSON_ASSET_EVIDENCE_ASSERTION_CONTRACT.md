# Person Asset, Evidence, And Assertion Contract

> Status: Proposed product/data contract. Drafted 2026-05-19 to support canonical projections, CRM, raw-profile search, Public Web enrichment, avatar/media handling, and future Agent operations. Read with `CANONICAL_SERVING_PROJECTION_CONTRACT.md`, `CRM_STATE_CONTRACT.md`, `DATA_ASSET_GOVERNANCE.md`, and `FRONTEND_API_CONTRACT.md` before changing person identity, profile registry reuse, public web signals, avatar serving, or contact promotion.

## Purpose

The product needs a reusable person layer that is broader than candidate cards and narrower than CRM.

The durable model is:

```text
PersonIdentity says who this is.
PersonAsset stores reusable raw or media assets.
PersonEvidence stores observed evidence and model-safe extracts.
PersonAssertion stores selected facts with provenance and authority.
Projection serves candidate membership.
CRM manages business follow-up state.
```

This layer prevents the same person from being fetched, searched, enriched, promoted, or exported through multiple incompatible paths.

## Non-Negotiable Rules

1. V1 identity is LinkedIn-first: `person_identity_key = linkedin:{profile_url_key}` when a normalized LinkedIn profile URL exists.
2. Raw provider payloads and large evidence documents are assets/evidence, not projection rows and not CRM events.
3. User-confirmed facts become `PersonAssertion` records. They do not overwrite raw assets.
4. `PersonAssertion` must reference evidence, CRM event, import row, or manual source provenance unless explicitly marked as operator-entered.
5. Avatar URLs from provider profiles must be treated as unstable external evidence. Stable display should use a media asset/cache reference when available.
6. Raw-profile and evidence-backed filters must query backend indexes over this layer. Frontend candidate cards must not scan raw profile JSON.
7. Person asset writes must be idempotent by identity, asset type, content hash/source key, and freshness policy.
8. Permission/visibility is field-level. Raw evidence, contact assertions, CRM notes, and exportable fields can have different visibility scopes.

## Core Objects

### `PersonIdentity`

Required logical fields:

| field | meaning |
| --- | --- |
| `person_identity_key` | stable person id |
| `primary_profile_url_key` | normalized LinkedIn key when present |
| `canonical_profile_url` | canonical public LinkedIn URL |
| `raw_profile_urls_json` | observed provider URLs for audit |
| `alias_keys_json` | pending or confirmed aliases |
| `merge_status` | `canonical`, `pending_review`, `conflict`, `merged` |
| `created_at`, `updated_at` | timestamps |

Rules:

- Same `profile_url_key` means same person unless an explicit conflict record says otherwise.
- Non-LinkedIn identities may enter a pending alias table, but cannot silently merge with a LinkedIn identity.

### `PersonAsset`

Reusable raw/media/object assets.

Examples:

- LinkedIn raw profile payload.
- Stable avatar media asset.
- Public homepage snapshot.
- X/GitHub/profile page snapshot.
- DataForSEO result manifest.

Required logical fields:

| field | meaning |
| --- | --- |
| `asset_id` | stable random id |
| `person_identity_key` | owner person |
| `asset_type` | `linkedin_raw_profile`, `avatar_media`, `homepage_snapshot`, `public_web_document`, etc. |
| `source_kind` | `harvest`, `dataforseo`, `manual`, `import`, `agent`, etc. |
| `source_run_id` | optional acquisition/enrichment run |
| `source_projection_id` | optional projection provenance |
| `content_ref` | object-store/file/database reference |
| `content_hash` | hash for idempotency |
| `source_url` | source URL when applicable |
| `fetched_at` | provider/source timestamp |
| `visibility_scope` | `public_summary`, `internal`, `restricted_contact`, `raw_private` |
| `status` | `available`, `superseded`, `failed`, `quarantined` |
| `metadata_json` | bounded metadata |

### `PersonEvidence`

Observed evidence derived from assets or provider/search results.

Examples:

- Public Web signal: likely email, homepage, X profile, GitHub profile.
- Search snippet matching a person.
- Model-safe analysis over fetched public documents.
- Manual review source link.

Required logical fields:

| field | meaning |
| --- | --- |
| `evidence_id` | stable random id |
| `person_identity_key` | nullable until identity is resolved |
| `asset_id` | source asset, nullable |
| `evidence_type` | `email_candidate`, `profile_link`, `employment_claim`, `education_claim`, etc. |
| `value` | compact value |
| `normalized_value` | normalized value for dedupe |
| `source_url` | public source URL when applicable |
| `source_domain` | source domain |
| `confidence_score` | numeric confidence |
| `identity_match_score` | numeric identity match |
| `publishable` | safe for user/export after policy checks |
| `evidence_excerpt` | bounded model-safe excerpt |
| `artifact_refs_json` | references, not raw payloads |
| `status` | `observed`, `suppressed`, `promoted`, `rejected` |

### `PersonAssertion`

Selected fact with provenance and authority.

Examples:

- Primary email.
- Personal homepage.
- X URL.
- GitHub URL.
- Current company.
- Preferred avatar.

Required logical fields:

| field | meaning |
| --- | --- |
| `assertion_id` | stable random id |
| `person_identity_key` | owner person |
| `assertion_type` | `primary_email`, `homepage_url`, `x_url`, `github_url`, `current_company`, `preferred_avatar` |
| `value` | asserted value |
| `normalized_value` | normalized value |
| `authority` | `operator_confirmed`, `agent_suggested`, `provider_observed`, `imported`, `legacy_migrated` |
| `verification_status` | `active`, `needs_review`, `rejected`, `superseded` |
| `source_evidence_id` | evidence source when applicable |
| `source_crm_event_id` | CRM event provenance when applicable |
| `source_run_id` | optional source run |
| `confidence_score` | numeric confidence |
| `valid_from`, `valid_to` | optional temporal validity |
| `created_at`, `updated_at` | timestamps |

Rules:

- Only `operator_confirmed` or explicitly approved `agent_suggested` assertions can become selected contact/export facts by default.
- A newer assertion supersedes older assertions through status and event provenance, not destructive overwrite.
- Export services must include assertion provenance and skip reasons when a requested field is missing or unverified.

## Avatar Media Asset

Provider avatar URLs are often short-lived or blocked by hotlinking. Stable display should use a media asset layer.

V1 behavior:

1. Store provider avatar URL as evidence/metadata.
2. If policy allows, fetch or import avatar asynchronously through `workflow_commands(command_type='media.asset.cache', owner='media_asset_owner')`.
3. Store a normalized image object under object storage or media cache.
4. Reference it through `PersonAsset(asset_type='avatar_media')`.
5. Public rows expose `avatar_asset_id` or a signed/static media URL, not provider hotlink as the only stable source.

The media owner must dedupe by content hash and stable entity key, apply storage limits/retention, and record ActivityRun/Attempt/EntityDelta evidence. Direct provider `avatar_url` hotlinks remain metadata until this owner writes the stable `PersonAsset`.

## Index Contract

Raw/evidence search uses backend indexes:

- `RawProfileIndex` over LinkedIn raw profile text and structured profile fields.
- `CandidateEvidenceIndex` over evidence values, domains, excerpts, and assertion status.
- `PersonSummaryView` for lightweight projection rows.

Index state exposed to public APIs:

| field | meaning |
| --- | --- |
| `raw_profile_index_watermark` | latest indexed raw profile boundary |
| `evidence_index_watermark` | latest indexed evidence boundary |
| `count_scope` | `exact_projection`, `index_partial`, `partial_served`, `unavailable` |
| `profile_fetched_at` | China-time display timestamp |
| `profile_indexed_at` | China-time display timestamp |

If indexes lag, APIs must expose partial/unavailable count scope instead of pretending card-only counts are global truth.

## Writer Ownership

| writer | owns |
| --- | --- |
| `PersonIdentityWriter` | identity/alias/conflict rows |
| `PersonAssetWriter` | raw/media/object asset rows |
| `PersonEvidenceWriter` | observed evidence rows |
| `PersonAssertionWriter` | selected assertions and supersession |
| `RawEvidenceIndexer` | raw/evidence index watermarks |
| `ProjectionWriter` | membership/readiness only |
| `CRMWriter` | CRM state and links to assertions |

Public readers, frontend clients, workflow recovery, and Agent nodes must not write these tables directly.

Implementation foundation added 2026-05-19:

- `src/sourcing_agent/person_identity.py` owns v1 LinkedIn/profile-key based `person_identity_key` and compact `PersonSummaryView` helpers.
- `serving_projection_members` writer path now derives `person_identity_key`, `profile_url_key`, and public summary fields through the shared helper instead of each reader inferring identity.
- `person_assets`, `person_evidence`, and `person_assertions` exist as control-plane tables with `PersonAssetWriter` facade methods.
- Projection/person detail APIs now expose public-safe assertion/asset/evidence summaries without returning raw profile payloads or unreviewed restricted contact values by default.
- Public Web promotion migration and live promotion now write `PersonAssertion(authority='operator_confirmed', verification_status='active')` plus CRM assertion-link events, so promoted homepage/X/GitHub/Substack/email values do not remain only in legacy target-candidate fields.
- CRM Public Web signal materialization now also writes canonical `PersonAsset(asset_type='public_web_signal')` and `PersonEvidence` rows while preserving existing `person_public_web_assets` / `person_public_web_signals` as compatibility evidence. The `crm.public_web.signals.materialize` command is the owner for this sync and records `person_asset` / `person_evidence` EntityDeltas with `person_asset_sync_contract=crm_public_web_signal_person_asset_sync_v1`. New readers and index builders should prefer the canonical `PersonAsset` / `PersonEvidence` layer; the legacy person-public-web tables remain migration/source-specific storage until historical backfill and reader cutover finish. The materializer is also the boundary between raw/audit evidence and user-visible signals: raw search links, fetched documents, and model input snapshots stay in artifacts, while `person_public_web_signals` only receives non-suppressed email candidates, confirmed/likely publishable profile-shaped links, or AI-explicit reviewable links where `link_assessments.user_visible_signal=true` and `review_queue_reason` explains why the link belongs in the human review queue. `model_no_assessment`, `not_same_person`, non-profile publication/post/article/video links, suppressed email rows, and AI-rejected low-value review leads are not normal user-visible signals.
- Historical Public Web signal repair is explicit operator/migration work through `POST /api/persons/backfill-public-web-signals`, which defaults to `dry_run=true` and reports `read_contract.normal_reader_repair=false`. The endpoint may backfill `person_public_web_signals` into `PersonAsset(asset_type='public_web_signal')` and `PersonEvidence`, but projection/person readers must not call it or repair missing assets in request paths.
- `projection_person_search_index` now provides projection-scoped keyword search and structured filter support. `PersonAssetWriter.rebuild_projection_person_search_index(...)` builds rows from public summary, person assets, evidence, and assertions in pages, persists a public `filter_record`, and projection APIs expose index readiness/watermarks without leaking raw payloads.
- Projection publication and Public Web assertion promotion enqueue durable `projection_person_search_index_build` work, and `/api/projections/backfill-person-search-indexes` provides the production-scale repair path for historical projections.
- `raw_profile_index` and `candidate_evidence_index` are now person-level control-plane indexes. `PersonAssetWriter.rebuild_person_indexes_for_projection(...)` rebuilds them from projection membership in pages, and projection index rebuild consumes these canonical person indexes instead of scanning person assets/evidence directly. `/api/persons/backfill-raw-evidence-indexes` is the production-scale repair/backfill entrypoint.
- Projection person detail, person summary, and projection candidate page read APIs now prefer `PersonAsset(asset_type='avatar_media', visibility_scope='public_summary', status='available')` and expose a `media_summary` contract with `avatar_asset_id` plus stable media URL when such an asset exists. When no asset exists they return `avatar_unavailable`; they do not convert provider hotlinks into stable media.
- `media.asset.cache` is the bounded media owner for stable avatar and logo media. It accepts inline media payloads or bounded URL/file fetch inputs, uploads normalized bytes to object storage, writes `PersonAsset(asset_type='avatar_media')` or `CompanyAsset(asset_type='logo_media')`, and records ActivityRun/Attempt/EntityDelta evidence. Existing LinkedIn `avatar_url` values are provider-observed metadata only; they are not a stable display contract and must not be used as proof that avatar serving is implemented.
- `/api/media/assets/{asset_id}` is the fail-closed read API for object-backed cached media. It serves only available public-summary `avatar_media` / `logo_media` canonical assets and never fetches provider URLs or repairs missing media in the request path. Frontend candidate cards prefer `media_summary.avatar_url` only when `avatar_status=available`, `media_contract.source=PersonAsset.avatar_media`, and `fallback_used=false`; old provider avatar fields remain metadata compatibility only. The remaining media follow-up is historical avatar/logo backfill.
- Historical provider avatar repair is explicit operator/migration work through `POST /api/media/backfill-person-avatars`. It defaults to `dry_run=true`, reads provider avatar metadata from `serving_projection_members`, and only plans `workflow_commands(command_type='media.asset.cache', owner='media_asset_owner')`; it does not fetch images in the request path unless an operator explicitly passes `run_now=true`.
- The HTTP route contract for historical Public Web signal, company Public Web fact, avatar, and company logo backfills is part of the fast pre-Agent gate: `tests/test_projection_crm_api_contracts.py::test_asset_backfill_http_routes_are_explicit_migration_paths` verifies dry-run defaults, `normal_reader_repair=false`, no request-path media fetch, explicit source-specific fact sync only on operator apply, and command-planning-only behavior for media repairs. W6/nightly must not be the first place these migration route boundaries drift. Operational runs should use `scripts/backfill_person_company_asset_media.py`, which enforces PG-only control-plane settings and requires `--reviewed` with `--apply`.

## Company Asset Follow-Up

The current `company_public_web_asset_runs` / `company_public_web_assets` lane is a useful company-level Public Web read model, but it is not the company fact owner. The canonical company fact/media foundation is PG-only `CompanyAsset` / `CompanyEvidence` / `CompanyAssertion`, written through `CompanyAssetWriter`. Company Public Web refresh now projects model-safe rows into `CompanyAsset` / `CompanyEvidence` when PG-only canonical storage is available; SQLite compatibility callers get a report-visible `postgres_required_for_company_asset_layer` skip and must not create canonical company facts. Agent/Operation-triggered refresh uses `company.public_web.refresh` owned by `company_public_web_owner`, and must record ActivityRun/Attempt/EntityDelta evidence before company facts are treated as refreshed.

Planned durable model:

```text
CompanyIdentity says which organization this is.
CompanyAsset stores reusable raw/media/object assets such as logo media, official site snapshots, research pages, engineering blogs, RSS/arXiv/OpenReview/crawl bundles, and public-web summaries.
CompanyEvidence stores observed company-level source evidence and model-safe extracts.
CompanyAssertion stores selected company facts such as official homepage, preferred logo, research blog URL, engineering blog URL, or official publication feed.
Collection authoritative projection remains the people-roster serving pointer.
```

Rules:

- Company asset overview pages read `CompanyAsset(asset_type='logo_media')` for stable logo media. They may show initials as a visual placeholder only when no logo media asset exists. Initials must not be treated as a company logo contract.
- Company logo media is written by the same `media.asset.cache` owner as avatar media. Company Public Web refresh may discover official URLs/facts, but it must not treat a discovered logo URL as a stable logo until the media owner caches it as `CompanyAsset(asset_type='logo_media')`.
- Fresh Harvest/raw profile work-experience `companyLogo` is the preferred first-acquisition logo evidence source only when no stable `CompanyAsset(asset_type='logo_media')` exists. Local profile apply may only plan `workflow_commands(command_type='company.logo.profile_experience.discover', owner='company_asset_owner')` as a non-blocking parallel command; it must not scan profile JSON, fetch media, or write logo evidence inline. The company asset owner reads at most one profile payload, records an unexpired logo URL as `CompanyEvidence(evidence_type='logo_url', source_kind='profile_experience_company_logo')`, and then plans `workflow_commands(command_type='media.asset.cache', owner='media_asset_owner')`; it must not fetch media or write `CompanyAsset.logo_media` directly. Expired or near-expiry profile logo URLs must fail closed as `source_discovery_required` and fall back to explicit logo source discovery. Historical LinkedIn CDN hotlinks are not stable backfill inputs.
- Historical company logo repair is explicit operator/migration work through `POST /api/media/backfill-company-logos`. It defaults to `dry_run=true`, accepts explicit logo URLs and existing logo evidence/hotlink assets, and only plans `workflow_commands(command_type='media.asset.cache', owner='media_asset_owner')`; it does not fetch images in the request path. Homepage favicon derivation is opt-in (`include_homepage_favicon=true`) and report-visible because it is heuristic, not a reader fallback.
- Historical company Public Web fact repair is explicit operator/migration work through `POST /api/company-assets/backfill-public-web-assets`. It defaults to `dry_run=true`, reads source-specific `company_public_web_assets`, and syncs them into `CompanyAsset` / `CompanyEvidence` only on explicit apply. Local asset overview and company tabs must not trigger this endpoint as fallback repair.
- Company Public Web fact and logo backfills follow the same HTTP preflight as person avatar and Public Web signal repair; route handlers must expose the same migration envelope as the orchestrator and must not become local asset overview fallbacks.
- Research / Engineering / Blog / Publications tabs should read from company-level Public Web assets and evidence, not from candidate-level PersonAsset rows.
- Company Public Web refresh remains explicit/default-off. It must not become a hidden acquisition stage or a fallback inside local asset overview.
- Company-level public web assets use the durable Command -> ActivityRun -> ActivityAttempt -> EntityDelta causality spine for Agent-triggered refresh. The source-specific company Public Web rows remain provenance/read models; command/activity/entity-delta rows are execution proof.
- `CompanyAssetWriter` is the normal writer for canonical company assets/evidence/assertions. These tables are PG-only and must not gain SQLite normal-path DDL.
- `company_public_web_assets` is source-specific provenance/read-model storage. Normal product readers that need reusable company facts should prefer `CompanyAsset` / `CompanyEvidence` after the refresh sync, not re-derive facts from the source read model.
- Canonical company fact readers are `/api/company-assets`, `/api/company-assets/evidence`, and `/api/company-assets/assertions`. They fail closed when PG-only canonical storage is unavailable and must not scan source-specific Public Web rows as fallback.

## Migration Notes

- `linkedin_profile_registry` remains the scheduler/source of URL fetch state. Person identity should reuse its normalized URL key rather than invent a second dedupe key.
- `person_public_web_assets` and `person_public_web_signals` are being migrated into `PersonAsset` and `PersonEvidence` shapes. Normal CRM Public Web signal materialization writes canonical `public_web_signal` assets/evidence now. Historical repair must use the explicit dry-run/apply backfill endpoint (`/api/persons/backfill-public-web-signals`); remaining work is running that migration where needed plus reader/index preference cleanup.
- `target_candidate_public_web_promotions` should migrate into `PersonAssertion` plus `CRMEvent` links.
- Existing candidate card avatar URLs should be treated as provider-observed metadata until a media asset exists.

## Tests And Gates

Implementation is incomplete until tests prove:

- Same LinkedIn profile URL resolves to one `person_identity_key`.
- Provider profile cache hits do not refetch profiles when a person is added to CRM.
- Public Web promotion creates a `PersonAssertion` with evidence provenance.
- Avatar serving works when the original provider avatar URL is unavailable.
- Projection/public reader rows can render from `PersonSummaryView` without loading raw profile JSON.
- Raw/evidence search returns count scopes and index watermarks.
- Contact exports include assertion provenance and skip reasons.
