# CRM State Contract

> Status: Proposed product/data contract. Drafted 2026-05-19 to guide the post-ECS CRM and Agent-facing architecture work. Read with `CANONICAL_SERVING_PROJECTION_CONTRACT.md`, `DATA_ASSET_GOVERNANCE.md`, `FRONTEND_API_CONTRACT.md`, and `NEXT_TODO.md` before changing target-candidate flows, Public Web promotions, manual review resolution, exports, person assets, or future Agent operations.

## Purpose

CRM is a standalone product module for managing people after they have been discovered, reviewed, selected, contacted, enriched, exported, or excluded.

It is not the workflow runner, not a projection, and not the Agent runtime.

The durable model is:

```text
Projection serves candidate sets.
Person assets/evidence/assertions describe people.
CRM records business state about selected people.
Agent creates typed actions that call the owning module.
```

The CRM module must support direct frontend use and future Agent use through the same writer APIs and event log. It must not depend on a job/run being alive.

## Current Local State Review

The current codebase already has useful pieces, but they are not yet a clean CRM module:

- `target_candidates` stores `record_id`, `candidate_id`, `history_id`, `job_id`, display fields, `primary_email`, `follow_up_status`, `quality_score`, and `comment`.
- `TargetCandidatesPanel.tsx` exposes a target-candidate page with follow-up status, quality score, comments, Public Web search, Public Web detail, promotion, and export actions.
- `person_public_web_assets` and `person_public_web_signals` already move part of Public Web enrichment toward person-level reusable assets.
- `target_candidate_public_web_promotions` records manual promote/reject decisions for Public Web signals.
- `manual_review_items` and `candidate_review_registry` are still job/candidate-review oriented and are not the same thing as CRM state.

Main gaps this contract closes:

- Target candidates are still job/history/candidate-id centered instead of person/projection centered.
- Mutable target-candidate fields do not have a complete append-only state-change log.
- Public Web promotion can behave like a target-candidate field update rather than a person assertion plus CRM event.
- CRM state, manual review state, person evidence, projection membership, and workflow execution are still easy to mix.
- Future Agent operations need a typed module boundary so the Agent does not become a second orchestrator.

## External Reference Patterns

These systems are references, not dependencies:

- [Twenty](https://docs.twenty.com/getting-started/core-concepts/data-model) models CRM data as objects, fields, and relations. Its standard object split between companies, people, opportunities, tasks, and notes is a useful reference for modular object boundaries.
- [EspoCRM](https://docs.espocrm.com/administration/entity-manager/) exposes entity types, fields, relationships, status fields, Kanban views, full-text search, collaborators, record locking, transactional saves, and audited relationships. These are useful contract patterns for status ownership and auditability.
- [Odoo CRM](https://www.odoo.com/documentation/19.0/applications/sales/crm.html) separates leads/opportunities, pipeline organization, lead assignment, reports, enrichment, and day-to-day activities. This is a useful product reference for pipeline stages and activities.

Do not embed a full CRM framework as the product runtime now. Odoo, EspoCRM, SuiteCRM, and Twenty bring their own object model, UI, permissions, plugin lifecycle, and migration surface. The current product has specialized sourcing workflows, provider queues, canonical projections, local assets, and evidence-backed person enrichment. Reusing a full CRM would likely create another source of truth.

Acceptable reuse:

- Use open-source CRM object models as design references.
- Optionally add export/sync adapters later for external CRM systems.
- Optionally use their UI/admin ideas for list views, Kanban, activities, permissions, and audit streams.
- Do not let an external CRM become the authoritative owner of `person_identity_key`, `ServingProjection`, provider registry state, or workflow recovery.

## Non-Negotiable Rules

1. CRM is person-first. V1 identity is `person_identity_key`, based on the canonical LinkedIn `profile_url_key` when present.
2. One person has one `CRMRecord` per workspace. Multiple projects, roles, or campaigns for the same person are represented by separate `CRMEngagement` rows.
3. CRM records can source people from any `ServingProjection`, external intake, or manual insert, but they do not own projection membership.
4. Local assets do not automatically enter CRM. A person enters CRM only through an explicit user action, Agent action with the required approval policy, import, or migration.
5. CRM may display `do_not_contact`, `manual_exclude`, or `archived`, but it does not directly change projection membership. Default collection/projection visibility changes are owned by the manual overlay writer.
6. User-confirmed facts such as primary email, homepage, X, GitHub, personal website, and preferred avatar are `PersonAssertion` records. CRM references selected assertions; it does not become the source of truth for person facts.
7. V1 keeps `workspace_id="default"` even in single-user mode so future multi-user/tenant migration does not require rewriting primary keys.
8. `CRMTask` is for reminders and follow-up activities only. Acquisition, profile fetch, Public Web enrichment, DataForSEO search, and export generation are long-running operations owned by their own operation/workflow services.
9. CRM state is append-only event-backed. Mutable current-state rows are materialized views of `CRMEvent`, not the only audit source.
10. CRM writer is the only owner of CRM state transitions. Public readers, projections, workflow runners, and frontend caches must not mutate CRM state directly.
11. User or Agent corrections do not overwrite raw assets. They create CRM events and, when they assert a fact about the person, delegate to the `PersonAssertionWriter`.
12. Manual exclude, archive, and do-not-contact are state overlays. They must not physically delete raw local assets or provider evidence.
13. Agent operations call CRM through typed actions and idempotency keys. Agent code must not write CRM tables directly.
14. Missing identity or ambiguous identity fails closed into a review state. It must not silently merge with an existing LinkedIn-backed person.
15. Legacy job-bound target-candidate endpoints may exist only during migration. The steady state is `/api/crm/...` plus projection/person references.

## Local Asset And CRM Relationship

Local assets are the broad inventory; CRM is an explicit business shortlist and follow-up system.

This distinction is required for scalability and product clarity:

- A company collection may contain thousands of local people. They remain browseable/searchable through collection authoritative projections.
- A run projection may contain only the current acquisition scope. It remains a result set, not a CRM list.
- A CRM record is created only when a user, import, migration, or approved Agent action explicitly selects a person for tracking.
- Adding a person to CRM must not trigger profile refetch. The writer first resolves `person_identity_key` / `profile_url_key` through the registry and reuses existing profile/person assets where available.
- Removing or archiving a CRM record must not delete local person assets, raw profiles, public web evidence, or projection membership.
- Collection-level manual overlays can hide excluded people from default collection views, but that is a projection/collection concern, not a CRM-table side effect.

The correct mental model is:

```text
AssetCollection / ServingProjection = what we know and can serve.
CRMRecord / CRMEngagement = who we are actively managing.
PersonAsset / PersonEvidence / PersonAssertion = what we know about the person and why.
```

## Approval Policy

Direct user actions are already approved by the current operator gesture. Agent actions need explicit approval tiers.

Default V1 policy:

| operation | default approval |
| --- | --- |
| `add_to_crm` for selected visible candidates | no extra approval |
| `add_crm_note` | no extra approval |
| `create_crm_task` | no extra approval |
| `set_crm_stage` for one visible record | no extra approval unless stage is sensitive |
| bulk `set_crm_stage` | requires approval |
| `do_not_contact`, `manual_exclude`, `archive` | requires approval |
| export contact fields or CRM notes | requires approval |
| trigger paid provider enrichment | requires approval and budget |
| promote email/homepage/social assertion | requires approval unless it was a direct user promotion gesture |

Approval evidence must be stored in `CRMEvent.metadata_json` or the upstream `AgentAction` audit payload.

## Core Objects

### `CRMRecord`

`CRMRecord` represents one person enrolled in the product's CRM scope.

It answers: "Do we care about this person as a target/contact, and what is their current CRM-level state?"

Required logical fields:

| field | meaning |
| --- | --- |
| `crm_record_id` | stable random id, e.g. `crmrec_...` |
| `workspace_id` | tenant/operator workspace; v1 may be `default` |
| `person_identity_key` | stable person id, v1 `linkedin:{profile_url_key}` |
| `candidate_identity_key` | source projection candidate key, v1 often same as person key |
| `collection_id` | company/local asset namespace when known |
| `display_name_cache` | compact display cache only, not source of truth |
| `headline_cache` | compact display cache only |
| `primary_company_cache` | compact display cache only |
| `avatar_asset_id` | optional person media asset reference |
| `lifecycle_status` | CRM-level lifecycle state |
| `visibility_status` | normal/hidden/archived/tombstone state |
| `owner_user_id` | assigned operator, nullable |
| `source_projection_id` | first/current source projection provenance |
| `source_run_id` | optional source run provenance |
| `source_collection_id` | optional source collection provenance |
| `source_reason` | selected/exported/imported/manual/agent reason |
| `current_engagement_id` | default active engagement, nullable |
| `crm_version` | optimistic concurrency version |
| `created_at`, `updated_at` | timestamps |

Rules:

- Unique v1 key is `(workspace_id, person_identity_key)`.
- If a future product needs multiple separate campaigns for the same person, create separate `CRMEngagement` rows, not duplicate person records.
- Cached display fields may be refreshed from `PersonSummaryView`, but they cannot become the source of truth for identity or evidence.

### `CRMEngagement`

`CRMEngagement` represents a pipeline/follow-up instance for a person.

V1 target-candidate follow-up maps here. This keeps "person is in CRM" separate from "person is in a specific outreach/research pipeline stage."

Required logical fields:

| field | meaning |
| --- | --- |
| `engagement_id` | stable random id, e.g. `crmeng_...` |
| `crm_record_id` | parent CRM record |
| `pipeline_id` | e.g. `default_sourcing`, future role/project pipelines |
| `stage` | current pipeline stage |
| `stage_category` | `open`, `waiting`, `terminal_success`, `terminal_loss`, `blocked`, `archived` |
| `priority` | operator/Agent priority |
| `quality_score` | optional 0-100 score with provenance |
| `next_action_at` | optional follow-up reminder timestamp |
| `last_contacted_at` | nullable |
| `source_projection_id` | source projection for this engagement |
| `source_run_id` | optional source run |
| `source_selection_reason` | why this person entered this engagement |
| `created_by_actor` | user/agent/import |
| `created_at`, `updated_at` | timestamps |

V1 default pipeline stages:

| stage | category | legacy mapping |
| --- | --- | --- |
| `new` | `open` | newly added target candidate |
| `researching` | `open` | needs profile/evidence review |
| `outreach_ready` | `open` | `pending_outreach` |
| `contacted_waiting` | `waiting` | `contacted_waiting` |
| `responded` | `open` | future |
| `interview_completed` | `terminal_success` | `interview_completed` |
| `accepted` | `terminal_success` | `accepted` |
| `rejected` | `terminal_loss` | `rejected` |
| `do_not_contact` | `blocked` | future |
| `archived` | `archived` | future |

Allowed transitions should be explicit:

```text
new -> researching -> outreach_ready -> contacted_waiting -> responded -> interview_completed -> accepted
new -> outreach_ready
outreach_ready -> rejected
contacted_waiting -> rejected
any open/waiting -> do_not_contact
any non-tombstone -> archived
archived -> previous_open_stage only via explicit restore event
```

### `CRMEvent`

`CRMEvent` is the durable audit source for CRM changes.

Required logical fields:

| field | meaning |
| --- | --- |
| `event_id` | stable random id, e.g. `crmevt_...` |
| `crm_record_id` | target record |
| `engagement_id` | nullable target engagement |
| `event_type` | structured event type |
| `actor_type` | `user`, `agent`, `system`, `migration`, `import` |
| `actor_id` | user id, agent action id, migration id, or system owner |
| `idempotency_key` | required for API/Agent/migration writes |
| `source_projection_id` | projection provenance when applicable |
| `source_run_id` | run provenance when applicable |
| `source_event_id` | upstream event/action id when applicable |
| `previous_state_hash` | optional optimistic audit hash |
| `new_state_hash` | optional materialized state hash |
| `patch_json` | compact state patch |
| `reason` | operator/Agent-readable reason |
| `metadata_json` | bounded metadata, no raw provider payload |
| `created_at` | timestamp |

Required event types:

| event type | owner action |
| --- | --- |
| `crm_record_created` | add person to CRM |
| `crm_record_merged` | identity merge/alias resolution |
| `crm_record_hidden` | hide from default CRM views |
| `crm_record_archived` | archive record |
| `engagement_created` | add person to a pipeline |
| `engagement_stage_changed` | update pipeline stage |
| `engagement_priority_changed` | update priority |
| `engagement_quality_scored` | update score |
| `crm_note_added` | add operator/Agent note |
| `crm_task_created` | create follow-up task |
| `crm_task_completed` | complete follow-up task |
| `person_assertion_requested` | request assertion writer promotion |
| `person_assertion_linked` | link completed assertion to CRM |
| `crm_exported` | export included this record |
| `source_projection_attached` | attach source provenance |
| `manual_review_linked` | link a manual review resolution |

Rules:

- Events are append-only.
- Updates must be idempotent by `(event_type, idempotency_key)`.
- Current CRM rows are updated in the same transaction as the event append.
- Event payloads must be bounded. Raw HTML, raw profile JSON, large search payloads, and large replay records belong in person asset/evidence stores, not CRM events.

### `CRMTask`

`CRMTask` is an optional activity/reminder object for follow-up operations. `crm_tasks` is the PG-only query/reminder current-state table; `crm_events` remains the append-only audit trail for task creation/completion. SQLite DDL/fallback is not a normal path for `crm_tasks`.

It should not be used to model long-running acquisition/enrichment work. Long-running work belongs to `OperationRun` or workflow queues.

Read APIs:

| endpoint | owner | source of truth |
| --- | --- | --- |
| `GET /api/crm/tasks` | CRM read adapter | `crm_tasks` |
| `GET /api/crm/records/{crm_record_id}/tasks` | CRM read adapter | `crm_tasks` |

Both endpoints must expose `read_contract.source=crm_tasks`, `read_contract.audit_source=crm_events`, `fallback_used=false`, and `legacy_target_candidates_used=false`.

Fields:

| field | meaning |
| --- | --- |
| `task_id` | stable random id |
| `workspace_id` | tenant/workspace boundary, default `default` |
| `crm_record_id` | target record |
| `engagement_id` | nullable engagement |
| `person_identity_key` | denormalized person key from the CRM record |
| `title` / `description` | bounded user/Agent-facing task text |
| `status` | `open`, `in_progress`, `completed`, `cancelled` |
| `priority` | `low`, `normal`, `high`, or future owner-defined values |
| `due_at` | optional |
| `created_by_actor` | user/agent/system |
| `created_by_actor_id` | optional actor id |
| `source_event_id` | event that created the task |
| `idempotency_key` | command/action-level dedupe key |

### Not CRM-Owned: Person Assets, Evidence, Assertions

CRM may reference these objects but must not own their storage:

| object | owner | examples |
| --- | --- | --- |
| `PersonAsset` | Person asset writer | avatar media, raw LinkedIn profile, homepage snapshot, X/GitHub profile snapshot |
| `PersonEvidence` | Evidence ingestion/indexer | DataForSEO result, public web snippet, fetched document, model-safe analysis |
| `PersonAssertion` | Assertion writer | confirmed primary email, confirmed homepage, confirmed X URL, current company claim |
| `ServingProjection` | Projection writer | run result membership, collection authoritative membership |
| `AcquisitionRun` | Workflow runner | provider search/profile execution |

Promotion boundary:

```text
Public Web signal -> PersonEvidence
User/Agent promotes signal -> CRMEvent(person_assertion_requested)
PersonAssertionWriter validates/writes assertion
CRMEvent(person_assertion_linked)
CRM current view may show selected assertion
```

This replaces the current pattern where a Public Web promotion can directly mutate `target_candidates.primary_email`.

## Public API Contract

Expected v1 API shape:

```text
GET  /api/crm/records
POST /api/crm/records
GET  /api/crm/records/{crm_record_id}
PATCH /api/crm/records/{crm_record_id}
GET  /api/crm/records/{crm_record_id}/events
POST /api/crm/records/{crm_record_id}/events
POST /api/crm/records/public-web-search
POST /api/crm/records/public-web-search/poll
POST /api/crm/records/public-web-search/cancel
POST /api/crm/records/public-web-search/retry
GET  /api/crm/records/{crm_record_id}/profile
GET  /api/crm/records/{crm_record_id}/public-web-search
GET  /api/crm/records/{crm_record_id}/public-web-promotions
POST /api/crm/records/{crm_record_id}/public-web-promotions
POST /api/crm/records/public-web-export
GET  /api/crm/engagements
POST /api/crm/engagements
PATCH /api/crm/engagements/{engagement_id}
POST /api/crm/batch-add-from-projection
POST /api/crm/export
GET  /api/projections/{projection_id}/crm-state
```

Ownership:

- `/api/crm/...` CRM-state writes go through `CRMWriter`.
- `GET /api/crm/records` is the CRM list owner for target-candidate views. It accepts `source_projection_id` and `source_collection_id` as provenance filters; local asset target tabs must pass `source_collection_id` instead of reading the global CRM list and filtering in the frontend. The response echoes both filters and must fail closed with no legacy target-candidate fallback.
- `/api/crm/records/.../public-web...` routes are the normal frontend path for candidate-level Public Web operations. They require real `crm_records` ids and must not accept legacy target-candidate-only ids as normal input. CRM list/detail/promotion/export reads are owned by `crm_public_web_*` tables; the current execution backend may still mirror from the existing Public Web worker until Phase 9b.2b retires that backend.
- `/api/projections/{projection_id}/crm-state` is read-only and returns CRM overlay state for visible candidates.
- `/api/target-candidates/public-web...` is permanently retired and returns `410` with canonical CRM endpoint pointers. `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS` is retained only as historical/report context and must not re-enable target-candidate Public Web execution. Other `/api/target-candidates/...` compatibility surfaces should continue moving behind CRM/projection owners and then retire.

### Batch Add From Projection

Input must include:

```json
{
  "source_projection_id": "proj_...",
  "candidate_identity_keys": ["linkedin:..."],
  "pipeline_id": "default_sourcing",
  "initial_stage": "outreach_ready",
  "source_reason": "operator_selected_from_projection",
  "idempotency_key": "..."
}
```

Rules:

- The CRM writer resolves each candidate through the projection service.
- Raw source rows that dedupe to the same `person_identity_key` create one CRM record.
- Existing CRM records get a new source event or engagement update, not duplicate records.
- Missing projection membership fails closed.

### Projection CRM State Read

Projection pages may request CRM overlay state for the current page or selected ids.

The response may include compact fields:

```json
{
  "projection_id": "proj_...",
  "records": [
    {
      "candidate_identity_key": "linkedin:...",
      "person_identity_key": "linkedin:...",
      "crm_record_id": "crmrec_...",
      "engagement_id": "crmeng_...",
      "stage": "outreach_ready",
      "stage_category": "open",
      "owner_user_id": "",
      "next_action_at": "",
      "last_event_at": "2026-05-19T12:00:00+08:00"
    }
  ],
  "count_scope": "current_page"
}
```

Rules:

- This endpoint reads CRM state only; it must not create records automatically.
- It must not scan unrelated jobs or target-candidate legacy rows.
- It should be pageable/batched for large projections.

## Frontend Contract

Frontend surfaces:

- Projection result page: shows per-person CRM state chips and "add to CRM" actions.
- Local asset entry: can add selected people from the authoritative collection projection to CRM.
- CRM page: replaces the long-term role of target-candidate page; serves pipeline/list/Kanban/task views from CRM APIs.
- Person detail drawer: composes `PersonSummaryView`, CRM state, assertions, evidence, assets, and source projection provenance.

Frontend must not:

- Compute CRM state from candidate card fields.
- Infer primary email/homepage/X from Public Web detail without a `PersonAssertion`.
- Treat loaded candidate rows as the CRM population.
- Update CRM fields on every local draft edit. Draft edits become CRM events only on explicit save.

## Manual Review Boundary

Manual review and CRM are related but not the same module.

Manual review owns workflow/product quality questions such as:

- ambiguous identity
- low profile richness
- missing LinkedIn URL
- evidence conflict
- unresolved external intake row

CRM owns business follow-up state such as:

- selected target
- outreach readiness
- contacted/waiting
- rejected/accepted
- do-not-contact
- notes/tasks/export history

When a manual review resolution changes a CRM-facing fact, it should create:

1. A manual review resolution record.
2. A `PersonAssertion` if it confirms a person fact.
3. A `CRMEvent` if it changes CRM state or attaches evidence to a CRM record.

## Agent Boundary

Agent is not part of the CRM state machine. Agent is a caller.

Allowed Agent-to-CRM operations:

| action | CRM owner |
| --- | --- |
| `add_to_crm` | `CRMWriter` |
| `set_crm_stage` | `CRMWriter` |
| `add_crm_note` | `CRMWriter` |
| `create_crm_task` | `CRMWriter` |
| `attach_source_projection` | `CRMWriter` |
| `request_person_assertion_promotion` | `CRMWriter` then `PersonAssertionWriter` |
| `export_crm_records` | `CRMExportService` |

Required Agent metadata:

- `agent_conversation_id`
- `agent_action_id`
- `idempotency_key`
- `operator_user_id` or service actor
- action reason
- source projection/person ids
- budget/cost metadata when enrichment is triggered

Rules:

- Agent does not write CRM tables directly.
- Agent does not mutate projection membership.
- Agent does not write raw person assets.
- Agent may request acquisition/enrichment through separate operation owners, then attach resulting evidence/assertions to CRM.

## Migration Plan

### Phase 1: Schema and Writer

- Implemented v1 foundation: `crm_records`, `crm_engagements`, `crm_tasks`, and `crm_events`.
- Implemented v1 foundation: `CRMWriter.add_projection_member_to_crm(...)` creates or reuses one CRM record per `(workspace_id, person_identity_key)`, creates a default engagement, appends an idempotent event, and preserves source projection/run provenance.
- Implemented v1 foundation: shared person identity resolution helper is used by projection member writes and CRM writes.
- Remaining: full stage mutation APIs and transactional multi-row writer hardening for production-scale bulk operations.

### Phase 2: Target-Candidate Backfill

- Foundation added: legacy `target_candidates` now has `person_identity_key`, `candidate_identity_key`, `source_projection_id`, optional `source_run_id`, `source_collection_id`, and `source_reason` fields so migration can preserve projection provenance.
- Implemented foundation: `CRMTargetCandidateMigrationBackfill` backfills each `target_candidates` row into `CRMRecord` + `CRMEngagement` through `CRMWriter`.
- Implemented foundation: `follow_up_status` maps to `CRMEngagement.stage` for the v1 default sourcing pipeline.
- Implemented foundation: legacy `primary_email` becomes `PersonAssertion(authority='legacy_migrated', verification_status='needs_review')` so contact export can require review instead of treating a target-candidate field as truth.
- Remaining: full historical production run, explicit `engagement_created` event type, and Public Web execution-backend retirement.

### Phase 3: Public Web Promotion Migration

- Keep `person_public_web_assets` and `person_public_web_signals` as person/evidence inputs.
- Implemented v1: `/api/crm/backfill-public-web-promotions` migrates `target_candidate_public_web_promotions` to `PersonAssertion` rows and `person_assertion_linked` CRM events.
- Implemented v1: future target Public Web promotion writes a `PersonAssertion` plus CRM assertion-link event in addition to legacy target-candidate compatibility state.
- Implemented v1: target-candidate page Public Web operations now call CRM-owned routes. The route contract is CRM/person-first.
- Implemented v1: legacy target-candidate Public Web HTTP aliases return `410` with canonical CRM endpoint pointers; the old `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS` override is removed and ignored.
- Implemented v1: legacy target-candidate Public Web HTTP aliases are unconditional `410` branches. API code no longer keeps dead override branches that call target-candidate Public Web orchestrator methods, so endpoint retirement is enforced by source boundary as well as runtime behavior.
- Implemented v1: legacy target-candidate Public Web orchestrator start/cancel/retry/list methods always fail closed with `legacy_target_public_web_orchestrator_disabled`; API retirement is not the only guard. Internal callers cannot bypass the canonical CRM Public Web owner.
- Implemented v1: legacy target-candidate Public Web lower-level execution helpers always fail closed. Direct calls to target-candidate batch creation, cancel, worker execution, or batch-summary sync return a report-visible `legacy_target_public_web_execution_disabled` envelope with `migration_override_status=removed`. Normal code must use CRM Public Web owner tables, `crm.public_web.queue_batch`, and `crm_public_web_search` workers.
- Implemented v1: lower-level target-candidate Public Web helper bodies no longer retain unreachable old write paths after the fail-closed check. `public_web_runtime_core.py` keeps no normal or dead-code route that can upsert/update target-candidate Public Web batches/runs and no longer directly calls legacy storage helpers. The target-to-CRM migration bridge reads legacy rows through `legacy_public_web_storage.py`.
- Implemented v1: legacy target-candidate Public Web read/promotion/export orchestrator methods always fail closed. Target-candidate profile compatibility must not derive contact readiness from legacy Public Web runs/assets/signals; CRM Public Web owns detail, promotion, and export behavior.
- Implemented v1: worker recovery no longer executes Public Web runtime functions. Historical `crm_public_web_search` and `target_candidate_public_web_search` `agent_worker` rows are quarantined/retired report-visibly by the worker daemon; normal CRM Public Web execution is owned by `crm.public_web.queue_batch` plus per-run phase commands. There is no generic recovery-kind-to-target-owner fallback in the normal worker daemon path.
- Implemented v1: worker daemon no longer imports the legacy target-candidate Public Web runtime module. Historical `target_candidate_public_web_search` recovery rows are matched by a local retired recovery-kind constant and terminalized as quarantine evidence only. The old `run-target-candidate-public-web-experiment` CLI command is also hard-retired and returns a report-visible retirement envelope instead of importing or executing target-candidate Public Web experiment code.
- Implemented v1: CRM Public Web owner storage exists for batches, runs, and promotions; CRM detail/promotion/export reads from `crm_public_web_*`. Legacy target-candidate Public Web rows are not mirrored into CRM owner tables by default; historical conversion requires explicit `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC=1` and remains report-visible migration-only evidence.
- Implemented v1: CRM Public Web reads are owner-only and fail closed. They do not mutate state on read, do not sync from legacy target-candidate Public Web rows during poll/detail/export, and do not treat person assets that only point at legacy run ids as valid CRM results.
- Implemented v1: Public Web smoke/service metrics expose `storage_owner_counts`, `crm_storage_owner_batch_count`, `legacy_storage_owner_batch_count`, and `execution_backend_counts`. Pre-Manual Signoff blocks missing owner/backend reports, any legacy storage owner in normal Public Web action cases, and any `target_candidate_public_web_v1` execution backend in normal CRM Public Web action cases.
- Implemented v1: CRM Public Web execution is CRM-owned. Start/cancel/retry/recovery use `crm_public_web_v1` owner storage; promotion writes `crm_public_web_promotions`, `PersonAssertion`, and `CRMEvent` directly without target-candidate bridge rows. Start now plans `workflow_commands(command_type='crm.public_web.queue_batch', owner='crm_public_web_owner')`; that owner creates/validates batch/run rows and plans per-run phase commands (`crm.public_web.search.submit`, `crm.public_web.search.poll_fetch`, `crm.public_web.documents.fetch`, `crm.public_web.evidence.adjudicate`, `crm.public_web.model_safe.finalize`, `crm.public_web.signals.materialize`). Route-local worker creation is not a normal path, and OperationRun completion is deferred until terminal downstream phase command success.
- Implemented v1: legacy target-candidate Public Web APIs no longer auto-create target-candidate compatibility rows for CRM records. A CRM record id sent to `/api/target-candidates/public-web...` receives the permanent retired `410` envelope before any bridge or target lookup.
- Implemented v1: legacy target-candidate Public Web aliases are not normal priority-lane routes. `_request_priority_lane` only treats canonical `/api/crm/records/public-web...` routes as interactive/light-lane Public Web surfaces; retired `/api/target-candidates/public-web...` aliases remain unconditional compatibility responses.
- Implemented v1 signoff evidence: fresh targeted CRM Public Web fake-provider/action smoke passed at `output/w7_crm_public_web_fresh_targeted_20260524/` with case status `completed`, `expectation_failures=[]`, Pre-Manual Signoff `passed`, `storage_owner_counts={'crm_public_web_v1': 1}`, `execution_backend_counts={'crm_public_web_v1': 1}`, and one succeeded `crm.public_web.queue_batch` command owned by `crm_public_web_owner`.
- Implemented v1: legacy target-candidate Public Web pre-delete evidence is now a normal diagnostics contract, not only a standalone script. `/api/migrations/legacy-public-web` exposes the read-only audit, `workflow_service_metrics.legacy_public_web_retirement` summarizes deletion readiness, and Pre-Manual Signoff can require `require_legacy_public_web_retirement_ready`. Legacy target-candidate Public Web rows or row-limit-truncated audits block W7e deletion.
- Implemented v1: `tests/test_crm_public_web_runtime_boundary.py` is the fast W7e source preflight. Normal source files cannot import CRM Public Web behavior from the legacy target-candidate facade and cannot call `target_candidate_public_web_*` storage helpers; those helpers are limited to storage/PG schema and `legacy_public_web_storage.py`. Historical/migration tests that need old rows must use `legacy_public_web_storage.seed_legacy_target_public_web_*`; direct `store.upsert_target_candidate_public_web_*` writes are allowed only in the PG storage-authoritative test that proves legacy table cold-backup/migration behavior. The migration-only reader returns empty lists if the old tables are already physically absent, so deletion audits do not require normal store helper/table recreation.
- Implemented v1: new SQLite compatibility shadows do not bootstrap `target_candidate_public_web_*` tables in the normal schema, and normal PG live/bootstrap defaults no longer list those retired tables. If older shadows or PG schemas already contain historical rows, they are preserved for migration/cold-backup reads; otherwise empty legacy tables are absent. Explicit migration test seeding recreates the retired tables only inside `legacy_target_public_web_migration_write_context(...)`, which also enables a migration-only PG table context. PG writer-schema repair skips legacy Public Web indexes unless historical tables already exist.
- Implemented v1: W7e physical deletion has an explicit migration-only archive/drop utility. `scripts/archive_drop_legacy_public_web_tables.py` can write `legacy_public_web_archive_v1` manifests and drop retired tables under `legacy_public_web_drop_v1`; non-empty legacy tables require a cold archive path before drop unless a dangerous override is supplied. This utility is an operator migration path only and is not a CRM/Public Web serving source.
- Implemented v1: 2026-05-26 active-runtime closeout wrote `runtime/audits/legacy_public_web_archive_w7e_latest.json` and `runtime/audits/legacy_public_web_drop_w7e_latest.json`; the drop manifest records zero archived legacy rows and physical removal of `target_candidate_public_web_batches`, `target_candidate_public_web_runs`, and `target_candidate_public_web_promotions` from PG. Future historical access must stay behind `legacy_public_web_storage.py` migration context.
- Implemented v1: legacy target-candidate Public Web storage writes are runtime-gated. `ControlPlaneStore.upsert/update_target_candidate_public_web_*` fail closed by default with `legacy_target_candidate_public_web_write_retired`; migration-only seeding must enter `legacy_target_public_web_migration_write_context(...)` through `legacy_public_web_storage.seed_legacy_target_public_web_*`.
- Remaining: run a small live-provider CRM Public Web validation before deleting legacy target-candidate Public Web aliases/env and legacy target-candidate Public Web tables/helpers. Fake-provider evidence proves ownership and recovery semantics; live evidence is needed for real search quality, promotion UX, export payload quality, and final operator acceptance. Delete only after broader production migration/signoff evidence confirms no old target-candidate-only Public Web data needs interactive access.

### Phase 4: API and Frontend Cutover

- Implemented v1: `/api/crm/...` APIs exist for list/add/update and target-candidate page normal state.
- Implemented v1: target-candidate page reads/writes CRM APIs, including CRM-owned Public Web route wrappers.
- Keep legacy target-candidate endpoints as explicit migration aliases only. `POST /api/target-candidates/export` is retired by default and can be enabled only with `SOURCING_ALLOW_LEGACY_TARGET_CANDIDATE_EXPORT=1` for controlled migration/test coverage.
- Candidate-level Public Web aliases under `/api/target-candidates/public-web...` are permanently retired and cannot be re-enabled by env override.
- If historical target-candidate Public Web data must be reviewed, use the cold-backup/migration audit path or an explicit migration/backfill command; do not let a read/start/poll path synthesize target rows.
- Add report-visible fallback metrics while aliases remain.

### Phase 5: Legacy Retirement

- Stop normal writes to `target_candidates`.
- Remove job/history filters from normal CRM reads.
- Retain legacy tables only as archived migration evidence or delete after approved backup.

## Tests And Gates

Implementation is incomplete until tests prove:

- Creating CRM records from a projection dedupes by `person_identity_key`.
- CRM cannot create records from a candidate not present in the declared source projection unless the source is explicit external/manual intake.
- CRM events are appended in the same transaction as current-state updates.
- Duplicate idempotency keys do not create duplicate events or records.
- Public projection readers do not write CRM state.
- Agent-style CRM actions go through the same writer and preserve actor/idempotency metadata.
- Public Web promotion creates or links `PersonAssertion`; it does not only mutate a CRM current-state field.
- Legacy target-candidate rows can be backfilled without changing current user-visible target-candidate state.
- Frontend CRM chips and CRM page state come from CRM APIs, not candidate card fields.
- Exports include source projection/person/assertion provenance and skip reasons.

Suggested scripted/manual coverage:

- Add selected OpenAI run-scope candidates to CRM from `/projections/{projection_id}`.
- Add Lovable live-roster candidates to CRM and verify deduped 140-person behavior.
- Add Google large projection page selections to CRM without scanning full job artifacts.
- Promote a Public Web email signal and verify CRM event plus person assertion.
- Update follow-up stage from frontend and from an Agent action; both paths produce equivalent CRM events.

## Anti-Patterns

Do not reintroduce:

- CRM records keyed primarily by `job_id`, `history_id`, or runtime candidate ids.
- CRM writes from public readers, projection readers, or workflow recovery.
- Public Web promotion that only edits `primary_email` without assertion provenance.
- Saved frontend filters as projections.
- Agent direct SQL/table writes to CRM.
- Physical deletion of local person assets for ordinary CRM exclude/archive.
- Large raw provider/evidence payloads embedded in CRM events.
- Silent legacy target-candidate fallback after CRM API cutover.
