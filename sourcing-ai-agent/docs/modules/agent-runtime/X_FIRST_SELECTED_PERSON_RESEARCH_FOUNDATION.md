# X-First selected-person research foundation

> Status: provider-free integration foundation. `served=0`. This document does not activate an Agent tool, Grok/Luna
> execution, PersonEvidence materialization, canonical identity merge, outreach, or product-visible completion.

## Product goal

The product action is `research_selected_people_on_x`. It accepts a user-selected set of people plus a typed research
intent and uses X-First to explore or verify evidence from each person's public X surface. It is deliberately generic:

- `analysis_mode = exploratory | verification | hybrid`;
- target dimensions and labels come from a fresh Research Scope Catalog, not from pre-training-specific fields;
- the default evidence channel is the subject's own Post and Reply;
- direct-credit, official-source, and conversation-graph channels remain explicit opt-ins; and
- optional China/Asia professional or educational experience review is a separately requested dimension.

The action does not require a CRM record. The selected population remains owned by the exact serving projection
snapshot. X-First provides external-account observations and reversible identity-link proposals; it does not become a
canonical person owner.

## Implemented portable slice

The first slice closes the provider-free round trip:

```text
exact selected projection-member snapshot
  -> sourcing.x_first.subject_selection.v1
  -> X-First selected-subject adapter
  -> x.portable.research_campaign.request.v1 + request binding
  -> X-First plan/result validator
  -> sourcing.x_first.verification_import_preview.v1
```

The sourcing-owned exporter:

- consumes the same owner-bound target shape already used for exact projection selection;
- requires the selected member set to match the bound, revision-fenced candidate keys exactly;
- exports only name, public professional facts, source profile URL, and explicit X-handle proposals;
- hashes every source row, the selected member set, the schema bytes, and the complete artifact; and
- never exports raw profile payloads, contact details, CRM overlays, or arbitrary `Candidate.to_record()` fields.

The import adapter returns a review projection only. It binds the selection, portable request, and portable result
hashes; maps terminal subject outcomes back to opaque source record refs; and exposes bounded link-proposal,
observation, and verification summaries. Its authority always keeps product writes and automatic identity merge off.

Implemented contracts and owners:

| Contract | Owner / source of truth | Normal consumer |
| --- | --- | --- |
| `sourcing.x_first.subject_selection.v1` | sourcing product export adapter | X-First selected-subject adapter |
| `x.portable.research_campaign.request.v1` | X-First orchestration owner | X-First planner / reviewed runner |
| `x.portable.selected_subject.request_binding.v1` | X-First selected-subject adapter | sourcing import adapter |
| `x.portable.research_campaign.result.v1` | X-First result validator / runner | sourcing import adapter |
| `sourcing.x_first.verification_import_preview.v1` | sourcing import adapter | later identity/evidence adjudication owner |

Neither project imports the other's Python runtime. Contract exchange uses versioned JSON plus exact schema and payload
digests. Dynamic exchange artifacts do not belong in `local_asset_packages` and must not use the cloud-asset restore
path.

## Agent request and owner-bound target

The future Agent request expresses research intent only. It must not ask the model to copy names, experiences, or
candidate keys into free-form input.

```json
{
  "analysis_mode": "verification",
  "questions": [],
  "target_dimensions": [],
  "target_match_operator": "any",
  "authored_surface_policy": "post_and_reply",
  "optional_channels": [],
  "as_of": "2026-07-18T00:00:00Z",
  "research_scope_catalog_ref": "artifact:...",
  "campaign_policy_ref": "artifact:..."
}
```

The server-owned target is minted from an authenticated projection read:

```json
{
  "workspace_id": "...",
  "projection_id": "...",
  "membership_revision": "...",
  "source_candidate_count": 42,
  "candidate_identity_keys": ["...", "..."]
}
```

Before runtime activation, the shared core of `CRMProjectionSelectionTargetBinder` should be extracted into a
parameterized `ProjectionSelectionTargetBinder`. CRM keeps a compatibility wrapper; the X action supplies its own
owner. This avoids two subtly different revision, visibility, and stale-selection implementations.

## Runtime owner and state machine

The proposed runtime owner is `x_research_campaign_owner`.

```text
submit
  -> approval_required
  -> queued
  -> dispatch_preflight
  -> running
  -> materializing
  -> completed | failed | cancelled
```

The state transitions have these service semantics:

1. `submit`: bind workspace, projection, expected membership revision, and every visible selected key. A stale or
   missing member produces zero Operation, Activity, Attempt, or provider effect.
2. `approval_required`: bind campaign budget, deadline, and provider/model policy before any paid call.
3. `queued`: create the durable Operation and root `x.research_campaign.create` command.
4. `dispatch_preflight`: revalidate request schema identity, exact selection, workspace parity, scope catalog, and
   campaign policy before creating provider attempts. Drift terminates as `reselection_required`.
5. `running`: execute each subject through `x.person_research.run`. Completed subjects and completed attempt ordinals
   are never rerun during partial retry.
6. `materializing`: validate the self-contained request/plan/catalog/result package, then enqueue
   `person.x_evidence.materialize` for reviewable evidence only.
7. terminal: every selected subject and every materialization decision is terminal-total before the Operation can
   become `completed`; late results after cancel enter quarantine.

Large Post/Reply bodies stay in a content-addressed artifact store. Workflow events and Agent results carry bounded
counts, hashes, terminal summaries, and opaque `artifact:` refs.

## Owner and source-of-truth matrix

| Concern | Canonical owner / source of truth | Fail-closed condition |
| --- | --- | --- |
| Selected population | serving projection repository/writer | revision or member-set mismatch |
| Name and professional seed facts | projection member `public_summary` | missing name or non-allowlisted input |
| Agent intent | `ActionRequestSpec` + Operation runtime | unknown field, schema digest, or action |
| Portable campaign | `x_research_campaign_owner` | request/catalog/policy/plan hash mismatch |
| Grok/Luna call receipt and cost | ActivityRun / ActivityAttempt | no receipt, deadline, grant, or effective model binding |
| X account and public observations | X-First result artifact | unbound platform account, Post, Reply, or receipt |
| Source subject to X account | reversible link proposal | automatic merge or missing adjudication |
| PersonAsset / PersonEvidence | product domain writers | direct X-First/product adapter write |
| PersonAssertion | assertion owner after explicit adjudication | model-only or unreviewed claim |
| Projection search refresh | `projection.person_search_index.build` owner | implicit membership mutation |

## Activation sequence

The following work remains intentionally deferred and must land in order:

1. extract the shared projection-selection binder and add the X owner wrapper;
2. register `research_selected_people_on_x` with a closed request schema and explicit approval/budget policy;
3. register durable commands `x.research_campaign.create`, `x.person_research.run`, and
   `person.x_evidence.materialize` with ActivityAttempt accounting and cancellation semantics;
4. add a content-addressed request/result package plus a semantic-validation receipt that binds validator revision;
5. implement item-level retry, no-evidence success, cancellation quarantine, and terminal-total campaign recovery;
6. implement a product-domain materializer that writes only through PersonAsset/PersonEvidence owners and remains
   idempotent on campaign, person, evidence kind, and content hash;
7. refresh the projection person-search index without changing projection membership;
8. add strict Agent result schemas and bounded frontend status/summary adapters; and
9. complete targeted service simulation, independent pinned review, and only then change `served=0`.

The existing `enrich_person_public_web` CRM action is not a substitute for this lane. Reusing it would incorrectly
require CRM membership and couple X research to the CRM public-web command chain.

## Current validation

Provider-free regression coverage includes:

- exact selected-member set and row hash binding;
- an explicit allowlist that excludes raw/contact fields;
- LinkedIn, professional-profile, known-X-proposal, and name-only seed shapes;
- request/result rebinding rejection;
- read-only import preview with no canonical/product write authority;
- no runtime import between `sourcing_agent` and `x_first`; and
- verification from broad authored Post and Reply without mandatory alias-search calls.

Live Grok/Luna execution and PersonEvidence materialization are not part of this slice.
