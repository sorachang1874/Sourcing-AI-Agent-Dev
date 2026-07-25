# Track D D1m Company Public Web Action Schema Activation

> Status: Stable implementation candidate with author evidence; fresh pinned non-author review pending. This document
> records the bounded non-live contract and is not a formal `GO`, live-provider approval, manual/product signoff, or
> milestone closure.

Date: 2026-07-17

## Scope

D1m activates only `refresh_company_public_web_assets` as the tenth schema-defined production Operation action. It
replaces that action's loose request carriers with one deterministic `seed_url_only` request and one owner-minted
company target while preserving the existing root -> source collection -> canonical materialization command topology.

D1m does not:

- serve any production action to a model;
- call or authorize a provider, model, or live path;
- add provider search, collector-bundle, or implicit URL-discovery behavior;
- change the company Public Web source-row, `CompanyAsset`, or `CompanyEvidence` owners;
- claim that Operation/action, command planning, phase effects, command terminal state, and linked Operation sync share
  one global UoW;
- close R-019 or R-029.

## Request contract

`ActionRequestSpec` owns `company_public_web_refresh_request_v1`, a closed request with these canonical input fields:

| Field | Presence | Canonical rule |
| --- | --- | --- |
| `target_company` | required | nonblank string, trimmed before persistence |
| `source_families` | required | nonempty closed-family array, deduplicated and sorted |
| `seed_urls` | required | nonempty HTTP/S array, URL-normalized, deduplicated, and sorted |
| `max_assets` | optional | integer `1..500`; canonical default `50` |
| `force_refresh` | optional | boolean; canonical default `false` |
| `refresh_nonce` | conditional | required and nonblank iff `force_refresh=true`; forbidden otherwise |
| `collection_mode` | optional | only `seed_url_only`; canonical default `seed_url_only` |

Legacy nested/identity carriers such as `command_type`, `command_payload`, `workflow_run_id`, `job_id`, `options`,
`company`, `company_name`, `company_key`, `workspace_id`, and `tenant_id` are not alternate execution sources. The
schema and binder reject them instead of merging precedence ladders. `provider_search` and `collector_bundle` are not
valid `collection_mode` values for this action.

The persisted action request must equal its canonical normalization. Strict JSON validation rejects Python-only
containers and prevents a replayed action from changing ordering, URL form, defaults, refresh identity, or target
shape after the initial write.

## Owner-bound target and transport

`CompanyPublicWebTargetBinder` is the target owner. It accepts exactly the `target_company` selector, resolves the
canonical company key through the existing company alias resolver, and mints:

```text
OwnerBoundTargetRef(
  owner_module="company_public_web_owner",
  target_ref={"workspace_id": <server/operator workspace>, "company_key": <canonical alias key>},
)
```

The stable request identity uses the exact target pair `workspace_id + company_key`.

- Authenticated transport derives workspace and actor from request state. Caller workspace/tenant/company-key aliases
  cannot override the owner target.
- Open mode preserves the existing explicit operator workspace and applies the same canonical company-key binder.
- A caller-supplied nonempty `target_ref`, incomplete authenticated bind context, workspace mismatch, noncanonical
  company key, or target/input owner alias fails before action/run persistence.

## Persisted, dispatch, command, and phase revalidation

D1m carries one exact request/target through the existing command decomposition:

1. **Submit:** the binder mints `{workspace_id, company_key}` and the schema canonicalizes the input before the first
   action write. `request_schema_version` and digest remain registry-derived.
2. **Persisted action/run:** `validate_persisted_action_request(...)` checks current schema pins, strict JSON,
   canonical-input equality, exact action/run linkage, and the owner-bound target.
3. **Dispatch:** `_revalidate_company_public_web_action_target(...)` rechecks Operation workspace, canonical alias key,
   canonical `target_company`, and `seed_url_only` before
   `_schema_defined_company_public_web_command_plan(...)` can plan `company.public_web.refresh`. The root command carries
   the exact target, canonical input fields, `operation_run_id`, and `action_id`; its physical `operation_id` binds the
   persisted OperationRun.
4. **Root owner:** before planning `company.public_web.source.collect`, the owner reloads the persisted command,
   OperationRun, and AgentAction and exact-compares schema/request/target, workflow identity, operation/action links,
   approval/status, and the expected command payload. The root copies the same bound target and canonical input into the
   source phase rather than re-deriving them from loose aliases.
5. **Source collection owner:** the same command-target revalidation runs before source-row/artifact effects. The
   source request carries the server-private physical command id, current attempt, and lease owner; these values are
   never accepted from the public action schema. Migration `0009_company_public_web_asset_run_idempotency.sql` adds a
   normalized-nonblank partial unique idempotency index. The PG owner takes sorted `run_id + effective
   idempotency_key` advisory locks, rejects split identities, and either creates one row, joins an exact completed v3
   row, or lets a higher current attempt of the same source command reclaim a `running|failed` row. A typed legacy or
   revisionless completed row is read-only evidence and fails closed/skips with the native
   `company_public_web_completed_run_replay_read_only` reason; it requires a fresh force-refresh or a separately
   reviewed repair, never implicit materialization. A stale,
   lower, or wrong-lease attempt returns `owner_lost` without changing the source run. Completion/failure takes the
   same identity locks and exact-compares the current command id/attempt/lease before a terminal CAS. The expected
   source idempotency, deterministic command id, and plan event id are recomputed from the exact persisted parent; a
   merely self-consistent replacement envelope does not pass. After the source snapshot is frozen, a pure completion
   contract precomputes the materialize plan, deterministic child, and source-run EntityDelta. One closed PostgreSQL
   UoW then locks and revalidates the exact unexpired source claim, exact-creates/reuses the plan event, child, and
   EntityDelta, and terminalizes the parent with the matching physical downstream edge. A takeover can win before or
   after that bundle, never between its writes. Stale claims, final-CAS failures, and identity collisions roll the
   entire bundle back. If pure materialize planning is empty or raises, the source Activity remains `retry_wait` and
   retains artifact refs, but writes zero EntityDelta and zero child; the successful later attempt records its own
   single delta. A post-commit acknowledgement loss accepts success only after exact bundle replay validation.
   D1m opts its root, source, and materialize drains into expired-`claimed` recovery. Active claims remain protected;
   after expiry a new physical claimant must win both the returned `mark-running` fence and a PostgreSQL-clock-checked
   exact command guard before it can create or update Activity rows. Guarded start locks the deterministic ActivityRun
   plus every deterministic ActivityAttempt identity through the current command attempt, then exact-validates primary
   ids, idempotency keys, workspace, command/activity links, workflow/operation/type/owner/provider fields, current
   attempt/lease, and nonblank owner metadata. Split primary/key identities, alternate nonterminal rows, current/future
   terminal execution Activity/Attempt rows, and future or malformed resume-control attempts fail closed before writes. A fully exact
   prior terminal execution attempt remains immutable and may coexist. A succeeded owner-specific resume-control
   terminal may coexist only under its deterministic `owner_specific_resume` id/key, with attempt generation no greater
   than the current command attempt and exact workspace/activity/workflow/command/provider/request-ref/lease fields;
   its input/output must also exact-bind `resume`, target company/key, boolean `force`, and a nonblank output reason.
   A successful takeover first closes every exact superseded prior `running` execution attempt as `owner_lost`, then
   starts the exact current attempt; `CommandKernel` accepts the returned spine only when both ActivityRun and current
   ActivityAttempt are exact `running` rows for that command and physical claim.
   The old claimant therefore cannot resume into an Activity or domain effect after takeover. If an exhausted final
   source attempt expires, a D1m-only PostgreSQL closure uses the database clock as the sole lease-expiry authority,
   locks that same deterministic Activity/Attempt set plus the command claim, repeats the full immutable validation,
   and fails the Command, exact ActivityRun, and any exact prior/current running execution attempts in one transaction.
   An exact current failed owner-loss partial is accepted and converged only when
   `error.reason == metadata.owner_lost_reason == output.reason` is nonblank, `output.status=skipped`,
   `error.owner_lost=true`, and `error.deterministic_terminal_failure=false`. A valid owner-specific resume-control
   Attempt is retained unchanged; future or malformed resume evidence conflicts. This also closes a deterministic
   `queued|retry_wait` Activity handoff left before the next guarded start. Any split identity, semantic mismatch,
   alternate row, invalid terminal proof, or still-active database lease rolls the whole closure back with zero
   Activity/Attempt/Command write. The shared claim API's global default and attempt-budget behavior are unchanged.
6. **Materialization owner:** the materialize command carries the exact parent-derived target, run id,
   operation/action links, workspace/company identity, `max_assets`, and `seed_url_only` mode. Its preflight reloads the
   complete canonical action input rather than trusting a lossy phase-local copy, then repeats the persisted
   request/target/causality comparison before canonical `CompanyAsset` / `CompanyEvidence` writes. It reconstructs the
   exact bounded model-safe snapshot frozen in the completed run. Snapshot schema
   `company_public_web_run_snapshot_v3` binds `discovered_assets + summary + artifact_paths +
   artifact_publication_sha256 + source_projection_revision + source_projection_completed_at + started_at +
   completed_at` under one SHA-256. The terminal timestamp is frozen before snapshot hashing and reused for the run
   finalizer, so materialization never consumes a later unbound clock value. The positive source-projection revision
   allocated by the durable owner is the primary latest-winner order for both PG and memory; explicitly defined
   timestamp/run-id fallback applies only to brownfield rows without a valid revision. Canonical asset and evidence
   upserts run in one exact-claim PostgreSQL transaction, and `updated_at` uses monotonic `GREATEST` semantics;
   mutable source-asset rows are not materialization authority, so a later same-URL/source-family run or a post-plan
   summary/artifact mutation cannot replace an earlier run's payload, provenance, or lineage. Asset upsert returns the
   exact payload written by the current run instead of rereading a row that a concurrent run may already have changed.
   A non-reclaimed joined `running|failed` row still fails closed in the orchestrator and is never promoted to
   completed. Repair of a completed typed source-command run derives mandatory deferred materialization from the
   authenticated physical source-owner request; mutable stored `company_asset_sync_policy` cannot switch that repair
   back to inline canonical writes.

The Operation target workspace scopes authorization and control-plane ownership only. Canonical `CompanyAsset` and
`CompanyEvidence` rows remain shared company facts in `workspace_id=default`, with the existing source-asset-derived
canonical IDs. Authenticated workspaces therefore converge on one shared canonical fact rather than creating private
duplicates; the standard shared reader continues to see the result.

Any mismatch fails closed before that stage's new domain effect. ActivityRun/Attempt/EntityDelta and terminal sync
remain linked through the command's physical `operation_id` and payload operation/action ids; this linkage is not a
claim that all stages or their terminal synchronization form one global exactly-once transaction.

## Owner and effect matrix

| Boundary | Canonical owner | Allowed effect after exact preflight | Explicit non-claim |
| --- | --- | --- | --- |
| request + target | `ActionRequestSpec` + `CompanyPublicWebTargetBinder` | persist canonical AgentAction/OperationRun request pins | no model/tool serving |
| dispatch | Operation adapter | plan `company.public_web.refresh` only | no inline refresh/provider/domain write |
| root command | `company_public_web_owner` | plan `company.public_web.source.collect` | no source or canonical asset effect |
| source phase | company Public Web source owner | atomically create/join/reclaim the command-attempt-owned source run; write source-specific rows/artifacts with canonical sync deferred; atomically commit plan event + materialize child + source-run EntityDelta + exact parent terminal CAS | no canonical company fact sync; source run/artifact publication precedes the completion bundle and linked Activity/Operation sync follows it |
| materialize phase | company Public Web materialization owner | verify the completed run's v3 assets/summary/artifact/timestamp envelope and atomically sync its exact-claim canonical rows into shared `workspace_id=default` PG-only `CompanyAsset` / `CompanyEvidence` | Operation workspace is authorization, not canonical-fact tenancy; no global command/effect/terminal UoW closure |

## Registry and residuals

- The D1m candidate registry partition is **10 schema-defined / 5 schema-less / served=0**.
- R-029 remains open for the other **5/15** API-submittable production actions. The empty/empty compatibility bridge,
  release-window hit audit, separate validation of the `NOT VALID` checks, and full-population deletion condition are
  unchanged.
- R-019 remains open. D1m closes source-run first-create, split-identity, same-command retry reclaim, exhausted-source
  Command/Activity/Attempt closure, successful-takeover cleanup of superseded running attempts, and the source
  completion event/child/delta/parent-terminal race. It still does not place the earlier source-row/artifact publication
  or later Operation/action sync in that UoW or add a global command-generation fence. The current direct state-sync
  caller ratchet is **24**; historical D1i/D1l/D-3 checkpoints that recorded **26** remain historical evidence. The new
  blocking advisory-lock owner still inherits R-019's typed-busy/overall acquisition-deadline residual for a permanent
  lock holder.
- Served Agent tool population remains **0**. D1m does not provide this action's populated revisioned model-safe result
  spec, result serializer/simulate mapping, or complete served predicate; shared result/registry infrastructure alone
  therefore cannot make the action callable by a model.
- Pre-D1m binaries can still emit revisionless source rows and use the old generic canonical writer. This candidate is
  therefore not rolling-overlap compatible. Before hosted activation, operators must use a quiesced single-version
  cutover or land a separately reviewed dual-write/compatibility bridge; that gate is tracked under R-019/NEXT_TODO.

## Evidence and review status

Latest fixed-forward author validation:

```text
D1m three-file request/owner/effect suite:      68 passed + 15 subtests
Operation Company Public Web selection:         52 passed + 61 subtests
full migration runner:                         24 passed + 65 subtests
```

Ruff check/format, changed-file compile, and diff checks are green. Global mypy remains exactly at the accepted
ceiling, **81 errors / 4 files**. The earlier broad candidate evidence remains **219 passed + 209 subtests** for the
combined D1 matrix, but it predates this fixed-forward and is not substituted for the exact latest nodes above. These
results are author evidence only. The enclosing D1m implementation commit and a fresh pinned non-author review
artifact remain pending at this point; no independent verdict is asserted.
Fake/scripted and later live validation remain outside D1m, and no provider/model/live environment is used or
authorized here.
