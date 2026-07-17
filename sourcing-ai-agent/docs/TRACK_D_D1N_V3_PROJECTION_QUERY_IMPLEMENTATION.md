# Track D D1n V3 — projection filter and Operation query contracts

> Status: Non-live contract leaf (2026-07-17), fixed-forward integrated by D1n S1b. The valid
> `4dddd0e..28c2b2e` pinned Ultra review returned `NO-GO 0/6/1/0`; the current author response is implemented but its
> fresh pinned re-review is pending. S1b adds isolated registry pins and a PostgreSQL result-slot adapter but does not
> populate the public/default registry, authorize a served tool, repair state, or call a provider/model/network
> transport; public `served` remains zero.

## Outcome

V3 defines two closed read surfaces while preserving the existing `search_projection` v1 text-search contract
unchanged:

- `filter_projection` v2 binds a canonical `cohort_selection.v1` plus server-owned projection, membership-revision,
  Cohort-registry, and selection-digest pins;
- `inspect_operation` retains exact v1/v2 history and exposes v3 result/query-owner semantics that bind a
  model-visible OperationRun id to authenticated workspace/action/actor context and project only canonical control,
  display, progress, result-readiness, and bounded provenance state.

The request schema remains `inspect_operation_request_v1`; S1b advanced the current result, query owner, serializer,
tool, adapter, and fixture revisions together without rewriting v1/v2 manifests. The fixed-forward readiness owner
treats completed-without-result-ref as `pending/fail_closed`. V3 also omits operator-authored `progress.reason` from
the model result, binds raw progress in the non-model physical fingerprint, and revalidates the complete canonical
control/readiness/policy/provenance semantics at execution and named serialization. The fixed-forward response pins
the V3 physical identity explicitly, rederives the complete registered command policy, requires exact full-stream
command/event topology, and adds a V3-only actor/source audit digest. Its request binder runs before dependency,
schema, connection, owner, or result effects; retained V1/V2 bytes and accepted V2 replay are frozen separately.

Neither surface infers a next command or performs a repair/write. Missing or unproved projection access is one
`projection_not_found` result. Missing or foreign Operation action/run ownership is one `operation_not_found` result.

## Cohort predicate semantics

The projection owner compiles the predicate; the Agent/UI never authors lane membership:

- empty `role_bucket_ids` means all roles and still requires one selected employment-status membership;
- `role_match=any` requires at least one requested role within a qualifying status;
- `role_match=all` requires every requested role for the same candidate within one qualifying status, so roles split
  across `current` and `former` do not satisfy the predicate;
- `employment_statuses` match server-owned lane membership exactly;
- revision, registry, selection, or freshness drift is explicit `stale` with reselection required, never empty success.

Successful results exact-copy selection/planning/execution/result/publication digests, freshness/readiness,
provider-mode/runtime-namespace/cache provenance, requested-lane coverage, lane summaries, counts, pagination, and
explicit truncation.

## Model-safe candidate identity

Canonical projection keys commonly contain scheme-like values such as `linkedin:` and may embed a public URL. F1
correctly rejects those values as model-visible identifiers. V3 therefore keeps the raw `candidate_identity_key`
inside the projection owner and publishes only `candidate_ref`, a SHA-256 reference bound to:

```text
projection_candidate_ref.v1 + projection_id + membership_revision + candidate_identity_key
```

The raw key is not serialized. A later selection-action binder must resolve `candidate_ref` only under the same exact
projection and membership revision; it must not treat the digest as a global person identity.

## Operation query parity

`inspect_operation` validates the exact action/run/workspace tuple before exposing data. Its success result exact-copies
the canonical `operation_run_control_state`, workflow-command control-policy projection, ActionRegistry display
contract, OperationRun progress, result readiness, and bounded event/command provenance. Cross-owner drift,
control-state status/flag/list/reason mismatch, invented policy fields, command-policy mismatch, private fields, and
raw local paths fail closed. Result readiness is derived once from Operation status plus durable result-reference
presence; a schema-valid but semantically forged readiness projection also fails closed. Exact historical occurrences
resolve by version plus digest, never registry order or the current alias.

The query owner is separately identified by owner id, revision, and contract digest for later F3 population. It does
not change the 15-row ActionRegistry denominator.

## Explicit boundaries

- The leaf does not change historical `projection_search_request_v1` or `projection_filter_request_v1` rows.
- `filter_projection` v2 is not yet installed as the current request contract or Agent tool.
- S1b supplies `inspect_operation` physical-owner result persistence and PG terminal fixtures, but public/default
  served population remains zero.
- Candidate-ref lookup/indexing, the two remaining canary physical-owner adapters, release evidence, and the assembled
  local Agent harness remain integration-owner work.
- Scope-matched independent review remains mandatory before hosted/live activation.

## Author validation

- focused V3 Cohort/filter/query/model-safe matrix: `28 passed`;
- Cohort/F1/F3/canonical runtime adjacency: `313 passed + 60 subtests`;
- scoped Ruff, format, Python compilation, and mypy (`0 issues`): green.

These are the original V3 leaf author results. S1b fixed-forward validation and review scope are recorded in
`TRACK_D_D1N_S1B_INSPECT_OPERATION_RESULT_IMPLEMENTATION.md`; neither artifact is an independent-review verdict or
live-provider authorization.
