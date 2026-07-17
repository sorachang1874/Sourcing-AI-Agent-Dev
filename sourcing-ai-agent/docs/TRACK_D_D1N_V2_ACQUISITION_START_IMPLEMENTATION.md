# Track D D1n V2 — exact-preview acquisition start contract

> Status: Current non-live implementation candidate (2026-07-17). Author evidence only; fresh pinned non-author
> review is pending. This leaf is not registered, dispatched, persisted, or served, performs no provider/model work,
> and keeps the public Agent tool population at zero.

## Outcome

V2 defines the closed `start_acquisition_run` successor without changing the historical v1 action contract. A caller
may supply only the immutable `(preview_id, preview_revision, preview_digest)` reference. Workspace/requester scope,
the complete canonical preview, request/result/tool pins, and the start snapshot are bound by the integration owner.
Inline company, Cohort, source, coverage, manifest, or budget overrides are invalid.

The exact owner reader requires all five identity axes:

```text
workspace_id + requester_id + preview_id + preview_revision + preview_digest
```

Missing, foreign, stale, expired, corrupt, or conflicting previews collapse to
`acquisition_start_preview_not_found_or_conflict` before an action, run, command, budget, event, provider, or model
write. The binder also verifies that the preview was compiled for the exact v2 request-schema digest; historical v1
previews are never reinterpreted.

## Approval and root-command contract

Approval re-reads the same exact preview and compares it with the complete persisted pending action. Only an
authenticated user or open-mode operator may mint `acquisition_confirmation_receipt.v1`; a model or service actor
cannot approve itself. The receipt binds:

- actor, action, workspace, requester, approval policy, and timestamp;
- exact preview id/revision/digest and complete start-snapshot digest;
- effective-request, canonical company, canonical Cohort, provider-manifest, and budget identity;
- request, result, serializer, interpretation, and tool-spec pins.

The v2 root command contains no free-text re-inference seam. It exact-copies the full approved start snapshot and the
receipt id/digest, and rejects a rehashed receipt whose bound identity differs from the snapshot.

## Result and product semantics

The leaf supplies one F1-compatible, revisioned, model-safe result spec with closed `success`, `deferred`, and `error`
variants. Successful output identifies the accepted action/run/command, exact preview, and exact confirmation
receipt. Canonical role/status multi-select values round-trip unchanged, including empty roles as all roles,
`current|former`, and `any|all` matching.

## Explicit boundaries

- The leaf has no public registry, Agent serving, dispatcher, storage writer, provider client, or model client.
- The future integration UoW must run exact preview/action approval preflight in the same transaction before creating
  the OperationRun, budget reservation, root command, or approval event.
- F4a currently persists commandless plan previews only; approval receipt/event and start-run UoW persistence remain
  the next integration-owner batch.
- Historical `acquisition_root_request_v1` stays in the existing ActionRegistry for old rows only. This batch does not
  activate v2 or mutate that registry.
- A scope-matched independent review and release evidence remain mandatory before served/live activation.

## Author validation

- focused V2 closed request/owner/approval/command/result matrix: `45 passed`;
- V1/F1/F3/acquisition-root adjacency: `423 passed + 56 subtests`;
- scoped Ruff, format, Python compilation, and mypy (`0 issues`): green.

These are author results, not an independent-review verdict or a live-provider authorization.
