# Track D D1n V1 acquisition-plan preview implementation

> Status: Implementation/decision record (Track D increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

Status: author candidate; fresh pinned non-author review pending. This document is implementation evidence, not a
formal `GO`, and it does not authorize model or provider execution.

## Scope

V1 implements the pure, provider-neutral contract for `plan_acquisition`:

- one closed `acquisition_plan_preview_request_v2` request;
- exact workspace/requester identity validation without rewriting owner identifiers;
- the canonical `cohort_selection.v1` role/status/match object;
- one compiler-owned, capability-free `cohort_provider_manifest.v2`;
- one immutable `acquisition_plan_preview.v2` carrying complete company, budget, schema, and future start-v2 pins;
- one revisioned model-safe result contract with closed success/deferred/error variants.

The leaf performs no storage, model, provider, approval, or release-state work. PG allocation/persistence, the
authenticated binder, start-v2, result-occurrence persistence, and tool activation remain downstream gates.

## Product semantics

Users may freely select any supported role-bucket subset (including research, engineering, or product), one or both
employment statuses (`current`, `former`), and `any` or `all` role matching. An empty role list keeps the canonical
"all roles" meaning. No company or provider adapter supplies hidden role/status defaults.

Thinking Machines Lab and Anthropic traverse the same company-neutral path. Their canonical company-registry data
changes manifest content and digests, not schema, branching, serializer, or release behavior. Harvest-specific syntax
remains inside compiler-owned lanes and this leaf never issues an execution capability.

## Closed review findings

The fixed-forward candidate closes the prior V1 `NO-GO` findings:

1. `manifest_digest` binds the complete canonical company target, all five budget ceilings, and all six request/result
   schema pins; `physical_query_digest` separately names the narrower physical query plan.
2. Thematic constraints must be preserved exactly by the compiler. Role-like, removed, or rewritten constraints fail
   closed and direct role intent to `cohort_selection.role_bucket_ids`.
3. Workspace/requester values are exact owner identities. Server-minted company names/labels must already be
   canonical; order-insensitive label-set ordering alone is canonicalized.
4. Persisted identifiers, versions, digests, timestamps, and terminal reason codes use anchored whole-value checks and
   are semantically revalidated during hydration. Historical v1 values are never reinterpreted as v2.
5. `AcquisitionPlanPreviewError.to_result()` delegates to the sole canonical error shape, exposes no diagnostic
   detail, and uses a bounded machine field path (or `__none__`).

## Author validation

- `tests/test_d1n_acquisition_plan_preview.py`: `130 passed`.
- compiler/runtime/selection adjacency: `82 passed, 71 subtests passed`.
- broader Harvest/planning/scripted adjacency: `190 passed, 2 failed`; the same two exact nodes fail on clean detached
  `8300c9973c5f1e18368da8aa8503a47503aff51a`, so they are not attributed to V1.
- scoped Ruff check/format, Python compilation, and `git diff --check`: green.
- a fake Thinking Machines Lab payload produced six deterministic lanes, a capability-free v2 manifest, and distinct
  physical/planning digests without provider I/O.

## Remaining gates

- F4 commandless PG UoW and immutable preview revision allocation;
- authenticated target binding and exact owner preflight;
- V2 preview-only start request, human approval receipt, and exact manifest recompile;
- result occurrence/terminal-winner pins and PG-backed simulate success;
- populated `AgentToolSpec`, release evidence, and scope-matched independent review.

Until those gates close, the public served population remains zero and live execution is forbidden.
