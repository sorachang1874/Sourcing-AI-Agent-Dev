# Execution Contract Guardrails

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


This document records the non-regression contracts that must stay true across planner, orchestrator, worker recovery, provider integration, and frontend-visible workflow state.

The goal is to prevent a repeat of "the code did not literally roll back, but the behavior rolled back anyway" after later refactors.

## 1. Single Source Of Truth

- Execution semantics must be decided once, then consumed downstream.
- Planner, runtime dispatch, worker recovery, and results assembly must not each re-infer core lane behavior from loosely related fields.
- Shared helpers/contracts must own:
  - baseline reuse vs delta vs fresh acquisition
  - current/former lane requirements
  - provider lane selection
  - batch execution policy

If a new abstraction needs to change these decisions, it must do so by extending the shared contract rather than adding another local gate.

## 2. Core Acquisition Lane Contracts

These are core lanes, not optional enhancements:

- `full_company_roster` / `live_roster`
  - current lane: `harvest_company_employees`
  - former lane: `harvest_profile_search`
- `scoped_search_roster`
  - current lane: `harvest_profile_search`
  - former lane: `harvest_profile_search`
- `former_employee_search`
  - primary lane: `harvest_profile_search`

These contracts must be reflected consistently in:

- plan metadata
- task `intent_view`
- runtime task dispatch
- workflow explain output
- tests that inspect actual provider usage

## 3. No Generic Cost Gate On Core Lanes

The active execution contract must not use a generic cost-approval field to decide whether core roster/profile-search lanes run.

Specifically:

- it must not disable `full_company_roster` former-member `harvest_profile_search`
- it must not disable `scoped_search_roster` current/former `harvest_profile_search`
- it must not be shown as the primary explanation for roster strategy selection

What must not happen again:

- a user-facing primary operator knob
- a catch-all explanation for why a roster strategy did or did not run

What must remain separately gated:

- targeted exact-person / scattered-member high-cost lookups
- publication-lead targeted name search

Those paths must use a dedicated targeted-name contract, not piggyback on a generic cost flag.

## 4. Specialized Contracts Beat Generic Gates

Generic gates such as cost flags, provider modes, or fallback toggles must not override a more specific acquisition contract.

Examples:

- a generic cost gate must not demote `former_employee_search` from `harvest_profile_search` to low-cost web search
- a generic review flag must not cause planner and runtime to disagree on whether a lane is required
- a generic compatibility fallback must not re-enable SQLite/live dual-track behavior in a PG-only path

When specialized and generic logic disagree, the specialized contract wins.

## 5. No Silent Dual-Track Fallback

Temporary compatibility paths are allowed only when explicitly documented. They must not silently take over the live path.

Required rules:

- PG is the live control-plane truth; SQLite must not remain an implicit live fallback
- runtime namespace isolation must separate `live` / `test` / `scripted` / `replay`
- shared provider cache must not leak replay/scripted/offline payloads into live execution
- frontend must consume API contracts, not runtime-file implementation details

If a compatibility path is still present, docs and tests must say so explicitly.

## 6. Harvest Batch Execution Contract

Profile hydration must follow a bounded-window policy:

- no unbounded fan-out that risks provider `429 code_22`
- no accidental serial waterfall where each next batch waits for the previous batch to fully finish and ingest before submit
- batch sizing and concurrency limits must be explicit and testable

When changing batch policy, validate both:

- provider safety
- wall-clock throughput

## 7. Timeline And Results Contract

User-visible stage/timeline semantics must reflect real execution:

- counts shown in progress must come from the same authoritative state used by results
- hidden post-stage work must not make `Final Results` appear complete while results are still unavailable
- preview-ready runs must not regress to `0/0` or stale counts after refresh/recovery

If the backend defers background work, the frontend must show that as deferred/background work, not as completed finalization.

## 8. Required Regression Tests

Every refactor that touches planning, orchestration, acquisition, recovery, or provider integration must preserve these tests or equivalent coverage:

- plan/runtime parity tests
  - same query yields the same lane/provider contract in plan metadata and runtime dispatch
- provider contract tests
  - `full_company_roster` former lane really uses `harvest_profile_search`
  - `scoped_search_roster` current/former lanes really use `harvest_profile_search`
- dual-track isolation tests
  - replay/scripted payloads cannot pollute live provider cache or live runtime assets
- recovery tests
  - queued/running workers that resume from cached completion are persisted as completed
- results/timeline tests
  - preview/result counts stay stable across polling, recovery, and board hydration
- batching tests
  - bounded concurrency is respected
  - no regression to all-serial waterfall for multi-batch profile hydration unless explicitly required by backpressure

## 9. Change Checklist

Before merging changes in this area:

1. Confirm whether the change touches a shared execution contract.
2. If yes, update the shared helper/module first, not only the caller.
3. Add or update a regression test that asserts the user-visible behavior.
4. Check whether explain/progress/results still tell the same story.
5. Document any temporary compatibility path and add a removal TODO.
