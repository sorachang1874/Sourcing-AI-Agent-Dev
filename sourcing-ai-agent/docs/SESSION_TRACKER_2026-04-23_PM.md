# Session Tracker 2026-04-23 PM

> Status: Closed on `2026-04-24`. The immediate stabilization pass from the `2026-04-23 20:00+` thread is now considered closed. Long-horizon follow-ups were promoted into `docs/NEXT_TODO.md`; this file remains as a historical closure record, not an active backlog.

## Scope

This tracker consolidates issues and follow-up actions discussed from roughly `2026-04-23 20:00` onward, plus directly related tails that were still active in the same stabilization thread.

Goal:

- avoid losing PM-session conclusions after context compaction
- distinguish `fixed` / `partially fixed` / `still open`
- keep one explicit checklist for the remaining cleanup instead of scattering notes across chat turns

## Non-Regression Rules Confirmed In This Window

- `Harvest profile-search` must remain the default company-scoped current/former recall lane for:
  - `full_company_roster` former lane
  - `scoped_search_roster` current/former lane
- `Harvest profile-search` must not be used for targeted exact-person / scattered-member lookup by raw person name.
- no generic “high-cost source” toggle may decide core roster/profile-search behavior.
- external semantic augmentation should keep retiring from the default product path, not regrow as a user-facing operator knob.
- `replay` is cache-only; cache miss must not create `_offline` members or pollute live shared cache.
- ECS/live runtime must stay `PG-only` and must not silently drift back onto disk SQLite.

## Fixed In This Window

### 1. Harvest live hydration batching is no longer simple serial waterfall

- `src/sourcing_agent/enrichment.py`
  - moved to bounded-window submission
  - batch sizing now adapts to URL count and source mix
  - live mode can use smaller balanced batches instead of a few oversized chunks
- `src/sourcing_agent/company_asset_completion.py`
  - background/live profile completion now reuses the same adaptive live fetch window
  - no more fixed `150 x 1 worker` serial completion for company-asset hydration
- regression coverage updated in:
  - `tests/test_enrichment.py`
  - `tests/test_company_asset_completion.py`

### 2. One-party Markdown now has explicit status headers and a guard test

- status banners were added across first-party Markdown
- new guard:
  - `tests/test_markdown_status.py`
- key current docs were also corrected for:
  - PG-first control plane
  - replay cache-only semantics
  - hosted restore using `control_plane_snapshot + company_snapshot`
  - Harvest batching/current lane semantics

### 3. `replay` fake-member pollution was stopped and ECS was put back on live provider mode

- `_offline` provider bodies no longer leak into shared live caches
- Surge AI replay pollution was cleaned out from ECS runtime/PG/provider cache
- ECS backend was restarted in `SOURCING_EXTERNAL_PROVIDER_MODE=live`

### 4. ECS hosted backend is back on explicit PG-only bootstrap

- ECS service env now injects:
  - `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
  - `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
  - `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
- old SQLite-live service drift path was removed from the active unit

### 5. Frontend same-origin proxy is live via Pages Functions

- `demo.111874.xyz/api/*` now proxies to hosted backend through Pages Functions
- this replaced the half-working split Worker route approach

### 6. 2026-04-24 closure pass: authoritative board source, browser hydration consistency, and smoke stage digest are now stable

- `src/sourcing_agent/orchestrator.py`
  - authoritative snapshot candidate boards no longer let a non-patch job overlay override the snapshot baseline
  - this closes the `xAI`/large-baseline class where `/dashboard` showed the correct first page but `/candidates?offset=0` drifted to a different prefix
- `frontend-demo/src/lib/api.ts`
  - dashboard candidate-page merge is now offset-aware instead of pure append/dedupe
  - summary dashboards that arrive with an incomplete first slice now proactively backfill page 0 before hydration continues
- `src/sourcing_agent/workflow_smoke.py`
  - canonical stage digest now synthesizes missing provider stages like `public_web_stage_2` from later completed stages
  - provider smoke exports and hosted smoke assertions no longer flap on “stage summary omitted but final stage completed” cases
- fresh verification completed in this closure pass:
  - `tests/test_hosted_workflow_smoke.py` targeted authoritative-snapshot overlay regression
  - `tests/test_workflow_smoke.py`
  - `tests/test_hosted_workflow_smoke.py`
  - `tests/test_scripted_test_runtime.py`
  - `tests/test_frontend_browser_e2e.py`
- outcome:
  - the previously failing large-org browser restore/hydration case is now green
  - fast browser suite is green
  - relevant smoke/runtime suites are green

## Closure Notes

### 6. Results/API slimming

Done earlier today and still valid:

- `/api/jobs/{job_id}` default is summary-first
- `/api/jobs/{job_id}/results` default payload is lighter than before

Still open:

- default summary-first behavior remains in place
- longer-horizon caller audits stay tracked in `docs/NEXT_TODO.md`

### 7. Background prefetch submit parity and backend stage timing are now materially improved

Completed in the latest pass:

- background Harvest prefetch submit is no longer pure sequential chunk dispatch
- request normalization now preserves numeric runtime-tuning overrides that drive the shared throughput contract
- backend stage summary normalization repairs missing `started_at/completed_at` for representative stage records before smoke consumes them
- fresh isolated smoke was rerun and written to `output/scripted_smoke_v4/`:
  - `openai_reuse`
    - `reuse_snapshot`
    - total about `1.51s`
    - `job_to_board_nonempty` about `1.02s`
  - `google_scoped_cold`
    - `delta_from_snapshot`
    - total about `5.14s`
    - `job_to_board_nonempty` about `1.08s`
  - `xai_live_roster`
    - `new_job`
    - total about `18.83s`
    - progress maxima:
      - `queued_worker_count=3`
      - `waiting_remote_harvest_count=3`
      - `pending_worker_count=3`
- `provider_case_report.stage_wall_clock_ms` / `provider_case_report.workflow_wall_clock_ms` now show up in those representative reuse/delta/live cases instead of staying empty

This item is now considered closed for the `2026-04-23 PM` stabilization tracker:

- the remaining work here is no longer a same-thread regression tail
- any broader timing/reporting/productization work now lives in `docs/NEXT_TODO.md`

## Promoted To NEXT_TODO

The items below are no longer treated as “keep the 2026-04-23 PM tracker open until fixed”. They were promoted into the long-horizon backlog because they are productization or performance programs rather than unresolved stabilization regressions:

### 8. Generic high-cost toggle removal is now complete, but the policy simplification work is not

Completed in this pass:

- the old generic toggle has now been removed from:
  - execution preference normalization
  - review decision schema / UI contract
  - planning / review / orchestrator active paths
  - smoke helpers and current regression fixtures
- the derived `high_cost_requires_approval` policy tail was removed with it

What still remains is broader policy cleanup, not the field itself:

- scoped/full company lanes still need one shared execution-policy contract that clearly says:
  - company-scoped current/former recall uses Harvest roster/profile-search by default
  - targeted exact-person lookup does not
  - public-web expansion is additive and off by default unless a separate strategy enables it

### 9. Runtime namespace isolation is not yet the final shape

Current gap:

- `live / replay / simulate / scripted` are semantically separated
- but not yet fully isolated by runtime namespace / DB namespace / cache namespace

Needed end-state:

- separate runtime roots or equivalent hard isolation for:
  - provider cache
  - company assets
  - hot cache
  - jobs/history
  - control-plane DB namespace

### 10. Background Harvest prefetch still needs one final in-flight budget/backpressure layer

The old “pure sequential submit” gap is no longer the main problem. What remains is the last global control layer:

- foreground/background now share most of the runtime-tuning shape
- but provider-safe in-flight budget and backpressure are still inferred mostly from chunk sizing/window caps
- this should still be unified further so scheduler, foreground hydration, and background prefetch do not drift again

### 11. Provider-grade workflow timing/reporting is improved, but not final

Still missing or incomplete:

- browser-level regression for “board hydration keeps current page and scroll position”
- a clean, automated large-snapshot cold-build benchmark report in standard smoke output

### 12. Cold materialization path is still too heavy

Latest confirmed benchmark in this thread:

- Google cold full build, ~`5897` candidates, foreground fast build:
  - total about `14.4 min`
  - biggest hotspots are still:
    - `prepare_candidates`
    - `payload_build_total`
    - `view_write_total`

Implication:

- the next highest-value optimization is still candidate-artifact CPU/JSON/materialization work
- not another shallow pass on PG merge alone

### 13. Excel intake rollout blocker closed later

Later closure note:

- Closed on `2026-04-24` in the active tracker.
- The homepage entry is enabled again by default, same-origin upload has local dev/preview proxy coverage, and a real-browser Excel upload regression covers multi-company split-job creation.

Historical status at `2026-04-23 PM` tracker close:

- homepage entry hidden by default
- backend/import logic is not the blocker anymore
- real-browser upload path / same-origin connectivity is still not stable enough for public exposure

Before re-enable:

- stable browser upload from localhost/127.0.0.1
- multi-company split-job flow verified outside Playwright-only happy path
- proper history/progress/results visibility for intake-created jobs

### 14. Worker/progress/results visible-state consistency still needs one more audit pass

This umbrella includes the user-visible class of bugs seen earlier in the same day:

- workers completed but timeline/progress did not advance correctly
- stage counts temporarily regressed to `0/0`
- results page waited on hidden post-stage work
- board hydration lagged or showed stale/empty intermediate state

Some of these were fixed earlier in the day, but this class should remain tracked until one broader regression pass confirms:

- authoritative counts match across progress/results/dashboard
- no hidden blocking finalize remains after preview/final results
- worker completion is reflected promptly in visible stage state

### 15. Semantic keyword contraction and lane projection still need one upstream contract

This remains open because the right fix is not downstream keyword patching:

- direction terms such as `Multimodal` should remain user-faithful unless the user explicitly asked for sibling concepts
- `Text` / `Vision` / `vision-language` style widening should be controlled at semantic-brief / task-projection boundary, not by late scattered rewrites
- current docs/tests should eventually assert:
  - user-visible plan keywords stay faithful to the request
  - downstream provider query families can expand for recall only when that expansion is explicit and explainable

### 16. Former-lane completeness and provider dispatch need one end-to-end regression contract

Still open from tonight's hosted debugging:

- `full_company_roster` and `scoped_search_roster` must not silently skip former-member Harvest profile-search
- a worker/checkpoint path must not report “completed” if no actual provider call was dispatched for the required lane
- progress refresh must not regress visible candidate counts after a provider batch has already been observed

### 17. Hosted smoke teardown got a first-pass shutdown fix, but the full matrix is still pending

Fixed in this pass:

- hosted harness teardown now also waits for:
  - `background-outreach-layering-*`
  - `background-snapshot-materialization-*`
- representative hosted smoke cases now pass in the same pytest process without the earlier teardown segfault

Still worth finishing later:

- rerun the wider hosted/browser matrix on top of this harness change
- keep thread shutdown policy explicit instead of relying on thread-name prefixes growing ad hoc

## Immediate Next Steps

1. Run focused regression for:
   - review instruction compile
   - planning modules
   - pipeline plan/review/stage2 endpoints
   - hosted smoke / workflow smoke helpers
   - frontend build
2. Propagate the latest status from this tracker back into:
   - `PROGRESS.md`
   - `docs/NEXT_TODO.md`
3. Continue on the next highest-value technical tails:
   - runtime namespace isolation
   - background prefetch bounded-window parity
   - workflow-level timing/report output
   - semantic-brief to provider-query-family contract
   - former-lane end-to-end dispatch guarantees
   - hosted/browser full-matrix rerun on top of the new harness shutdown fix
