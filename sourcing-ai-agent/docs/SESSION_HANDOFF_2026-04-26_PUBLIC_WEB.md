# Session Handoff 2026-04-26 Public Web Search

> Status: Current handoff for continuing target-candidate Public Web Search productization. Read this with `../PROGRESS.md`, `NEXT_TODO.md`, `PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md`, and `FRONTEND_API_CONTRACT.md` before changing code.

## Real Goal

Productize the old optional `Public Web Stage 2` as a target-candidate page batch action:

- The default sourcing workflow stays single-stage and does not run Public Web Search.
- Users add people to `target_candidates`, then explicitly select target candidates and trigger `Public Web Search`.
- The system writes durable, reusable, auditable person-level public-web assets keyed by normalized LinkedIn URL when available.
- Raw HTML/PDF/search payloads are internal analysis inputs and excluded from default exports.
- Public-web emails can be high-confidence candidates, but `target_candidates.primary_email` must not change until a manual promotion record has been written first.

## What Has Been Built

### 1. Storage cleanup prerequisite

- Product-facing SQLite snapshot import/export/restore paths were retired before adding Public Web persistence.
- New Public Web tables use the PG-authoritative control-plane schema/live registration path.
- Remaining SQLite behavior must stay migration-only or ephemeral shared-memory shadow only.
- LinkedIn URL normalization is storage-neutral in `src/sourcing_agent/linkedin_url_normalization.py`; new modules should not call identity helpers through a store facade.

### 2. Method calibration / experiment harness

`src/sourcing_agent/public_web_search.py` contains the candidate-level method work:

- source-family query planning
- DataForSEO batch/queue search
- entry-link classification/ranking
- source-aware fetch slicing
- deterministic email extraction and suppression
- candidate-level AI adjudication payloads
- sanitizer that blocks model-invented emails and unknown URLs
- academic summary extraction from Scholar/publication/homepage slices
- artifact-only CLI: `run-target-candidate-public-web-experiment`
- configurable AI evidence budget: `--max-ai-evidence-documents` / `PublicWebExperimentOptions.max_ai_evidence_documents`
- candidate-level LLM adjudication sees source-balanced `entry_links` plus explicit `search_evidence` from DataForSEO URL/title/snippet/query/rank/provider context, not only fetched document evidence

Important empirical runs:

- `live-dataforseo-batch-entry-discovery-10x4-ai-skip`
  - 10 candidates, 40 queries, DataForSEO queue path, about 88 seconds.
  - Confirmed batch/queue is much faster than synchronous search.
- `live-dataforseo-entry-discovery-10x10-social-links`
  - 10 candidates, 100 queries.
  - High recall for GitHub/Scholar/homepage; X/Substack links remain noisy and need identity adjudication.
- `replay-fetch-homepage-email-4-candidates`
  - Extracted `cbfinn@cs.stanford.edu` and `dainves1@gmail.com` from homepage/CV evidence as promotion-recommended email candidates.
- `live-dataforseo-fetch-scholar-academic-summary-3x7`
  - Confirmed Google Scholar HTML can include publication rows.
  - Parser was fixed to be attribute-order independent for `gsc_a_at` anchors.
  - Qwen produced useful academic summaries after source-aware slice rebuild.

Known calibration cases:

- Noah Yonack GitHub profile can expose `noah.yonack@gmail.com` in a rendered/authenticated view, but current unauthenticated fetch did not capture it. Track this for GitHub API/browser-backed profile extraction or better rendered fetch.
- Company-page queries are low-ROI for candidate email discovery and should stay out of target-candidate default fetch.
- Direct resume/CV and generic email/contact search are noisy; homepage/Scholar/GitHub/publication fetch should produce the email evidence first.

### 2.5 Quality evaluation gate

A quality layer now exists and should remain the gate before expanding export/promote semantics beyond the current promoted-only default:

- Module: `src/sourcing_agent/public_web_quality.py`
- CLI: `evaluate-public-web-quality`
- Inputs: experiment directories or direct `signals.json` paths
- Outputs: JSON report, signal CSV, Markdown summary
- Checks:
  - email source URL/family/evidence/publishability/promotion status/trusted identity
  - X/Substack/GitHub/Google Scholar trusted vs needs-review counts
  - GitHub repo/deep links, X utility/post links, Substack non-profile pages, non-profile Scholar URLs, and search-only/unreviewed identity
- Useful commands:

```bash
PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality \
  --experiment-dir runtime/public_web/experiments/replay-fetch-homepage-email-4-candidates \
  --output-dir runtime/public_web/quality/replay-fetch-homepage-email-4-candidates \
  --summary-only

PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality \
  --experiment-dir runtime/public_web/experiments/live-dataforseo-entry-discovery-10x10-social-links \
  --output-dir runtime/public_web/quality/live-dataforseo-entry-discovery-10x10-social-links \
  --summary-only
```

Observed quality conclusion:

- Replay homepage/CV sample: two promotion-recommended emails had source URLs/evidence and produced no quality issues.
- Live 10x10 social discovery sample: media/profile recall was high, but search-only X/Substack/GitHub/Scholar links were noisy and remained untrusted; detail UI must show these as review-needed until fetch + AI identity adjudication upgrades them.
- Live fetch + AI Scholar/academic sample: trusted media links did appear after evidence fetch/adjudication, but many links remained review-needed or ambiguous; the report also caught legacy non-canonical model labels such as `scholar_profile`/`github_profile`, now normalized and flagged for audit.
- Larger live quality pass `live-public-web-quality-11x14-fetch12-ai16`: 11 candidates, 154 queries, 131 fetched docs, 12 email candidates, 2 promotion-recommended emails, 94 trusted media links. It exposed a fetch-queue bug: discovered GitHub links were front-inserted and starved X/Substack/Scholar fetch diversity. GitHub discovered links are now appended; retest 12 fetches/candidate before raising to 16.
- Fetch diversity sanity `live-public-web-quality-diversity-fix-3x14-fetch12-ai16`: 3 candidates, 33 fetched docs, 7 email candidates, 3 promotion-recommended emails. Fetched docs covered homepage, resume, Scholar, GitHub, X, Substack, publication, and academic profile. Keep 12 fetches/candidate for product-cap validation; use 16 only for exploratory stress tests.
- Search evidence check `live-public-web-quality-search-evidence-3x14-fetch12-ai16`: 3 candidates, 248 signals, 8 email candidates, 3 promotion-recommended emails, 39 trusted media links under the old identity-only media count. X/Substack changed from mostly `unreviewed` to model-adjudicated labels after source-balanced DataForSEO `search_evidence`; one high-risk email issue remained.
- URL-shape hardening follow-up: shared URL-shape warnings now mark X/Substack/GitHub/Scholar non-profile/deep links. Re-running the same quality artifact gives 9 trusted media links under the clean-profile publishable definition; dirty X/Substack deep links remain evidence/review signals.
- Runtime-efficiency follow-up: candidate analysis supports bounded concurrent URL fetches, and the experiment CLI supports bounded concurrent candidate finalization/LLM adjudication. DataForSEO queue wait remains external tail latency.

### 3. Backend productization slice

New service boundary:

- `src/sourcing_agent/target_candidate_public_web.py`

New PG-authoritative tables:

- `target_candidate_public_web_batches`
- `target_candidate_public_web_runs`
- `person_public_web_assets`
- `person_public_web_signals`
- `target_candidate_public_web_promotions`

New API:

- `POST /api/target-candidates/public-web-search`
- `GET /api/target-candidates/public-web-search`
- `GET /api/target-candidates/{record_id}/public-web-search`
- `GET/POST /api/target-candidates/{record_id}/public-web-promotions`
- `POST /api/target-candidates/public-web-export`

Backend behavior:

- POST is intentionally light: creates/join idempotent batch and per-candidate runs, queues recoverable workers, and returns.
- It does not run live DataForSEO/fetch/LLM inside the request thread.
- Per-candidate runs are the source of truth; batch rows are progress aggregates only.
- Worker lane is `exploration_specialist`.
- Worker metadata uses `recovery_kind=target_candidate_public_web_search`.
- Search checkpoint persists query manifest, provider task IDs, poll count, query results, classified links, and errors.
- Recovery takeover should poll/fetch existing provider tasks instead of resubmitting.
- Completed runs with a normalized LinkedIn URL key upsert `person_public_web_assets`.
- Signal rows are first-class, model-safe detail/export inputs.
- Promotion rows are written before any Public Web email updates `target_candidates.primary_email`.
- Public Web export defaults to promoted-only model-safe signals/evidence/promotions/manifest and excludes raw HTML/PDF/search payloads.

### 4. Frontend first slice

Another session added the first target-candidate page integration and this session reviewed it.

Files:

- `frontend-demo/src/components/TargetCandidatesPanel.tsx`
- `frontend-demo/src/lib/api.ts`
- `frontend-demo/src/types.ts`
- `frontend-demo/src/styles.css`
- `contracts/frontend_api_contract.ts`
- `contracts/frontend_api_contract.schema.json`
- `contracts/frontend_api_adapter.ts`

Current frontend behavior:

- Checkbox selection on target candidates.
- `Public Web Search` button submits selected `record_ids`.
- State refresh button and 5s polling while any run is non-terminal.
- Card-level status chip, counts, recommended email count, and primary links from run summary.
- Public Web detail section calls `GET /api/target-candidates/{record_id}/public-web-search`.
- Detail shows email candidates and profile/evidence links with identity labels, publishability/suppression state, source links, and URL-shape warning chips.
- Frontend consumes `GET/POST /api/target-candidates/public-web-search`.
- Browser state is selection/cache only; it is not the Public Web truth source.
- The UI promotes Public Web emails/clean profile links through `POST /api/target-candidates/{record_id}/public-web-promotions`; it does not mutate `primaryEmail` directly.
- The UI has a dedicated Public Web export button backed by `POST /api/target-candidates/public-web-export`, defaulting to promoted-only model-safe data.
- Browser E2E now covers both selected-candidate search trigger/polling and completed-run detail promotion -> promoted-only export download.

## Cross-Session Review Result

The other session's frontend/API work is directionally aligned with the product contract:

- It keeps Public Web Search user-triggered, not a default workflow stage.
- It uses backend API state, not localStorage/runtime files, as the source of truth.
- It keeps export separate and does not include raw HTML/PDF.
- It implements email/link promotion through the promotion persistence/API path.
- It does not claim company-level Public Web refresh is done.

No code rollback is recommended from this review.

## Remaining Gaps

These are expected gaps, not regressions:

- No company-level Public Web API/CLI lane yet.
- Promotion override/reason workflow for non-publishable signals is not implemented.
- Public Web export mode controls are backend/API-level; product UI currently defaults to promoted-only.
- Current frontend polls up to 500 runs globally; this is acceptable for v1 but should become scoped/filtered as the target-candidate pool grows.
- Target-candidate card editing/navigation still needs a redesign. The old "查看历史记录" jump was hidden because it routes through the candidate detail page and returns to the wrong board; remarks/comment editing is still a later UX task.

## Local Runtime State On 2026-04-26

The local frontend warning `Public Web Search 接口暂不可用，请确认后端服务已更新并重启。` is expected if the backend on port `8765` is still the old process:

- The observed `8765` process was started on `2026-04-25` before these routes existed.
- `GET /api/target-candidates` returned the local 11 target candidates.
- `GET /api/target-candidates/public-web-search?limit=5` returned `404 {"error":"not found"}` from that stale backend.
- The local control-plane state observed for those 11 target candidates had `target_candidate_public_web_runs = 0` and `target_candidate_public_web_batches = 0`; the new promotions table is created by the updated schema bootstrap after the backend restarts on current code.

So the card text `未开始 / 尚未触发候选人级 Public Web Search` is the accurate state for the current local 11 candidates. The toolbar export label that says it will export 11 candidates describes the selected/filter scope, not "11 candidates already have confirmed Web Search results". Promoted-only Web Search export will only contain manually promoted Public Web email/link signals when such promotions exist.

## Recommended Next Sequence

1. Restart the local backend/worker on the current branch and verify the new route exists.
   - Run `bash ./scripts/dev_backend.sh --print-config` first if DSN/runtime selection is unclear.
   - After restart, verify `GET /api/target-candidates/public-web-search?limit=5` no longer returns 404.
2. Trigger a small Public Web Search batch from the target-candidate page.
   - Start with 2-3 selected candidates and confirm state moves through queued/searching/analyzing/completed or needs_review.
   - Inspect the detail section for source URLs, identity labels, URL-shape warnings, and publishability before promotion.
3. Run or review the Public Web quality gate.
   - Use existing replay/live artifacts first if live provider/model budget is unavailable.
   - For a broader live pass, use `--limit 12-15`, `--max-queries-per-candidate 14-16`, `--max-results-per-query 20`, `--max-fetches-per-candidate 12`, `--max-ai-evidence-documents 16`, and `--ai-extraction on|auto`.
4. Continue hardening promotion/export quality.
   - Add explicit override reason flow before allowing non-publishable signals to be promoted.
   - Add product controls if users need `promoted_and_publishable` export mode.
5. Add cancellation/retry and company-level Public Web later.
   - Company lane stays lower priority and API/CLI-only until candidate-level ROI is stable.

## Validation Already Run

From this review session:

```bash
cd frontend-demo && npm run build
PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/api.py src/sourcing_agent/orchestrator.py src/sourcing_agent/worker_daemon.py tests/test_target_candidate_public_web.py tests/test_results_api.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web_api_queues_idempotent_runs' tests/test_target_candidate_public_web.py tests/test_markdown_status.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback' tests/test_markdown_status.py
PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/public_web_search.py src/sourcing_agent/public_web_quality.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py -k 'public_web or target_candidate_public_web or url_shape or bounded_concurrency'
PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-public-web-quality-search-evidence-3x14-fetch12-ai16 --output-dir runtime/public_web/quality/live-public-web-quality-search-evidence-3x14-fetch12-ai16-shape-warnings --summary-only
node --check frontend-demo/scripts/run_target_public_web_promotion_export_e2e.mjs
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_frontend_browser_e2e.py -k 'target_candidate_public_web_selection_trigger_and_polling or target_candidate_public_web_promotion_and_export'
git diff --check
```

Observed results:

- frontend build passed
- ruff passed
- targeted backend/API/Public Web tests passed
- frontend detail build passed
- URL-shape quality re-evaluation passed; trusted media count now uses clean-profile publishability
- markdown status test passed
- browser E2E passed for both selected-candidate trigger/polling and completed-run promotion/export download

## GitHub Handoff Notes

The current handoff target is branch `productization-2026-04-25-stable` on `origin`.

Before any future commit, inspect staged files deliberately and keep runtime/cache/vendor/build outputs excluded. Do not use `git add .` in this repository; use explicit source/doc/test/frontend paths so local runtime assets and generated output do not leak into Git.
