# Claude Code Event Workflow Review Prompt

> Status: Archived 2026-06-11. Historical record only — do not treat as active guidance; see `docs/INDEX.md` for current docs. (Previous status: Current handoff prompt. Use this when starting a fresh Claude Code session to review event-level provider workflow design and remaining orchestration risks.)

For the current 2026-05-06 PG-backed manual scripted candidate-board incident, start with `CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md` before using the broader prompt below.

```text
You are reviewing an existing production-oriented recruiting automation / public-information enrichment codebase.

Repository:
/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent

Read first:
- AGENTS.md
- PROGRESS.md latest 2026-05-06 entries
- docs/NEXT_TODO.md Highest Priority
- docs/CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md
- docs/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md
- docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md
- docs/WORKFLOW_PROGRESS_CONTRACT.md
- docs/HARVESTAPI_PLAYBOOK.md section "linkedin-profile-search"
- docs/DATA_ASSET_GOVERNANCE.md section "Snapshot Selection Is Not Shard Membership"

Project context:
The product runs provider-backed sourcing workflows. It searches company/scoped candidate pools, fetches LinkedIn profiles through Harvest/Apify actors, materializes candidate assets, and serves a candidate board with recall filters and outreach layers.

The recent focus was event-level workflow response for scoped search + profile prefetch:
- provider completed events should become local ingest/marker/progress quickly
- next profile actor submit should happen when provider slots are free, without waiting for full materialization
- full materialization/retrieval/layering is downstream writer-budget work
- completed-worker reconcile must be idempotent and marker-driven
- job_result_view must point to the correct current snapshot; old jobs must not be silently repointed without policy

Important incidents already addressed:
- Meta/Google scoped search had actor slot idle windows because next-submit was indirectly blocked by writer lock/full materialize paths
- some profile workers were counted active before real provider limiter slots existed
- completed reconcile could repeat materialize because consumed worker markers were missing
- Google/Gemini current lane was incorrectly skipped by broad baseline reuse
- Harvest profile-search same input can transiently return 0 profiles; now only true 0-result is retried, while missing pages/total drift are degraded/audited
- large snapshot rebuilds were slowed by all-history preloads, raw profile stat/open, and repeated facet/text projection
- stale job_result_view / hot-cache artifacts can make UI serve an old snapshot

Your task:
Perform a code review focused on whether event-level workflow design still has unhandled risks that would only appear during manual/live testing.

Review these modules first:
- src/sourcing_agent/orchestrator.py
- src/sourcing_agent/enrichment.py
- src/sourcing_agent/seed_discovery.py
- src/sourcing_agent/workflow_efficiency.py
- src/sourcing_agent/remote_provider_events.py
- src/sourcing_agent/worker_daemon.py
- src/sourcing_agent/snapshot_materializer.py
- src/sourcing_agent/candidate_artifacts.py
- src/sourcing_agent/api.py

Review questions:
1. Can a remote provider completion still be blocked behind a long job writer lock before next-submit opportunity runs?
2. Can any worker be counted as active remote/provider work before it has a real provider limiter lease or remote run id?
3. Can completed reconcile consume the same search/profile/company-roster worker twice, or rerun full materialize when only marker backfill is needed?
4. Are same-kind sibling workers handled consistently for search-seed, profile-prefetch, company-roster segmented shards, and exploration/public-web lanes?
5. Does scoped search keep current/former lanes independent, parallel where safe, and separately reusable?
6. Are Harvest 0-result retry, degraded page coverage, and provider incomplete states correctly separated across connector, seed discovery, acquisition, progress, and tests?
7. Can stale job_result_view, stale summary candidate_source, or hot-cache bad manifest still cause the candidate board/layering to serve an old snapshot?
8. Are workflow_efficiency metrics strong enough to expose provider slot idle windows, next-submit delay, repeated reconcile, and materialize duplication before manual UI testing?
9. Are there request-hot-path operations that still scan large company asset history, raw profile JSON, or full candidate docs?
10. Which missing regression tests would have caught the latest incidents earlier?

Expected output:
- Findings first, ordered by severity, with file/line references.
- For each finding, explain the failure mode, why current tests may miss it, and the smallest durable fix.
- If no code changes are needed, say so explicitly and list residual risks.
- If you make changes, keep them bounded, update PROGRESS.md and docs/NEXT_TODO.md, and run targeted tests.

Validation expectations:
- Prefer ./.venv-tests/bin/pytest with PYTHONPATH=src.
- Run focused tests before broad suites.
- Do not trigger live Harvest/Apify calls unless explicitly asked.
- Do not revert unrelated dirty worktree changes.
```
