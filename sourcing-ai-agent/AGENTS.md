# AGENTS.md

> Status: Repository entry doc. Read together with `docs/INDEX.md` and `PROGRESS.md` before making contract or deployment changes.


## Scope
This file defines repository-specific workflow and product rules for `sourcing-ai-agent`.
It extends the parent `AGENTS.md` in `../AGENTS.md`.

## Project Framing For New Sessions
This repository implements a recruiting and public-information enrichment product.
It combines provider-supported search/profile APIs, public web evidence, workflow orchestration, and operator review flows to support sourcing, target-candidate management, and reusable company/person assets.

Required rule:
1. When summarizing this repository or updating docs, describe it with product/operations language such as sourcing, recruiting automation, public-information enrichment, provider queues, recovery, quality gates, and auditability.
2. Keep the business context explicit: candidate research, company roster completion, target-candidate follow-up, and exportable evidence-backed signals.
3. Avoid introducing unrelated high-sensitivity wording when the task is ordinary product engineering or runtime operations.

## Current Upgrade Goal
This repository is in a service-grade upgrade phase.

Required rule:
1. Optimize for the long-term hosted `serve` architecture, not for local one-off scripts.
2. Large architectural or storage changes are allowed when they reduce future rework and preserve safe rollout boundaries.
3. Prefer modular boundaries, shared contracts, and reusable infra over narrowly scoped patches.
4. Do not preserve old path or data-flow semantics just for compatibility if they create a second long-term execution model.
5. During this phase, do not sync unfinished upgrade work to ECS unless the user explicitly asks.

## Engineering Method
This codebase should now be treated like an existing production-grade system under active re-architecture.

Required rule:
1. Understand the real engineering objective before changing code. Do not optimize a single symptom while leaving the same failure mode elsewhere.
2. Perform impact analysis first:
   - shared variables
   - types
   - config
   - schemas
   - interfaces
   - callsites
   - tests
3. Prefer root-cause fixes over case-by-case patches.
4. When a shared contract changes, update all downstream consumers in the same pass when feasible.
5. If a touched module still has obvious high-value service-grade cleanup in the same area, continue and finish it instead of stopping after the first small fix.
6. Do not introduce new dual-track semantics unless there is a hard migration constraint and the fallback is clearly temporary.
7. Keep code clean enough that future hosted, local, and import/restore flows can reuse the same contract.
8. Project-owned local asset packages belong under `local_asset_packages/`; sibling projects are temporary import sources, not long-term runtime dependencies.

## Contract Field Ownership And Preflight
Recent W6 failures showed that response fields with similar names can drift when ownership is implicit. `facet_summary_scope` and `filter_contract.facet_count_scope` are separate contracts; deriving one from the other caused cross-endpoint disagreement that a long smoke should never need to discover.

Required rule:
1. Every shared contract field must document its owner, source of truth, allowed values, derivation rule, normal consumers, forbidden consumers, fallback/migration status, and fast preflight test.
2. Fields that affect candidate counts, filter counts, readiness, row membership, status text, routing, exportability, permission visibility, cost/budget behavior, recovery ownership, or workflow terminal state must appear in the relevant owner matrix before normal code consumes them.
3. Public endpoints that expose the same contract object must use one owner/adapter. `/progress`, `/dashboard`, `/candidates`, and `/board-patches` may add endpoint-specific rows or patches, but they must not independently reinterpret `board_runtime_state`, lifecycle, projection readiness, facet scope, or filter-count semantics.
4. W6/Nightly/full scripted matrices validate long-chain stability, latency, recovery, and pressure behavior. They must not be the first detector for basic field semantics, hidden fallback, source-of-truth drift, or owner ambiguity. Convert every such incident into a fast unit/preflight before rerunning the expensive matrix.
5. Similar field names are not proof of equivalent semantics. If one field is derived from another, the contract must state that explicitly and tests must assert parity. Otherwise the frontend/backend must treat them as independent fields.
6. Temporary fallback or migration bridges must be report-visible, signoff-gated, and paired with a deletion plan in `docs/NEXT_TODO.md`. Do not add a silent fallback ladder to make a test pass.
7. Before Phase 13 Agent product work, review projection/public-reader, workflow/durable runtime, PersonAsset/evidence/assertion, CRM, Public Web, export, and frontend API contracts for field owner matrices and fast preflight coverage.

## Production-Grade Design Review
Every review, plan, and implementation slice must be judged as service-level product engineering, not as a local patch.

Required rule:
1. Engineering efficiency: prefer designs that reduce repeated infrastructure churn, amortize provider/runtime coordination costs, and remove recurring manual repair steps.
2. Architecture quality: move decisions toward explicit contracts and canonical sources of truth; do not add hidden fallback ladders unless the migration boundary and deletion condition are documented.
3. Business semantics: user intent, requested population boundary, coverage proof, planner strategy, lifecycle phase, and frontend wording must be represented by clear fields rather than inferred separately in multiple readers.
4. Downstream closure: when a contract changes, review all affected writers, readers, APIs, workers, CLIs, frontend adapters, docs, tests, and operational playbooks.
5. Risk coverage: account for stale snapshots, partial migrations, missing registry rows, retries, race conditions, lease/takeover behavior, provider webhook delays, and hosted/local environment drift before declaring a design complete.
6. User interaction quality: validate in-flight user experience, board visibility, progress wording, and cross-endpoint consistency, not only terminal job state.
7. Test quality: add business/service-level guardrails such as latency budgets, page data coherence, board materialization visibility, batch efficiency, actor-slot utilization, and ECS/local planner parity when those are the real product risks.
8. Root-cause depth: bug fixes should identify and remove the failure mechanism. If a fix only masks the symptom, record it as a stopgap and immediately define the durable follow-up.
9. Forward compatibility: prefer abstractions that naturally cover the next company, provider, workflow stage, asset type, and migration path without company-specific rewrites.

Before accepting a design, ask whether one more bounded step can retire an old heuristic, derived source, manual repair, or ambiguous contract. Take that step when it materially reduces future regression risk.

## Independent Review Gate
Significant changes and milestone closeouts require an independent adversarial review before manual signoff, live provider validation, W6/nightly, or Phase 13 product work. This gate is defined in `docs/INDEPENDENT_REVIEW_GATE.md` and uses `docs/INDEPENDENT_REVIEW_BRIEF.md` as the standing reviewer context.

Required rule:
1. The reviewer must not be the author of the implementation or contract change.
2. Prefer a different model/tool from the author when available, but a separate non-author read-only Codex session is an accepted reviewer even when the implementation author also used Codex. Model selection and fallback per lane are governed by the checked-in Model Routing Table in `docs/INDEPENDENT_REVIEW_GATE.md`.
3. The review is adversarial: prioritize correctness, contract ownership, hidden fallback, provider-cost, migration, runtime recovery, data-quality, and frontend/backend drift risks.
4. Run the reviewer in read-only mode. For Codex, use the canonical `scripts/run_independent_review_gate.py --execute` app-server transport; it reads newest-model/highest-effort settings from `~/.codex/config.toml`, mirrors those values into the isolated thread/turn requests without CLI pins, and stores the output plus hash-bound active-settings/transcript/rollout evidence under `runtime/reviews/`. A valid artifact requires `reviewer_exit_code=0`, complete `thread/start` active model/effort/tier evidence, matching rollout corroboration, and no model reroute.
5. After a review request is recorded against a pinned commit, unrelated work and the next implementation batch may proceed. A `NO-GO` must still be fixed or explicitly accepted before the affected scope can enter live/W6/manual validation or milestone signoff; a green test suite does not override that scope-local block.
6. Provider-costing live gates and milestone signoff must require a valid `GO` review artifact that matches the current gate scope, not just a path that exists. `NO-GO`, timeout, no-output, invalid, missing-metadata, prompt-file, or unrelated artifacts fail closed.
7. Independent review is required for Contract docs/schema/owner/source-of-truth changes, frontend/backend public API semantics, durable runtime command/activity/recovery/idempotency/outbox/causality, provider/model behavior, Public Web quality/materialization/export/promotion, projection/public reader, CRM/person/company asset, export, migration bridge/fallback/legacy deletion, Agent/Operation control surfaces, and any feature or phase being claimed as complete.
8. Run targeted tests and fast preflight before the independent review. W6/nightly, live provider validation, or manual browser review should validate long-chain behavior after the gate, not discover basic Contract drift.
9. Small local changes may skip the gate only when they do not change shared semantics, provider/model behavior, storage schema, read/write ownership, frontend/backend contract fields, action semantics, test gates, or milestone readiness.
10. Independent review is a scoped read-only gate, not a full development-session resume. The reviewer should inspect the listed diff scope and targeted Contract sections with `rg`, `git diff -- <files>`, and `nl -ba <file> | sed -n ...`; it should not read full `PROGRESS.md`, full `docs/NEXT_TODO.md`, or full long Contract files unless the review scope specifically requires that full context.

## Workflow Contract Operating Rules
The 2026-05-08/09 PG-backed scripted reruns (`rerun1` through `rerun70`) established stricter workflow development rules. These are now repository constraints, not one-off incident notes.

Required rule:
1. Treat workflow behavior as a contract-first state machine. Before editing profile scheduling, worker recovery, post-profile materialization, board publication, or public progress APIs, identify the durable owner of each state and the metric/report that proves it.
2. Do not let final success hide service failures. A run that eventually produces correct candidates is still failing if report completeness, recovery phase metrics, profile scheduler contract, post-profile SLOs, cross-endpoint board parity, or provider-mode isolation are missing or violated.
3. Pre-manual scripted handoff requires all of these to pass:
   - PG-backed isolated runtime for workflow confidence
   - strict smoke expectations for the scenario
   - contamination audit with `status=clean`
   - provider invocation report with every invocation in the expected non-live mode
   - `scripts/review_scripted_smoke_run.py` signoff with no blocking or manual-review findings
4. Report completeness is fail-closed. Missing `event_level_efficiency`, `profile_scheduler_contract`, `service_metrics.post_profile_completion`, `service_metrics.recovery_phase_metrics`, `board_runtime_state_parity`, provider invocation mode, or board-visible replay evidence is a blocking test failure, not an absent optional diagnostic.
5. Public reads and frontend merges must not compensate for broken backend convergence. `/progress`, `/dashboard`, `/candidates`, and `/board-patches` may expose diagnostics for stale or incomplete state, but normal convergence belongs to event-time writers, durable queues, recovery services, and explicit backfill/repair commands.
6. Callback/daemon/recovery ownership must stay separated:
   - provider/webhook/watcher callbacks record evidence, release matching leases, enqueue deterministic durable work, emit refill audit signals, and return
   - callbacks must not scan/replan/claim/submit profile registry rows, run local apply, publish board patches, run full snapshot materialization, resume workflows, or refresh global metrics
   - worker recovery owns selected worker execution and callback invocation
   - profile-refill daemon owns registry scan/replan/claim/provider submit
   - `local_apply_closure` drain owns local apply only
   - `board_visible_delta_apply` drain owns board-visible patch publication only
   - `snapshot_full_materialization` owns full artifact/retrieval/index compaction separately
7. Profile scheduling has one normal entry point: `MultiSourceEnricher.queue_background_profile_prefetch(...)`. Full-roster, live-roster, scoped-search, scoped-search+baseline, search-seed, registry refill, company-asset completion, and enrichment maintenance paths must use this scheduler and the shared `ProfilePrefetchBatchPlan`. Do not add caller-local chunking heuristics, direct Harvest profile submitters, synchronous `fetch_profiles_by_urls(...)` fallbacks, or ThreadPool-based profile planners to workflow or maintenance paths.
8. `linkedin_profile_registry` is the profile URL scheduler source of truth. Provider workers are remote-run envelopes; URL ownership, retry wait, coalescing timers, terminal state, and dispatch identity live on registry rows. PG workflow confidence must use transaction-scoped scheduler locks; SQLite compatibility is unit-test only.
9. Retry is item-level and isolated. Successful profile URLs must not be retried with failed URLs, retry waves must wait for normal-wave closure, and the default retry budget is one retry per URL unless the provider/scenario contract explicitly says otherwise.
10. Live-roster, scoped-search, and scoped-search+baseline must share the same post-profile and board-runtime contracts. Their differences are denominator shape, baseline reuse, and discovery-lane proof, not separate profile scheduling, local apply, board publication, or frontend progress semantics.
11. Board-runtime state is the frontend source of truth. Stage 1 progress may remain as execution detail, but user-facing profile/card progress, candidate sync, layering state, and filter counts must come from the canonical backend projection and be cross-endpoint consistent.
12. SLO semantics must distinguish hard scheduler failures from diagnostic headroom:
   - `local_completion_to_next_submit_start_ms` and `next_submit_provider_attempt_elapsed_ms` are hard local scheduler/handoff signals when configured
   - `profile_scheduler_contract.violation_detected` is a hard scheduler contract failure
   - `remote_to_next_submit_start_ms` is end-to-end diagnostic unless it exceeds the published hard threshold, because it includes provider event lag and marker lag outside the immediate local slot-refill handoff
13. If a gate fails, first decide whether it exposed a product/runtime bug, a contract ambiguity, or an evaluator/fixture bug. Do not weaken a gate until the business behavior it represents is explicitly reviewed and the replacement metric is documented.
14. Any temporary compatibility path must have a removal condition in `docs/NEXT_TODO.md`. New normal workflow behavior must not depend on legacy helpers, summary mirrors, public-read mutations, or frontend fallback ladders.

## Durable Causality And Auditability
Auditability means typed execution causality, not just logs.

Required rule:
1. Normal-path durable work must be explainable as `append-only workflow_event -> reducer -> workflow_command -> owner result/domain event -> read-model update`.
2. `workflow_commands` must carry physical causality columns for normal-path work: source event, parent command, causal group, owner, produced counts, no-op reason when counts are zero, readiness effect, and downstream refs. A `payload.causality` debug mirror is not the source of truth.
3. Metrics and SLO reducers must prefer typed causality over timestamp/snapshot pairing. If a metric still uses heuristic pairing, it must expose that fact in the report, be blocked by normal-path signoff, and have a documented migration/deletion condition before it can appear in W6/Nightly.
4. Workers and callbacks may record result/events; they must not mint arbitrary downstream commands or repair public read models outside the reducer/owner contract.
5. Public readers remain fail-closed read-model consumers. They may expose provenance and readiness, but they must not create causality by scanning artifacts or legacy summaries.
6. Future Agent operations should query this causality graph and request operation events; they must not bypass domain owners.

## Session Resume And Regression Prevention

Required rule:
1. After context compaction, model restart, handoff, or any session resume, do not immediately run exploratory or modifying commands. First restore context from:
   - the latest user/assistant interaction record available in the current thread or handoff summary
   - `PROGRESS.md`
   - `docs/NEXT_TODO.md`
   - any active session tracker or handoff doc referenced by those files
2. Before taking action after a resume, explicitly identify:
   - what the user is asking for now
   - what was already completed or ruled out in the previous turns
   - which commands/tests were already run and whether their results are still fresh
   - the next bounded action plan
3. Do not repeat long environment probes, provider checks, rebuilds, or broad test suites after compaction unless the restored context says the previous result is stale or the current task genuinely needs a fresh run.
4. If recent conversation logs are unavailable, state that assumption, then use the Markdown trackers as the source of truth before proceeding.
5. Record active work in Markdown while working, not only at the end. A fix that depends on chat memory is not durable.
6. When a symptom appears in planner, frontend, or results code, trace the upstream writer/contract first. Do not patch the first reader that exposed the symptom unless the writer contract is already proven correct.
7. Prevent behavior rollback by preserving these validated contracts:
   - hosted/live control plane is PG-only
   - `Public Web Stage 2` stays default-off
   - `full_company_roster` and `scoped_search_roster` core lanes may use Harvest profile search without a generic cost gate
   - replay/scripted/simulate provider data cannot pollute live provider cache
   - frontend plan labels must prefer backend execution semantics
8. If a temporary dual-track path is unavoidable, add an explicit removal condition in `docs/NEXT_TODO.md` and a test or banner that prevents it from becoming silent default behavior.

## Agent Network And GitHub Access

GitHub, `gh`, Codex, Claude Code, and model-backend transport are developer-environment prerequisites, not product runtime behavior.

Required rule:
1. Before running GitHub operations, pushing branches, invoking `gh`, or relying on Codex/Claude Code backend calls after a network error, run `make agent-network-preflight`.
2. Treat `scripts/agent_network_preflight.sh` as read-only diagnostics. It may check shell proxy env, git proxy config, GitHub DNS, Clash/Mihomo `GLOBAL`, HTTPS reachability, `gh auth status`, and `git ls-remote`; it must not mutate environment, git config, Clash config, selected nodes, TUN, DNS, system proxy, or macOS network settings.
3. Agents must not set `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`, or git proxy config as an automatic workaround for GitHub/Codex failures.
4. Agents must not toggle Clash mode, TUN mode, DNS mode, system proxy, or the selected `GLOBAL` node. They also must not hot-reload or restart Clash/Mihomo, including controller socket mutation calls such as `PUT /configs`.
5. If GitHub or model-backend domains resolve to `198.18.x.x`, report that fake-ip filtering needs user attention. If `GLOBAL` is `DIRECT`, report that the user should choose a non-DIRECT node. If `gh auth status` fails, ask the user to refresh/login with `gh`; do not alter proxy settings or reload VPN state to force it.
6. Local service proxy handling remains limited to `scripts/local_dev_proxy_guard.sh`, which only normalizes loopback `NO_PROXY` for local backend/frontend calls. It is not a general GitHub/network repair path.

## Validation and Delivery
Required rule:
1. Before calling work complete, run validation that matches the blast radius:
   - lint
   - type checks
   - tests
   - build
2. For large infra or architecture changes, rerun simulate-only workflow validation before delivery.
3. If failures appear, fix the generalized cause, not only the current fixture.
4. Final delivery should explicitly cover:
   - impact analysis
   - implementation approach
   - actual changes
   - validation results
   - remaining risks / assumptions
5. Do not announce completion early when shared paths, tests, or docs are still stale.
6. Keep the default regression loop fast. Expensive browser E2E or hosted smoke coverage may be opt-in, but the explicit command to run them must stay documented and working.
7. For provider-backed workflow changes, do not deliver on unit tests alone. Inspect at least one realistic smoke report or artifact path, and verify the relevant contract sections are present rather than inferred.
8. Before handing a scripted workflow to manual browser testing, run Pre-Manual Scripted Signoff and report the signoff path/status. A strict smoke pass without signoff is not delivery-ready.

## Local Environment Guardrails
This repository has repeatedly failed for non-code reasons when the wrong shell or Python interpreter was used.

Required rule:
1. Before launching local dev, scripted/browser test environments, local live smoke, or hosted/ECS workflows, read `docs/RUNTIME_PREFLIGHT.md` and follow the mode-specific checklist.
2. For local runtime commands, prefer repository `.venv/bin/python` over system `python3`.
3. For tests, prefer `.venv-tests/bin/python` / `.venv-tests/bin/pytest` when available, and rebuild them via `make bootstrap-test-env` when migrating machines or shells.
4. Prefer `bash ./scripts/...` over `./scripts/...` or ad hoc shell aliases.
5. Before debugging backend startup or DSN resolution, run `make dev-doctor` or at minimum:
   - `bash ./scripts/dev_backend.sh --print-config`
   - `PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli show-control-plane-runtime`
6. Treat `ModuleNotFoundError: requests` or `ModuleNotFoundError: psycopg` during local startup as an interpreter-selection bug first, not as an application bug.
7. If `.venv-tests/bin/python` resolves to an old system interpreter or triggers `xcode-select`, treat it as a broken migrated venv and recreate it instead of patching packages in place.
8. When touching local dev scripts, preserve these invariants in code, not only in docs.
9. When the user expects local frontend/backend to remain available after the current tool call, use durable launch paths such as `make dev-launch-backend` / `make dev-launch-frontend` or a named `screen` session. Do not rely on raw `nohup ... &`, background `&`, or foreground dev servers owned by a transient tool session.

## Storage and Runtime Semantics
This repository is moving to a canonical-store plus hot-cache model.

Required rule:
1. Canonical company assets are the authoritative asset store.
2. Local runtime cache is a hot cache for serving and recovery, not the source of truth.
3. New code must not hardcode raw `runtime/company_assets/...` joins when a shared resolver can be used.
4. Serving and recovery code may prefer hot cache reads when the requested snapshot is present there.
5. Governance, repair, registry, and canonical backfill code should default to the canonical asset store.
6. Query jobs should reference snapshots, generations, and result views rather than copy full company assets into per-job runtime directories.
7. Compatibility artifacts are allowed, but they must not become the only source consumed by hot paths.
8. New durable runtime, OperationRun, workflow-current-state, CRM task/current-state, and Agent-callable execution tables must be PG-first and PG-only for normal paths. Do not add SQLite DDL, fallback writes, or SQLite-backed tests for these tables unless the code is explicitly migration-only and report-visible.

## AI-First Direction
Required rule:
1. Use AI where ambiguity is real:
   - company identity resolution
   - intent expansion
   - evidence synthesis
   - review prioritization
   - segmentation / outreach reasoning
2. Keep control plane, storage layout, orchestration, and deterministic serving contracts deterministic by default.
3. When user correction becomes available, treat it as durable system memory, not a transient UI-only override.

## Request Semantics: Single Source of Truth
This project has repeatedly regressed when different modules read different request fields.

Required rule:
1. Treat the effective request as the canonical execution contract.
2. Prefer `build_effective_job_request(...)` / `build_effective_request_payload(...)` as the normalization boundary.
3. Do not introduce new logic that independently re-derives behavior from raw request fields when the effective request already exists.
4. When task metadata is passed downstream, include the same canonical request payload so workers, seed discovery, review, and retrieval consume the same semantics.
5. If a field must exist in both `intent_view` and another payload for compatibility, keep them synchronized in the same change.
6. When changing request semantics, review and keep aligned in the same pass:
   - `request`
   - `request_preview`
   - `intent_view`
   - task metadata
   - dispatch matching inputs
   - retrieval / preview payloads

## Project-Specific Contract Rules
These are not optional style preferences. They are known failure modes from this codebase.

1. `organization_keywords` may contain scopes, teams, products, or sub-orgs. Do not silently treat them as new companies.
2. Terms like `Veo`, `Nano Banana`, `Gemini`, `ChatGPT`, `Claude`, `Reasoning`, `Pre-train`, `Post-train` are often scope or thematic signals, not organization identity.
3. If a company-level baseline exists, new query-specific shards must not overwrite or degrade the organization baseline without an explicit promotion decision.
4. Current lane and former lane coverage must be tracked separately and reused separately. Do not collapse them into one vague "baseline ready" signal.
5. If effective baseline coverage already satisfies the request, do not continue to propagate inherited `force_fresh_run`.
6. Keep dispatch, planning, acquisition, and retrieval aligned on the same reuse decision semantics:
   - `new_job`
   - `reuse_snapshot`
   - `delta_from_snapshot`
   - `join_inflight`
   - `reuse_completed`

## Planning and Dispatch Must Stay Lightweight
User-facing planning paths must stay fast even when runtime assets are large.

Required rule:
1. Do not add directory scans, snapshot unions, or expensive rebuilds to the request path unless explicitly approved.
2. Prefer cached reads from:
   - `organization_asset_registry`
   - `acquisition_shard_registry`
   - `organization_execution_profiles`
   - cached completeness ledger / summary artifacts
3. Heavy repair or recomputation belongs in:
   - acquisition completion hooks
   - import / restore flows
   - warmup daemons
   - explicit repair / admin commands
4. If planning needs fallback repair, make it conditional, rare, and clearly labeled in the returned explanation.
5. Do not put organization-registry backfill, snapshot scanning, or full asset rebuild into hot planning / dispatch paths.
6. If a cache is missing, prefer one bounded repair step and record that it happened. Do not silently turn every user query into a repair job.

## Workflow Stage Invariants
Stage ordering has regressed before. Preserve these invariants:

1. `linkedin_stage_1` must complete before `stage_1_preview`.
2. `stage_1_preview` must not be generated from partial or stale candidate materialization when the required Stage 1 lane is still missing.
3. `public_web_stage_2` must not be marked complete before its worker results are actually materialized.
4. Do not mark acquisition milestones as completed early just because workers were scheduled or partially finished.
5. If you change stage transitions, validate:
   - progress payloads
   - runtime summaries
   - stage summary files
   - preview artifacts
   - final retrieval triggers

Project-specific guidance:
1. Prefer parallelism across independent acquisition lanes or shards, not inside a shared mutable merge step.
2. When parallelizing external-provider work, preserve dedupe, budget control, and replayability.
3. When changing concurrency in hosted workflow execution, also review recovery, takeover, and progress endpoints.

Repository validation commands:
1. Python quality gates:
   - `make lint`
   - `make typecheck`
   - `make verify-python`
2. Targeted tests:
   - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_<module>.py`
3. Full suite:
   - `PYTHONPATH=src ./.venv-tests/bin/pytest -q`
4. Frontend build:
   - `cd frontend-demo && npm run build`

Additional repo-specific verification:
1. Run the full suite when the change touches shared request semantics, planning, acquisition, dispatch, storage, worker recovery, or other cross-cutting behavior.
2. For changes to hot paths or shared contracts, inspect at least one realistic payload or artifact, not just test mocks.
3. For changes touching provider query generation, explicitly check for duplicate or near-duplicate queries before considering the change complete.
4. Normalize obvious equivalent query forms when appropriate, especially:
   - hyphen vs space vs underscore variants
   - capitalization-only differences
   - repeated aliases that would hit the same provider population
5. Keep scripted/simulate provider fixtures close to real provider envelopes, including:
   - nested wrapper shapes such as `result/data/profile`
   - sparse but still usable payloads
   - retryable vs unrecoverable error envelopes
   - partial background/recovery progress states
6. For workflow confidence, prefer PG-backed scripted smoke over SQLite/in-process simulations. SQLite is appropriate for focused unit or migration tests, not for service-level scheduler/recovery confidence.
7. If a scripted smoke report lacks provider invocation modes, board-runtime parity, recovery phase metrics, profile scheduler contract, or post-profile completion SLOs, treat the report as incomplete even if final candidates are correct.

## Asset Governance and Reuse
This codebase has both company-level assets and query/shard-level assets. Keep the boundary clear.

Required rule:
1. Small organizations should default to full company asset thinking unless the request explicitly narrows execution strategy.
2. Large organizations should default to scoped search / shard reuse unless the request explicitly asks for full roster.
3. Do not let a query-specific shard masquerade as a new company asset.
4. Do not degrade an authoritative company baseline just because a newer scoped snapshot exists.
5. When changing authoritative selection or reuse logic, also review:
   - `organization_asset_registry`
   - company identity registration / alias resolution
   - `organization_execution_profiles`
   - completeness ledger
   - shard registry
   - import / restore flows
   - cloud bundle docs
6. If a workflow writes new company assets, make sure the corresponding reusable registries are refreshed or a bounded warmup/repair path is clearly exposed. Do not leave new assets discoverable only by ad hoc filesystem scans.

## Hosted Path and Recovery
Production-like execution should follow the hosted path unless this is explicitly a repair operation.

Required rule:
1. Treat `serve + run-worker-daemon-service` as the default runtime path.
2. Treat manual `execute-workflow` as a debugging / repair tool, not the normal orchestration path.
3. Treat `import-cloud-assets` as the default restore / import path instead of ad hoc manual restore chains.
4. If you touch recovery or watchdog logic, validate hosted behavior, not only detached or unit-only flows.
5. Do not introduce fixes that only work when a human manually supervises the workflow.

## Cost and Safety for External Providers
This project can incur real external API cost.

Required rule:
1. Prefer dry checks, cached assets, `simulate`, `replay`, or scripted fake-provider modes when validating planner or orchestration logic.
2. Do not jump to live external-provider execution when a local or simulated verification path is sufficient.
3. When live calls are necessary, keep them scoped and explain why a lower-cost path was insufficient.
4. Avoid redundant provider calls caused by query alias duplication, incomplete request normalization, or repeated recovery retries.
5. For live large-org changes, sanity check expected provider parameters before execution.
6. Scripted/manual/browser workflow tests must fail closed if a non-live run attempts `provider_mode=live`, has missing provider-mode evidence, writes synthetic fixture requests into live provider cache, or leaves root/local-dev recovery daemons active.
