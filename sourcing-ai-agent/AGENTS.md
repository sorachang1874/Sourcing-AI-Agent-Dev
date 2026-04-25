# AGENTS.md

> Status: Repository entry doc. Read together with `docs/INDEX.md` and `PROGRESS.md` before making contract or deployment changes.


## Scope
This file defines repository-specific workflow and product rules for `sourcing-ai-agent`.
It extends the parent `AGENTS.md` in `/home/sorachang/projects/Sourcing AI Agent Dev/AGENTS.md`.

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

## Session Resume And Regression Prevention

Required rule:
1. After context compaction, model restart, or handoff, read the latest `PROGRESS.md`, `docs/NEXT_TODO.md`, and the active session tracker before running commands.
2. If recent conversation logs are available, use them to confirm what was already tested or ruled out. Do not repeat long environment probes unless the tracker says the previous result is stale.
3. Record active work in Markdown while working, not only at the end. A fix that depends on chat memory is not durable.
4. When a symptom appears in planner, frontend, or results code, trace the upstream writer/contract first. Do not patch the first reader that exposed the symptom unless the writer contract is already proven correct.
5. Prevent behavior rollback by preserving these validated contracts:
   - hosted/live control plane is PG-only
   - `Public Web Stage 2` stays default-off
   - `full_company_roster` and `scoped_search_roster` core lanes may use Harvest profile search without a generic cost gate
   - replay/scripted/simulate provider data cannot pollute live provider cache
   - frontend plan labels must prefer backend execution semantics
6. If a temporary dual-track path is unavoidable, add an explicit removal condition in `docs/NEXT_TODO.md` and a test or banner that prevents it from becoming silent default behavior.

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

## Local Environment Guardrails
This repository has repeatedly failed for non-code reasons when the wrong shell or Python interpreter was used.

Required rule:
1. For local runtime commands, prefer repository `.venv/bin/python` over system `python3`.
2. For tests, prefer `.venv-tests/bin/python` / `.venv-tests/bin/pytest` when available, and rebuild them via `make bootstrap-test-env` when migrating machines or shells.
3. Prefer `bash ./scripts/...` over `./scripts/...` or ad hoc shell aliases.
4. Before debugging backend startup or DSN resolution, run `make dev-doctor` or at minimum:
   - `bash ./scripts/dev_backend.sh --print-config`
   - `PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli show-control-plane-runtime`
5. Treat `ModuleNotFoundError: requests` or `ModuleNotFoundError: psycopg` during local startup as an interpreter-selection bug first, not as an application bug.
6. If `.venv-tests/bin/python` resolves to an old system interpreter or triggers `xcode-select`, treat it as a broken migrated venv and recreate it instead of patching packages in place.
7. When touching local dev scripts, preserve these invariants in code, not only in docs.

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
   - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_<module>.py`
3. Full suite:
   - `PYTHONPATH=src .venv-tests/bin/pytest -q`
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
