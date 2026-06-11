# Claude Code Project Handbook 2026-06-10

> Status: Current Claude Code handoff handbook for local asset governance, service-grade refactor, and future Agent-native development. Read with `../AGENTS.md`, `AGENTS.md`, `README.md`, `PROGRESS.md`, `INDEX.md`, `NEXT_TODO.md`, `SERVICE_GRADE_ARCHITECTURE_PLAN.md`, and `RUNTIME_ASSET_RETENTION_GOVERNANCE.md`.

> 2026-06-11 revision note: the owner approved a plan restructure after the Claude Code deep audit. Superseded items in this handbook: (1) the M0.9 standalone review-gated apply flow is CANCELLED — the 10 cold bundles are sha256-verified and the source dirs are reclaimed via the `runtime/test_env` TTL prune path instead; (2) the M1 "workflow/command spec manifest" milestone is redefined as a CommandSpec registry in `durable_runtime` with the manifest as its export; (3) the Independent Review Gate scope is narrowed to destructive operations on non-rebuildable assets and contract-heavy changes. See `SERVICE_GRADE_ARCHITECTURE_PLAN.md` "2026-06-11 Plan Revision" and `NEXT_TODO.md` for the current five-track structure.

## Project Context

This repository is a sourcing/recruiting automation product, not a generic scraping script. It manages company/candidate assets, LinkedIn/profile enrichment, public web evidence, CRM state, exports, provider workflows, and operator-facing frontend workbenches.

Current long-term direction:

```text
user intent
-> typed acquisition/profile/public-web/CRM/export action
-> OperationRun
-> WorkflowCommand
-> ActivityAttempt / EntityDelta
-> review/export/projection
```

OpenClaw, Codex, Claude Code, LangGraph, or other general Agents may become outer planners/search/browser runtimes. They must not write business tables directly or bypass approval, budget, retry, circuit, provenance, promotion, export, CRM writer, or projection-owner contracts.

## Non-Negotiable Invariants

- PG-only normal path. Do not reintroduce SQLite as a normal control plane, durable runtime, provider registry, CRM, Public Web, or projection path.
- Do not manually delete runtime assets. Use reviewed manifests, cold-copy/hash proof, dry-run, active-process checks, and `--apply --reviewed`.
- Do not `git add .`. The worktree is dirty and long-unsynced; use scoped branches/PRs and inspect staged files before commit.
- Contract-heavy changes need targeted tests, then Independent Review Gate, then any live/provider/manual signoff.
- Independent Review Gate default is GPT-5.5 with `model_reasoning_effort=xhigh` and `service_tier=fast`. Artifacts must record reviewer model, reasoning effort, and service tier.
- Frontend must consume backend-owned display/control contracts. Do not re-derive run status, retry/cancel capability, export readiness, or Public Web phase wording from raw command type/status.
- Agent-native Search/fetch/browser can be a supplemental evidence source, but results must enter the same ProviderTask/Evidence/EntityDelta/adjudication path as DataForSEO/API providers.
- Public Web manual promotions and PersonAssertions are durable assets. Retry or force refresh must not erase confirmed user state.
- Network/GitHub/Codex/Claude transport is read-only-diagnostic only. Run `make agent-network-preflight` when needed, but do not set proxy env, write git proxy config, hot-reload/restart Clash/Mihomo, call Clash controller mutation APIs such as `PUT /configs`, toggle TUN/DNS/system proxy, or change the selected `GLOBAL` node.

## Current Repository State

The current stable framing is service-grade workflow closure before an OpenClaw/Codex adapter. The most relevant current docs are:

- `README.md`
- `PROGRESS.md`
- `docs/INDEX.md`
- `docs/NEXT_TODO.md`
- `docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md`
- `docs/RUNTIME_ASSET_RETENTION_GOVERNANCE.md`
- `docs/AGENT_OPERATION_CONTRACT.md`
- `docs/PRE_AGENT_CONTRACT_REVIEW.md`
- `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`
- `docs/MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md`
- `docs/FRONTEND_API_CONTRACT.md`

Current codebase risk:

- Worktree contains many modified/untracked files across docs, frontend, scripts, tests, runtime, and review artifacts.
- Some important newly introduced files may be untracked in local state, including review-gate and live-validation scripts/tests. Check `git status --short` before any PR.
- Runtime, output, provider payloads, screenshots, local secrets, review prompts, node modules, venvs, and logs must not be broadly staged.

## Asset Governance Status

Current disk pressure snapshot from 2026-06-10:

- `runtime`: about `148G`
- `runtime/test_env`: about `117G`
- `runtime/cold_bundles`: about `1.3G`
- `output`: about `5.3G`
- filesystem free space: about `63GiB`

M0.6 completed:

- Evidence: `runtime/asset_governance/m0_6_20260609_runtime_test_env_closeout/prune_apply.json`
- Result: `35` low-risk historical runtime/output directories removed.
- Reclaimed: `14745489340` bytes.
- Failed operations: `0`.
- Not touched: `runtime/company_assets`, PG state, projections, registry pointers, provider cache, signoff/pressure artifacts, phase/milestone artifacts, or `current/latest` aliases.

M0.9 prepared but not applied:

- Bundle manifest: `runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_manifest.json`
- Cold bundle archive root: `runtime/cold_bundles/m0_9_20260610_runtime_supersession/`
- Selected artifacts: `10`
- Source bytes: `10730637032`
- Archive bytes: `1372774127`
- Estimated next reclaim: `9357862905` bytes.
- Current acceptable review artifact: none. Historical `v8`, `v9`, and `v10` M0.9 artifacts are evidence only and must not be used for destructive apply.
- Required scope digest after plan regeneration: `2fe946d01dec5e534861e5c7ca2cf2e635da15c1d8524cb0d35a901c01add512`

M0.9 is blocked until:

- A fresh current-scope Independent Review artifact returns `GO` with complete GPT-5.5/xhigh/fast metadata.
- The prune plan is regenerated with that fresh `GO` review artifact.
- The regenerated plan has the same scope digest.
- Dry-run succeeds.
- The user accepts stopping local dev runtime.
- Active backend/worker/frontend processes are stopped before destructive apply.
- `--apply --reviewed --strict` succeeds.

Use `docs/RUNTIME_ASSET_RETENTION_GOVERNANCE.md` for exact commands. Do not use `--skip-process-check` for real apply.

## Service-Grade Refactor Roadmap

M0: Documentation and GitHub checkpoint.

- Keep README/INDEX/PROGRESS/NEXT_TODO current.
- Create scoped branch/PRs only.
- Preserve review artifacts and validation commands in PR descriptions.

M0.5-M0.9: Local asset governance.

- Finish runtime/test_env and output retention with reviewed cold bundles.
- Keep company snapshot consolidation separate from runtime/output retention.
- No manual deletion.

M1: Workflow and command spec manifest.

- Centralize action/command owner, input/output schema, display/control contract, activity/entity evidence, approval/budget, retry/cancel/resume, and Agent exposure.
- Manifest should be consumable as an Agent tool spec.

M2: Provider Task Runtime v1.

- Unify DataForSEO, Harvest/Apify, document fetch, model adjudication, and future Agent Search/fetch/browser sources.
- Required semantics: stable item key, provider attempt, batch envelope, per-item retry, pending budget, late-result quarantine, cost/circuit metadata, provenance.

M3: Candidate Acquisition Service closure.

- Make `plan -> review -> probe -> discover -> normalize -> publish projection` a service-level contract.
- Legacy job/snapshot shells become report-visible migration/read compatibility only.

M4: Profile Fetch Service closure.

- Explicit URL item lifecycle: requested, cache hit/fetch required, provider fetch, terminal admit, projection admission.
- Retry failed URL items only; never rerun successful URL items or whole batches.

M5: CRM Public Web quality closure.

- Complete small live-provider validation for DataForSEO pending/timeout, model-provider circuit, manual promotion preservation, export payload quality, and evidence reviewability.
- Public Web should support DataForSEO/API provider plus reviewed Agent-native Search/fetch/browser as two evidence sources into one adjudication path.

M6: OpenClaw/Codex adapter.

- Expose read-only context tools and controlled action tools only after M1-M5 are stable.
- Adapter must not access DB tables, legacy routes, migration/backfill routes, or provider clients directly.

## Recommended Claude Code Workflow

1. Read `../AGENTS.md`, `AGENTS.md`, `README.md`, `PROGRESS.md`, `docs/INDEX.md`, and `docs/NEXT_TODO.md`.
2. Summarize current state and intended scope before editing.
3. Run `git status --short` and identify exact files to touch.
4. If GitHub, `gh`, Codex, Claude Code, or `chatgpt.com` transport fails, run `make agent-network-preflight` and report the failing invariant. Do not mutate proxy/VPN state.
5. For asset governance, inspect current manifests before running any apply-capable command.
6. For code changes, search all usages of changed contract fields/symbols before editing.
7. Run targeted tests first, then broader tests only when the touched surface is shared.
8. Run Independent Review Gate for contract-heavy changes, asset destructive apply, provider/model behavior, frontend/backend API semantics, and milestone closeout.
9. Use browser validation after frontend UI changes.
10. Update docs while context is fresh.
11. Report impact, changes, validation, and remaining risks.

## Useful Commands

Check local services:

```sh
bash ./scripts/dev_status.sh --runtime-dir runtime --api-port 8765 --frontend-port 4173
```

Read-only Agent/GitHub/network preflight:

```sh
make agent-network-preflight
```

Check disk pressure:

```sh
df -h .
du -sh runtime runtime/test_env runtime/cold_bundles output 2>/dev/null
```

Run pre-Agent contract gate:

```sh
make ci-pre-agent-contract
```

Run Independent Review Gate:

```sh
PYTHONPATH=src .venv/bin/python scripts/run_independent_review_gate.py \
  --execute \
  --timeout-seconds 600 \
  --title <scope-title> \
  --files "<space-separated touched files>" \
  --extra-context "<what the reviewer must verify>"
```

Run frontend build:

```sh
cd frontend-demo && npm run build
```

Check docs status banners:

```sh
python3 - <<'PY'
from pathlib import Path
paths = [Path("README.md"), Path("PROGRESS.md"), *sorted(Path("docs").glob("*.md"))]
missing = []
for path in paths:
    lines = path.read_text(errors="replace").splitlines()[:8]
    if not any(line.startswith("> Status:") for line in lines):
        missing.append(str(path))
print("\\n".join(missing))
print(f"checked={len(paths)} missing={len(missing)}")
PY
```

## PR And Branch Discipline

- Prefer one milestone per PR.
- The first GitHub sync should be a scoped checkpoint, not a whole-worktree dump.
- Include validation commands and review artifact path in PR description.
- Do not commit secrets, local provider payloads, raw runtime directories, local PG data, node modules, venvs, generated screenshots, or bulky output unless a specific artifact is intentionally part of a reviewed evidence trail.
- If committing review artifacts, include only the final relevant `GO` artifact, not every prompt/intermediate failure unless needed for audit.

## Known Open Risks

- M0.9 asset cleanup still needs reviewed apply.
- Public Web live-provider validation remains pending after UI/contract browser validation.
- Model-native Search remains experimental and fail-closed.
- Provider Task Runtime is not yet unified across DataForSEO, Harvest/Apify, document fetch, model adjudication, and Agent Search/fetch.
- `orchestrator.py` and `storage.py` remain large service-boundary risks.
- Operation Workbench is productized enough for current controls but not yet a natural-language Agent UI.
- GitHub history does not yet reflect the full current local state; scoped branch/PR synchronization is still required.
