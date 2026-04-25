# Session Handoff 2026-04-25

> Status: Current handoff for opening the next development session. Read with `../PROGRESS.md`, `NEXT_TODO.md`, and `SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md`.

## Current State

- Productization workstreams are closed for this pass:
  - runner / recovery ownership
  - promoted aggregate coverage proof
  - plan semantics label sharing
  - runtime isolation / PG-only hosted guardrails
  - Harvest/materialization behavior reports
  - scripted provider scenarios
  - asset governance persistence
  - Excel intake target/export linkage
  - browser E2E plan/board hydration regressions
- Latest code-level fix:
  - async `/api/plan/submit` hydration now persists frontend history plan semantics metadata
  - frontend history recovery reads backend `dispatch_preview` and `effective_execution_semantics`
  - `full_company_roster` user-facing strategy label is `全量 live roster`
- GitHub handoff:
  - branch: `productization-2026-04-25-stable`
  - remote tracking: `origin/productization-2026-04-25-stable`
  - commits:
    - `ef1ea4c Stabilize productization workflow contracts`
    - `53d30f1 Add workspace development guardrails`
  - runtime/cache/vendor/build outputs, generated `object_sync`, and `frontend-demo/public/tml` offline assets were intentionally excluded.

## Validation Completed

```bash
./.venv-tests/bin/python -m pytest tests/test_execution_semantics.py tests/test_frontend_history_recovery.py -q
cd frontend-demo && npm run build
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q
./.venv-tests/bin/python -m pytest tests/test_regression_matrix.py tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q
./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q
bash ./scripts/run_python_quality.sh typecheck
./.venv-tests/bin/python -m pytest tests/test_markdown_status.py -q
./.venv-tests/bin/python -m pytest -q
```

Observed results:

- browser E2E: `5 passed, 2 skipped, 2 subtests passed`
- hosted smoke: `15 passed, 1 skipped, 2 subtests passed`
- full pytest: `1135 passed, 8 skipped, 25 subtests passed`

Scripted/simulated workflow reports also passed:

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/closeout_simulate_20260425 \
  --seed-reference-runtime \
  --fast-runtime \
  --strict \
  --timing-summary \
  --report-json output/closeout_20260425/simulate_report.json \
  --summary-json output/closeout_20260425/simulate_summary.json

PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/scripted_long_tail_closeout_20260425 \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/google_multimodal_long_tail.json \
  --fast-runtime \
  --case google_multimodal_pretrain \
  --strict \
  --timing-summary \
  --report-json output/closeout_20260425/google_long_tail_report.json \
  --summary-json output/closeout_20260425/google_long_tail_summary.json
```

## Resume Rules

1. Start in the repository root:

```bash
cd "/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
```

2. Read:

```text
PROGRESS.md
docs/NEXT_TODO.md
docs/SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md
docs/SESSION_HANDOFF_2026-04-25.md
```

3. Use the repository virtual environments:

```bash
./.venv-tests/bin/python -m pytest -q
cd frontend-demo && npm run build
```

4. Do not use `git add .`; the worktree contains runtime/cache/vendor/build artifacts.

## Guardrails Not To Regress

- Hosted/live control plane stays PG-only.
- `Public Web Stage 2` stays default-off.
- Replay/scripted/simulate provider cache cannot pollute live provider cache.
- Core roster/profile-search lanes are not gated by a generic high-cost flag.
- Frontend plan labels prefer backend execution semantics.
- Query-specific or Excel/import snapshots do not become authoritative coverage baselines without proof.

## Remaining Product Work

These are not blockers for the current stable branch:

- governance review UI and automated promotion-time `coverage_proof` stamping
- cross-process materialization writer queue / backpressure executor
- partial dataset streaming if Harvest exposes partial dataset reads
- broader CI promotion of scripted behavior guardrails
- Excel row-level continuation UI and large-batch UX benchmark
