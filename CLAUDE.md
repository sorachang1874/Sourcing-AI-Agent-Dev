# CLAUDE.md — thin tool-native layer (content is single-sourced in AGENTS.md)

@AGENTS.md

## Critical rules (mirror; AGENTS.md and deeper AGENTS.md files win on conflict)

- External providers are FAIL-CLOSED. Never set the live triple-gate env
  (`SOURCING_EXTERNAL_PROVIDER_MODE=live` + `SOURCING_LIVE_PROVIDER_CONFIRM=1` +
  `SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS=1`) without explicit operator approval.
  Paid dispatch: inventory local + remote history first, delta-only, never retry/resume a
  terminalized paid command.
- **Billing-capable env is FIVE vars, not three** (2026-07-25). Besides the triple gate,
  these two permit a *model* call that costs money and are OFF by default — never set
  either without explicit operator approval:
  `SOURCING_WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL` (WS7 promote shadow; the seam sits on the
  authoritative write path) and `SOURCING_WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL` (WS7 divider
  shadow; refill/mint path, held under the scheduler lock). Without them a real provider
  client reaching either seam makes zero calls; the scripted clients
  (`SOURCING_SCRIPTED_*`) are billing-free and are the only sanctioned way to exercise
  these paths.
- Live-ops actions run committed `scripts/live_*.py` only — never /tmp scripts. The
  script registry lives in `sourcing-ai-agent/scripts/README.md`.
- Live PG schema: `sourcing_live_tml_path_20260719`. `.local-postgres.env` has no
  `export` — source with `set -a; source ...; set +a`. PG max 100 connections; NEVER run
  full-module `tests/test_pipeline.py` (non-lane, exhausts connections — RESIDUAL_LEDGER
  R-009).
- Use each package's `.venv` python. x-first tests:
  `PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests`.
- On premise/state conflicts: stop and ask; never silently expand paid scope.

## Routing

Current state: [PROGRESS.md](PROGRESS.md) · work queue: [NEXT_TODO.md](NEXT_TODO.md) ·
docs router: [sourcing-ai-agent/docs/README.md](sourcing-ai-agent/docs/README.md) ·
live lane state: `sourcing-ai-agent/.coord/BOARD.md` (gitignored; git wins).
