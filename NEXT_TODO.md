# NEXT_TODO — workspace work queue snapshot

```
owner: operator        refreshed: 2026-07-22       next-cleanup: next milestone
budget: ≤120 lines, replace-not-append; each row links its owning doc; done rows are DELETED
```

## Now (approved, in flight)

| Item | State | Route |
|---|---|---|
| Harness reorg R1 entry chain | executing | [design](sourcing-ai-agent/docs/HARNESS_REORG_DESIGN.md) §4 |
| Harness reorg R2 tool-native layer (skills/commands, scripts registry) | approved | design §4 |

## Next (approved sequence)

| Item | Gate | Route |
|---|---|---|
| Harness reorg R3–R5 (docs lifecycle, deliverables manifest, CI lints) | operator approval after R2 report | design §4–5 |
| Runtime-root selection + daemon restart (+ google 60-file rebuild via reconcile) | after harness reorg, operator picks root | [Phase C design](sourcing-ai-agent/docs/PHASE_C_DATA_CONSOLIDATION_DESIGN.md) + PROGRESS hazards |
| Parked 23 delivered-job commands: per-job re-enqueue vs cancel | after daemon restart | neutralization ledger (operator memory) |

## Blocked on external walls

| Item | Wall |
|---|---|
| Review re-fire queue (S1e2b rerun4, FF-SCHEMA rerun5, fnID roster, FT1 rerun4, FT2 rerun7, former-shard review) | chshapi quota; probe before firing |
| Any live acquisition (incl. TML empty-name former rows, hosted smoke intake evidence) | HarvestAPI monthly quota |

## Backlog (unowned, needs a lane)

- anthropic Jennifer Wang identity dup (completeness-gate finding, 2026-07-22).
- OpenAI legacy registry rows 103702/023615/023543 source_path still on old roots.
- test_env_live snapshot copies retirement (post root-unification).
- Track A code decomposition (orchestrator 82K lines) — owned by approved Track A plan.
- Residual test failures ledger: d3 identity-literal characterization;
  test_asset_reuse_audit full-company-warn case (both pre-exist, see RESIDUAL_LEDGER).
