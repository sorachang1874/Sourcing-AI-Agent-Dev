# NEXT_TODO — workspace work queue snapshot

```
owner: operator        refreshed: 2026-07-22       next-cleanup: next milestone
budget: ≤120 lines, replace-not-append; each row links its owning doc; done rows are DELETED
```

## Now

| Item | State | Route |
|---|---|---|
| Registration-sync code fix: registry `source_path` should record canonical (not hot-cache) location | NEW — tick-1 reconcile re-minted a HOT_CACHE source_path on google 152139 (no longer destructive thanks to 23fb308, but the debt self-regenerates every reconcile) | [Phase C design](sourcing-ai-agent/docs/PHASE_C_DATA_CONSOLIDATION_DESIGN.md) C3 + asset_registration.py |
| google repair snapshot 20260722T054928: review → registry promotion (or discard) | awaiting operator review (pointer already advanced; registry row absent by design) | serving-repair flow + completeness gate (passes on it) |

## Next (approved sequence)

| Item | Gate | Route |
|---|---|---|
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
