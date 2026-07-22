# NEXT_TODO — workspace work queue snapshot

```
owner: operator        refreshed: 2026-07-22       next-cleanup: next milestone
budget: ≤120 lines, replace-not-append; each row links its owning doc; done rows are DELETED
```

## Now

| Item | State | Route |
|---|---|---|
| SALVAGE downloads of 4 orphaned PAID dataset groups (GDM fn8/fn24 function rosters ~2,346 items; TML Full-mode 192 urls; TML roster 220; OpenAI 19-url tail) | operator APPROVED 2026-07-22; **BLOCKED on local VPN/proxy (all external HTTPS = HTTP 000, api.apify.com resolves to fake-IP 198.18.0.104)** — retry probes the moment network returns; retention clock ticking (runs of 07-19/20) | salvage report (operator memory `artifacts-intake-20260721/SALVAGE_REPORT.md`) + `scripts/live_apify_dataset_salvage.py` |
| google repair snapshot 20260722T054928: review → registry promotion (or discard) | awaiting operator review (pointer already advanced; registry row absent by design) | serving-repair flow + completeness gate (passes on it) |
| PG shard registry: 2026-07-22 simulate-run rows (result_count=40) mask real lineage | NEW data-debt from salvage audit | salvage report §coverage |
| HarvestAPI capability-boundary probe round | designed, EXECUTES when quota returns | [HARVESTAPI_PLAYBOOK](sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md) §能力边界 |

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
