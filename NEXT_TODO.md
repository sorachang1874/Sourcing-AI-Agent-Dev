# NEXT_TODO — workspace work queue snapshot

```
owner: operator        refreshed: 2026-07-22       next-cleanup: next milestone
budget: ≤120 lines, replace-not-append; each row links its owning doc; done rows are DELETED
```

## Now

| Item | State | Route |
|---|---|---|
| Salvage lineage backfill: register real Apify receipts (google fn8/fn24, tml roster+Full-mode, openai batch) as shard-registry rows from `*/20260722T113432` manifests | closeout DONE 2026-07-22 (google serving cutover 40-simulate→221424/4,297 REAL; tml/openai cache-merge + provenance rows; simulate snapshots quarantine-renamed; R-033); this backfill is the remaining forward step — `scripts/backfill_acquisition_shard_query_families.py --rebuild-from-assets --dry-run` first | R-033 + salvage report §5 |
| profile_fetched flag reconciliation: openai 20 closures + tml Full-mode upgrades not yet visible in counts | envelopes merged into authoritative snapshots' harvest_profiles; flags/PG counts need the supplement flow | company_asset_supplement + R-033 |
| 8 stalled `running` workflow_current_state rows (7 cancelled jobs + orphan `tml_full_fetch_20260719`) → terminalize; decide 2 stale pending plan_review_sessions (#2 OpenAI, #7 Google, 07-19/20) | latent resurrection surface + the only non-terminal legacy-strategy carriers (blocks legacy-fallback deletion conditions) | recon-gap-close pgLegacy inventory (2026-07-22) |
| HarvestAPI capability-boundary probe round | designed, EXECUTES when quota returns | [HARVESTAPI_PLAYBOOK](sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md) §能力边界 |

## Next (approved sequence)

| Item | Gate | Route |
|---|---|---|
| Refactor continuation: **B0a+B0b+B1+B2 core+B3 core DONE 2026-07-22**（Step 3 size 转向退役 `a7b1b1b`；Block (a) resolver 抽取+梯队批1 `6bc844d`/`e713971`；拆分波1 smoke+harvest `588d784`/`bcdb035`；**mypy 81→0** `535d089`；R-035 关闭 `8bb7137`；法证打捞三件+PG 重试真缺陷根修 `93ba0f6`；god-file 425/39,459）; **R-009 CLOSED 2026-07-22（god-file 删除,防复活守卫入 lane）**;next = B3 尾（cli.py 注册表化）+ B4/B5 + W7.1 议案（四裁决已批,shard 记录补齐为 promote 前置批） | launch each session via `/refactor-goal` | [REFACTOR_MASTER_PLAN.md](sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md) §7 |
| Parked 23 delivered-job commands: per-job re-enqueue vs cancel | re-enqueue-safe under legacy deletion (pgLegacy Q2 verified); operator decides keep-vs-cancel | neutralization ledger (operator memory) |

## Blocked on external walls

| Item | Wall |
|---|---|
| Review re-fire queue (S1e2b rerun4, FF-SCHEMA rerun5, fnID roster, FT1 rerun4, FT2 rerun7, former-shard review, **B1 Step 2a former-only dispatch pinned `baa0040`**, **B3 Step 3 size-steering retirement（合同修订：INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT+acquisition_strategy 转向分支退役+4 处翻转测试，pinned `a7b1b1b`）**, **Block (a) CandidateSourceResolver 抽取（里程碑切片：新模块+Edge B 结构守卫入 lane，pinned `6bc844d`）**, **WS1 Step 5 统一设计文档（contract-heavy 设计评审：WS1_STEP5_COHORT_SHARD_UNIFICATION_DESIGN.md+盘点,pinned `30d9f16`）**) | chshapi quota; probe before firing |
| Any live acquisition (incl. TML empty-name former rows, hosted smoke intake evidence) | HarvestAPI monthly quota |

## Backlog (unowned, needs a lane)

- anthropic Jennifer Wang identity dup (completeness-gate finding, 2026-07-22).
- OpenAI legacy registry rows 103702/023615/023543 source_path still on old roots.
- test_env_live snapshot copies retirement (post root-unification).
- Track A code decomposition (orchestrator 82K lines) — owned by approved Track A plan.
- Residual test failures ledger: d3 identity-literal characterization;
  test_asset_reuse_audit full-company-warn case (both pre-exist, see RESIDUAL_LEDGER).
