# NEXT_TODO — workspace work queue snapshot

```
owner: operator        refreshed: 2026-07-22       next-cleanup: next milestone
budget: ≤120 lines, replace-not-append; each row links its owning doc; done rows are DELETED
```

## Now

| Item | State | Route |
|---|---|---|
| Salvage lineage backfill **DONE 2026-07-23**: 3 行落库并核验（google fn8=2096/fn24=250 与清单 candidates_in=2346 吻合,tml root=220;run id+完整 request_filters 入 metadata）经新 `--from-salvage-manifest` 路径（`161b73b`,dry-run 全只读;旧 `--rebuild-from-assets` 不识 salvage 布局且 dry-run 曾会写库——均已修）。**余一契约缺口**：openai/tml 的 profile-detail 批无 registry lane 契约值（显式 skip 记录在案）——新 lane 值属过审事项,已并入评审队列 | R-033 + salvage report §5 |
| profile_fetched 对账 **DONE 2026-07-23**（`df1997d` 新 committed 脚本,证据制匹配:openai +20 flag/+20 mode,tml +10 flag/+204 Full-mode 可见;备份留存;ledger 已刷新）。**⚠️ 待操作者确认的副作用**：tml authority 指针经 ledger 刷新从 `20260719T183049`（simulate 时代原始快照）翻至 `20260722T113432`（salvage 真实快照,含本会话回填的 lineage 行）——方向与 R-033/google cutover 一致但系 side effect;另 26 个 tml 历史快照被注册为非权威行;openai 分数重算 80.59→79.09（公式漂移,非 flag 变化所致）。如需回退:仅翻两行 authoritative 位 | company_asset_supplement + R-033 |
| HarvestAPI capability-boundary probe round | designed, EXECUTES when quota returns | [HARVESTAPI_PLAYBOOK](sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md) §能力边界 |

## Next (approved sequence)

| Item | Gate | Route |
|---|---|---|
| Refactor continuation: **B0a+B0b+B1+B2 core+B3 core DONE 2026-07-22**（Step 3 size 转向退役 `a7b1b1b`；Block (a) resolver 抽取+梯队批1 `6bc844d`/`e713971`；拆分波1 smoke+harvest `588d784`/`bcdb035`；**mypy 81→0** `535d089`；R-035 关闭 `8bb7137`；法证打捞三件+PG 重试真缺陷根修 `93ba0f6`；god-file 425/39,459）; **R-009 CLOSED 2026-07-22（god-file 删除,防复活守卫入 lane）**;next = B3 尾（cli.py 注册表化）+ B4/B5 + W7.1 议案（四裁决已批,shard 记录补齐为 promote 前置批） | launch each session via `/refactor-goal` | [REFACTOR_MASTER_PLAN.md](sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md) §7 |
| Parked 23 delivered-job commands: per-job re-enqueue vs cancel | re-enqueue-safe under legacy deletion (pgLegacy Q2 verified); operator decides keep-vs-cancel | neutralization ledger (operator memory) |

## Blocked on external walls

| Item | Wall |
|---|---|
| Review re-fire queue (S1e2b rerun4, FF-SCHEMA rerun5, fnID roster, FT1 rerun4, FT2 rerun7, former-shard review, **B1 Step 2a former-only dispatch pinned `baa0040`**, **B3 Step 3 size-steering retirement（合同修订：INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT+acquisition_strategy 转向分支退役+4 处翻转测试，pinned `a7b1b1b`）**, **Block (a) CandidateSourceResolver 抽取（里程碑切片：新模块+Edge B 结构守卫入 lane，pinned `6bc844d`）**, **WS1 Step 5 统一设计文档（contract-heavy 设计评审：WS1_STEP5_COHORT_SHARD_UNIFICATION_DESIGN.md+盘点,pinned `30d9f16`）**, **WS1 no-policy 回退删除切片（隐藏回退移除评审：fail-closed+套件迁移+shard-id 单写者根修,pinned `9544b69`）**, **WS1 写默认退役（策略 id 统一+双接受单 owner,pinned `cee0a3f`）**) | chshapi quota; probe before firing |
| Any live acquisition (incl. TML empty-name former rows, hosted smoke intake evidence) | HarvestAPI monthly quota |

## Backlog (unowned, needs a lane)

- anthropic Jennifer Wang identity dup (completeness-gate finding, 2026-07-22).
- OpenAI legacy registry rows 103702/023615/023543 source_path still on old roots.
- test_env_live snapshot copies retirement (post root-unification).
- Track A code decomposition (orchestrator 82K lines) — owned by approved Track A plan.
- Residual test failures ledger: d3 identity-literal characterization;
  test_asset_reuse_audit full-company-warn case (both pre-exist, see RESIDUAL_LEDGER).
