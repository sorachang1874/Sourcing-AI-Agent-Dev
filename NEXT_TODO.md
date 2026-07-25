# NEXT_TODO — workspace work queue snapshot

```
owner: operator        refreshed: 2026-07-22       next-cleanup: next milestone
budget: ≤120 lines, replace-not-append; each row links its owning doc; done rows are DELETED
```

## Now

| Item | State | Route |
|---|---|---|
| Salvage lineage backfill **DONE 2026-07-23**: 3 行落库并核验（google fn8=2096/fn24=250 与清单 candidates_in=2346 吻合,tml root=220;run id+完整 request_filters 入 metadata）经新 `--from-salvage-manifest` 路径（`161b73b`,dry-run 全只读;旧 `--rebuild-from-assets` 不识 salvage 布局且 dry-run 曾会写库——均已修）。**余一契约缺口**：openai/tml 的 profile-detail 批无 registry lane 契约值（显式 skip 记录在案）——新 lane 值属过审事项,已并入评审队列 | R-033 + salvage report §5 |
| profile_fetched 对账 **DONE 2026-07-23**（`df1997d` 新 committed 脚本,证据制匹配:openai +20 flag/+20 mode,tml +10 flag/+204 Full-mode 可见;备份留存;ledger 已刷新）。tml authority 翻转 **operator 确认保留 113432（2026-07-23 亲答）**;26 个历史快照非权威行与 openai 分数重算已记录 | company_asset_supplement + R-033 |
| HarvestAPI capability-boundary probe round | designed, EXECUTES when quota returns | [HARVESTAPI_PLAYBOOK](sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md) §能力边界 |

## Next (approved sequence)

| Item | Gate | Route |
|---|---|---|
| Refactor continuation: **B0a+B0b+B1+B2 core+B3 core DONE 2026-07-22**（Step 3 size 转向退役 `a7b1b1b`；Block (a) resolver 抽取+梯队批1 `6bc844d`/`e713971`；拆分波1 smoke+harvest `588d784`/`bcdb035`；**mypy 81→0** `535d089`；R-035 关闭 `8bb7137`；法证打捞三件+PG 重试真缺陷根修 `93ba0f6`；god-file 425/39,459）; **R-009 CLOSED 2026-07-22（god-file 删除,防复活守卫入 lane）**;next = B3 尾（cli.py 注册表化）+ B4/B5 + W7.1 议案（四裁决已批,shard 记录补齐为 promote 前置批） | launch each session via `/refactor-goal` | [REFACTOR_MASTER_PLAN.md](sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md) §7 |
| Parked 23 delivered-job commands: per-job re-enqueue vs cancel | re-enqueue-safe under legacy deletion (pgLegacy Q2 verified); operator decides keep-vs-cancel | neutralization ledger (operator memory) |

## Blocked on external walls

| Item | Wall |
|---|---|
| Review re-fire queue (S1e2b rerun4, FF-SCHEMA rerun5, fnID roster, FT1 rerun4, FT2 rerun7, former-shard review, **B1 Step 2a former-only dispatch pinned `baa0040`**, **B3 Step 3 size-steering retirement（合同修订：INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT+acquisition_strategy 转向分支退役+4 处翻转测试，pinned `a7b1b1b`）**, **Block (a) CandidateSourceResolver 抽取（里程碑切片：新模块+Edge B 结构守卫入 lane，pinned `6bc844d`）**, **WS1 Step 5 统一设计文档（contract-heavy 设计评审：WS1_STEP5_COHORT_SHARD_UNIFICATION_DESIGN.md+盘点,pinned `30d9f16`）**, **WS1 no-policy 回退删除切片（隐藏回退移除评审：fail-closed+套件迁移+shard-id 单写者根修,pinned `9544b69`）**, **WS1 写默认退役（策略 id 统一+双接受单 owner,pinned `cee0a3f`）**, **R-037 410 族修复（scripted↔生产完成奇偶性:harness recovery 覆盖+remote-run 事件 watcher+终态装配复用,durable/terminal 语义触发器,pinned `aa9f6f8`）**, **W7.2 divider 设计+八裁决+S1 合同切片（`5a67a11`+`149667b`+S1 `93de793`+S2 `e04b3a6`+S3 影子 `ad59a22`+S4 R6 身份 `60c7a25`（S5 flip 等本链 GO）,contract-heavy 触发器 1）**, **WS1 4b-B 执行切换（pinned `1e44f23`）+4c 退役（pinned `606aa3c`）**, **W7.3 AI-promote 设计+八裁决+S0 oracle+S1 合同+S2 模型面+S3 影子（`ff88686`..`902afab`+S3 `cea1dc1`,contract-heavy 触发器 1+3;S4 payload 快照/S5 flip/S6 live 各自把门）**, **CI 环境盲点修复族（synthetic asset seam `6fe44e4`+node provisioning `1f9848e`+apt-index `edf5b5b`,PR #17 恢复）**, **W7.4 补偿/recovery 议案④ 设计+八裁决+S1 合同切片（设计 `69254a1`+S1 `671bf42`+**对抗性验证后硬化 `04018a6`**,contract-heavy 触发器 1;评审范围以硬化 commit 为准——S1 首版的 OQ5「新 dispatch identity 结构上不可表达」claim 已被证伪并撤回,派生校验降为 **S3 阻断性前置**（`derivation_verified`,设计 §3.4 D-C9）;S2/S3 各自把门;**第二轮对抗验证 blockers 0**,残余 overclaim 清除+R-047 入账 `17f2e57`）**, **W7.5 议案⑤ 后链路行为层设计（`3dfb734`,494 行;OQ1-OQ9 待 operator 裁决;S0 oracle 强制）**, **WS7 影子激活批 `ecb1f8f` 的对抗整改（本批,contract-heavy 触发器 1+3）——①**影子安全门**：促 shadow 的能力探针对真 provider client 同样为 True，live 模式会在权威写路径上同步发起计费调用且无 opt-in/无 kill switch;现改能力∧许可双谓词 + 专属默认 OFF 的 `SOURCING_WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL` / `SOURCING_WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL`（刻意不属三重 live 门）;②反惰性棘轮重建为 AST 布线注册表（首版对 9 点中 5 点空转;现实测 9/9 删除即红;8 个 caller 点为源码级覆盖已如实说明）;③divider 语料输入侧「real ready set」标注更正为**重构集**（live `ready` 行数为 0，队列态伪造）+ 来源拆分（33/34 拒绝来自磁盘 roster）;④D8 去重后 22/55=40.0%（原 45%）且**非本语料首发**（`e04b3a6` docstring 已有）;⑤电池主张收窄到实际触发过的 V1;⑥弃置量改同口径（梯子 150,576 vs AI 129,190）;⑦两个头条指标标注为构造性必然;⑧promote docstring 从「真模型比较断言」改写为地板上界论证;⑨agree 33/36 全面拆解（27 由 guard 强制、判官仅 9/36 相关）。**S5 阻断项 R-048 入账**：事故 #1 重放为 PROMOTED 且五器全 pass——退役阈值族会重开该事故） | chshapi quota——**探测 2026-07-23：耗尽至 2026-07-28 19:20**（reviewer CODEX_HOME 最小探针,gpt-5.6-sol/ultra/priority 策略在位）;07-28 后按队列顺序 re-fire |
| Any live acquisition (incl. TML empty-name former rows, hosted smoke intake evidence) | HarvestAPI monthly quota |

## Backlog (unowned, needs a lane)

- anthropic Jennifer Wang identity dup (completeness-gate finding, 2026-07-22).
- OpenAI legacy registry rows 103702/023615/023543 source_path still on old roots.
- test_env_live snapshot copies retirement (post root-unification).
- Track A code decomposition (orchestrator 82K lines) — owned by approved Track A plan.
- Residual test failures ledger: d3 identity-literal characterization;
  test_asset_reuse_audit full-company-warn case (both pre-exist, see RESIDUAL_LEDGER).
