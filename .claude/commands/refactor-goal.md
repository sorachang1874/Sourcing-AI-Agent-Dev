持续推进本工作区的重构续章与 harness/playbook 完善。这是长期 goal，按批次自治工作。

## 使命

三条主线并进：①按 `sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md`（唯一权威计划,operator 已批准）完成重构——acquire 策略统一到分 shard 抽象、Track A 单体分解、测试退役与 god-file 拆分、mypy 清零；②harness 持续完善——每次踩坑固化为 gate/registry/棘轮,绝不留口头教训；③把验证过的模式蒸馏回 ai-assisted-engineering-playbook（sibling repo,动前先 pull）。

## 每次会话启动

1. 读状态链：PROGRESS.md → NEXT_TODO.md → `sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md` §7 批次状态 → `sourcing-ai-agent/.coord/BOARD.md`（gitignored,git 为准）。
2. 实测校验：git status/log 与快照一致；PG 可达（`set -a; source sourcing-ai-agent/.local-postgres.env; set +a` 只读探测）；daemon 状态。声称与观察不符→先报差异。
3. 取 §7 第一个未勾批次（或 NEXT_TODO Now 区的收口跟进）继续。批次内自治推进,不逐步请示。

## 工作纪律（AGENTS.md 全款适用,以下是本 goal 的强调项）

- **先探索定方法再执行**：任何未量化成本画像的重型命令,先小样本/dry-run/探针（教训：rebuild-company-serving-view 75 分钟事故 vs live_promote 秒级——差别在于先探明了语义）。
- 守卫的拒绝是信息不是障碍：不绕守卫,先读懂它为什么拒。
- 评审门协议：契约级改动实现+定向测试+pin+记录评审请求后继续下一批;verdict 等 chshapi 配额;NO-GO 只冻所涉 scope。
- 每批收尾：NEXT_TODO/PROGRESS 刷新、§7 打勾、残差入 RESIDUAL_LEDGER 带 tripwire、BOARD 替换过时状态、跨会话事实进 memory。
- 提交节奏：file-scoped 原子提交,禁 `git add .`;测试先行,棘轮数字只降不升。
- 卡在配额/网络墙时：转 local-only 批次,墙况记 NEXT_TODO,不空转。
- 重侦察/审计任务可用多 agent 工作流（fan-out 侦察→对抗验证→完备性批评家）,机械小批次不用。

## 红线（永不例外）

外部 provider FAIL-CLOSED,三重门需 operator 明示;付费派发先盘点、delta-only、终态命令永不 retry/resume（cmd_a32cc93e15b5f52150ac4da0 / cmd_1330a600a2f91e5c0617a4b7 永禁）;live-ops 只跑 committed scripts/;PG 变更先列出再执行、mutation 走事务+rowcount 核验;禁全量 test_pipeline.py（R-009）;不动 proxy/VPN/Clash;premise 冲突停下来问,绝不静默扩大付费范围。

补充指令：$ARGUMENTS
