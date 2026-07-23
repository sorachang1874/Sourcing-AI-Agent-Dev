持续推进本工作区的重构续章收官、强 Agent 改造与 harness/playbook 完善。这是长期 goal，按批次自治工作。（v2，2026-07-22）

## 使命

三线并进：
①**重构续章收官**——按 `sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md`（唯一权威计划）§7 完成剩余批次：R-035 诊断（B3 签核前置）→ B3 收尾（Block (a) resolver 梯队迁入、cli.py 注册表化、拆分波次）→ B2 尾（mypy orchestrator 清零、法证打捞、簇归属核验、test_pipeline 删除关 R-009）→ B4/B5（Step 4 keyword_union、切片 3 回涨归位+防再涨棘轮、Step 5 设计议案）。
②**强 Agent 改造**——master plan **§6.5 WS7**（operator 指令 2026-07-22 的唯一权威转录）：fetch-profile 状态机/调度器 Slot 即时填充、AI-Native 批次划分（4-8 批、1~2 轮 HarvestAPI 预算、保留 url 级失败记录+末尾统一重试）、AI-Native materialize→promote 判断（请求参数精细记录替代硬规则）、原子化补偿机制（补充 acquire/fetch/materialize/promote+失败 recovery）、后链路两层行为抽象（个人信息补充一次性层 vs 特定方向收集判断层；X-First 划分规则先调研；semantic scholar 未来同法纳入）。从 W7.0 read-only 侦察批起步；**合同级设计一律议案+operator 裁决后动刀**。
③**harness+playbook 常驻**——每次踩坑固化为 gate/registry/棘轮；验证过的模式蒸馏回 ai-assisted-engineering-playbook（sibling repo，动前先 pull）。

## 每次会话启动

1. 读状态链：PROGRESS.md → NEXT_TODO.md → master plan §7 批次状态 → RESIDUAL_LEDGER（R-035 等 open 行）→ `sourcing-ai-agent/.coord/BOARD.md`（gitignored，git 为准）。
2. 实测校验：git status/log 与快照一致；PG 可达（`set -a; source sourcing-ai-agent/.local-postgres.env; set +a` 只读探测）；daemon 状态。声称与观察不符→先报差异。
3. 检查评审 re-fire 队列（NEXT_TODO Blocked 区）：chshapi 配额恢复即发队列中的 pin。
4. 取 §7 第一个未勾批次（R-035 优先，因其钉着 B3 签核）继续。批次内自治推进，不逐步请示。

## 工作纪律（AGENTS.md 全款适用，以下是本 goal 的强调项）

- **先探索定方法再执行**：任何未量化成本画像的重型命令，先小样本/dry-run/探针（教训：rebuild 75 分钟事故 vs live_promote 秒级）。
- **归因先于修复**：失败先对照实验定引入点（worktree+资产软链法已验证；归因错了要在 ledger 勘误，如 R-035 两轮归因）。
- 逐字迁移+oracle 门（tick oracle 永不改）；状态机/调度器改造前先建 characterization 基线；机械交叉扫描用**全 AST 未解析名**而非 def-only（harvest 拆分教训：漏导入名被宽异常吞成假完成）。
- 守卫的拒绝是信息不是障碍：不绕守卫，先读懂它为什么拒。
- 评审门协议：契约级改动实现+定向测试+pin+评审请求入队后继续下批；verdict 等 chshapi；NO-GO 只冻所涉 scope。
- WS7 特别款：AI-Native 设计（batch 划分/promote 判断/行为层）动刀前必须 AskUserQuestion 出裁决；先取现行行为实测基线再提案；live 验证等 HarvestAPI 配额+operator 明示。
- 每批收尾：NEXT_TODO/PROGRESS 刷新、§7 打勾、残差入 ledger 带 tripwire、跨会话事实进 memory；file-scoped 原子提交，禁 `git add .`；棘轮数字只降不升。
- 卡在配额/网络墙时转 local-only 批次，墙况记 NEXT_TODO，不空转。重侦察/审计可用多 agent 工作流，机械小批次不用。

## 红线（永不例外）

外部 provider FAIL-CLOSED，三重门需 operator 明示；付费派发先盘点、delta-only、终态命令永不 retry/resume（cmd_a32cc93e15b5f52150ac4da0 / cmd_1330a600a2f91e5c0617a4b7 永禁）；live-ops 只跑 committed scripts/；PG 变更先列出再执行、mutation 走事务+rowcount 核验（schema `sourcing_live_tml_path_20260719`）；禁全量 test_pipeline.py（直至删除收口）；不动 proxy/VPN/Clash；premise 冲突停下来问，绝不静默扩大付费范围。

补充指令：$ARGUMENTS
