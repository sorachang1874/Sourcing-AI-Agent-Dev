# GPT-5.6 接管简报（2026-07-09）

> Status: Current handoff brief. 本文档是 GPT-5.6(Codex)接管重构工程的唯一进场入口，由离任的
> Claude 会话于 2026-07-09 编写。读完本文再按 §2 的顺序读其余文档；与旧文档冲突时以本文 + §2 清单为准。

## 1. 你接手的是什么

一个招聘自动化/公开资料补全产品（`sourcing-ai-agent/`），正处于服务化重构中程。五轨道结构
（`docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md` 2026-06-11 revision）：Track A（orchestrator 分解，
Phase 0–4 已收官）、Track B（存储 PG-pure + God-class 拆解，**当前主战场**）、Track C（serving，
C1/C2 已落）、Track D（强 Agent 化，未开）、Track E（治理）。

**当前分支 `governance-phase0-ttl-20260611`**（git root 是工作区目录，不是 sourcing-ai-agent/）。
2026-07-09 已落的关键提交：`d5109f1`（Track B ②.0+②.1 域退役）、`430a369`（provider fail-closed
合入）、`8b555b6`（合同 lane skip→fail 加固）、`6e2526a`（协议升级：残差台账/决策卡/模型路由/评审接入）。

**Track B ② 轮现状**：`storage.py`（PG-pure，15,091 行）按域退役到 `store.repos.<domain>`。
②.0 linkedin_profile_registry、②.1 criteria/confidence 已完成并提交。**你的第一批 = ②.2
manual_review**，按 handbook §3 顺序与 §4 协议逐字执行。

## 2. 阅读顺序（第一个 session 必读）

1. 工作区 `AGENTS.md` + `sourcing-ai-agent/AGENTS.md`（行为规则、review gate、workflow 契约）
2. `docs/TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md` —— ② 轮入口执行手册：§3 域顺序、
   **§4 每批七步协议（含 A/B 电池 + 变异自检 + step 7 异步评审）**、§6 付过学费的 gotchas、§7 决策卡
3. `docs/TRACK_B_PG_PURE_STORE_DESIGN.md` §6 —— 末两条批记录（②.0/②.1）是 ②.2 的直接模板
4. `docs/RESIDUAL_LEDGER.md` —— 已知失败/残差唯一台账（green-modulo-ledger 验收；新失败先归因再入账）
5. `docs/NEXT_TODO.md` + `PROGRESS.md`（2026-07-09 条目）—— 全局轨道状态
6. `docs/INDEPENDENT_REVIEW_GATE.md` + `docs/INDEPENDENT_REVIEW_BRIEF.md` —— 评审协议与模型路由表
7. 动 repository 代码前：`src/sourcing_agent/control_plane_repository.py`（基类原语）+
   `src/sourcing_agent/repositories/criteria_confidence.py`（②.1 成品形态）

## 3. 硬约束（违反任何一条都可能造成真实损失）

- **Provider 安全（2026-06-27 计费事故后 fail-closed）**：`SOURCING_EXTERNAL_PROVIDER_MODE` 未设
  即 `simulate`；**永远不要**设置 live 模式/双钥确认变量（`SOURCING_LIVE_PROVIDER_CONFIRM`、
  `SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS`）除非 owner 明确要求本次 live 验证。测试一律仿真。
- **PG-only**：`ControlPlaneStore` 需要已解析 DSN + postgres_only；本地 `make local-pg-up`（容器
  55432），测试 env 见 handbook §5。禁止引入任何 SQLite 正常路径。
- **git 纪律**：不 `git add .`（工作区有 4 个已知不入库的杂散路径：`.github/workflows/containerized-pre-release.yml`、
  `ai-assisted-engineering-playbook/`、`sourcing-ai-agent/configs/scripted/samples/`、`sourcing-ai-agent/logs/`）；
  按显式路径 stage；不可重建资产的破坏性操作走 reviewed 流程。
- **测试纪律**：targeted 优先；`tests/test_pipeline.py` 永不全量跑（>90 分钟，非 lane，台账 R-009）；
  验收门 = `make ci-pre-agent-contract`（REQUIRE flags 已内建，PG 不可用会红）+ 域直连套件；
  失败 ⊆ `RESIDUAL_LEDGER.md` 未 closed 行 = 绿，否则现场 **git worktree 基线对照**归因（勿 stash）。
- **lint 门**：`run_python_quality.sh` ruff 段全过；mypy 段与台账 R-011 基线（87 条）**只降不增**。
- **评审独立性反转**：你（GPT/Codex）现在是**作者家族**。`INDEPENDENT_REVIEW_GATE.md` 模型路由表
  写于 Claude 为作者的时代（reviewer = GPT-5.5）——**你的第一个 session 应把该表的 author/reviewer
  家族对调**（评审 lane 主/兜底改为非 GPT 家族，如 Claude；不变量 = 评审 lane 永不落回作者家族），
  这是一处一行级文档修改，做完记入批记录。
- **网络**：`make agent-network-preflight` 会因预置 proxy env 报 FAIL，但实际连通正常——**只读报告，
  不得改 proxy/VPN/Clash 状态**。

## 4. ②.2 manual_review 批的执行骨架（handbook §4 的具体化）

1. **Scout**：`grep -n "def .*manual_review" src/sourcing_agent/storage.py` 清单域方法；全仓调用方
   （直调 + getattr 特征）机械生成 rename map（**不得手打**——②.1 手打漏项事故，§6(d)）。
2. **Repository 落地**：形态照抄 `criteria_confidence.py`（模块级 `_row_value` 逐字复制 storage 版、
   fail-closed 原语、`_call_native_write`、跨域依赖走回调注入——repo 永不 import storage）。
   mapper 是否 descriptor 化先看 §6 ②.1(a) 的 None-直通判据，拿不准就逐字保留。
3. **A/B 验证**：读面同参深拍 + 哨兵 tier 断言；写面同脚本双跑字节比对（三层冻钟 + 租约远期 +
   独立 sequence 显式 RESTART，§6 ②.0/②.1 条）；**电池自检：先破坏一处确认变红**。A/B 文件临时，跑完删。
4. **切换 + 删除**：同批完成 caller 直迁 + storage 方法删除，不留双轨；fan-out 编辑时 worker 带
   「不在映射表→不猜、上报 ambiguous」条款，收口逐条核对。
5. **lane + 套件**：`make ci-pre-agent-contract` + 域直连套件 + 被改测试文件 + lint 门。
6. **文档**：TRACK_B doc §6 追加批记录；扫台账 tripwire 列；协议修订则更新 handbook §4。
7. **异步参考评审**：批 settle 后按 gate 文档发非阻塞跨模型评审（注意 §3 的家族反转）。

## 5. 待 owner 决策（不阻塞 ②.2，但到期要催）

见 handbook §7 三张决策卡：D-1 ③ jsonb/timestamptz 窗口（截止 2026-07-31）、D-2 on-disk-SQLite
工具退役（随 D-1）、D-3 lovable_board 分子契约（截止 2026-07-31；关联台账 R-001/R-007）。

## 6. 环境速查

```bash
# python:仓内 .venv(裸 python 不在 PATH)
cd sourcing-ai-agent && PYTHONPATH=src .venv/bin/python -m pytest tests/<suite> -q
# 本地 PG
make local-pg-up        # psql/pg_dump 在 /opt/homebrew/opt/postgresql@16/bin
# 合同 lane(≈4 分钟)
make ci-pre-agent-contract
# postgres_only 直连套件 env:见 handbook §5
# 服务状态
bash ./scripts/dev_status.sh --runtime-dir runtime --api-port 8765 --frontend-port 4173
```

## 7. 上一任留下的口径

- 「验证优先于速度」：每批的验收数字（lane 计数、A/B 电池、worktree 对照、mypy 计数）写进批记录，
  下一个 session 靠这些数字而不是叙述接续。
- 「机器能变红的规范才存在」：新纪律尽量落成守卫/flag/棘轮，纯散文条款视为未实施。
- 「残差引用 id，不重新诉讼」：评审/审计遇到已接受残差，引台账行 id 即过。
