# Sourcing AI Agent — workspace

```
owner: operator   last-verified: 2026-07-22   route-audit: 随 docs/HARNESS_REORG_DESIGN.md 各阶段
```

AI 驱动的人才 sourcing 流水线：LinkedIn roster/profile 采集（HarvestAPI/Apify）→ 华人
线索分层（L1–3）→ X-First 账号发现 + LLM judge（v1 引用契约）→ 13 列 CRM+X-First CSV
交付，底座是 PG-only 的 durable workflow 引擎。

## 入口链（按序读）

1. [AGENTS.md](AGENTS.md) — 工作区工程规则（CLAUDE.md 为 Claude Code 镜像其关键条目）。
2. [PROGRESS.md](PROGRESS.md) — 有界的当前状态快照；[NEXT_TODO.md](NEXT_TODO.md) — 有界的工作队列。
3. [sourcing-ai-agent/docs/README.md](sourcing-ai-agent/docs/README.md) — 文档路由器：问题 → 归属模块 → canonical 文档。

多 agent 实时协调走 gitignored 的 `sourcing-ai-agent/.coord/`（短暂通道）；**git 为权威**，
持久决定必须提升到上面的快照与模块文档。

## 布局

| 路径 | 内容 |
|---|---|
| [sourcing-ai-agent/](sourcing-ai-agent/AGENTS.md) | 主包：采集、workflow runtime、存储、serving、live-ops 脚本 |
| [x-first-researcher-sourcing/](x-first-researcher-sourcing/AGENTS.md) | X-First 包：Grok 采集、judge 契约、批处理 runner |
| archive/legacy-research-2026-04/ | 4 月研究资产冻结归档（pipeline 前时代） |
| ai-assisted-engineering-playbook/ | 实践手册姊妹仓（独立 git，本仓不跟踪） |
| .worktrees/ | lane 工作树（独立 git 上下文，本仓不跟踪） |

## 环境与数据边界

- Python 一律用各包 `.venv`（不要直接用 Homebrew python）。本地控制面 Postgres：
  `make -C sourcing-ai-agent local-pg-up`（env 在 `sourcing-ai-agent/.local-postgres.env`，
  需 `set -a` 方式 source）。验证命令与测试 lane 规则见 AGENTS.md 与路由器 testing 路由。
- 外部 provider 默认 **FAIL-CLOSED**——live 访问需要 provider 模块文档描述的三重门 env。
- git 只装代码、文档、示例配置；`runtime/`（live 数据、快照、secrets、provider cache）
  永不入库，跨设备恢复走独立安全存储；`runtime/secrets/providers.local.json` 换机重配。
