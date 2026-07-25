---
name: session-handoff
description: 会话交接与接手（takeover intake、快照刷新、状态提升）。当用户说交接给下一个 agent、从上一个 agent 接手、写 handoff，或会话临近结束需要固化状态时使用。
---

# 会话交接 / 接手

## 交出（本会话结束前）

1. 刷新工作区快照（替换不追加，守预算）：`PROGRESS.md`（≤200 行）+
   `NEXT_TODO.md`（≤120 行）——这是下个会话的第一入口，不写单独的 HANDOFF 文件。
2. `.coord/BOARD.md` 只留 live lane 状态；持久决定提升进 git（快照/模块文档/commit）。
3. 未提交的工作要么切片提交、要么在 NEXT_TODO 里标注 worktree/分支位置；
   绝不把可恢复状态只留在会话上下文里。
4. 大宗证据（审计 JSON、清单）放 operator memory 的 artifacts 目录并在快照里留指针。

## 接手（新会话开始）

1. 走入口链：`AGENTS.md`/`CLAUDE.md` → `PROGRESS.md`/`NEXT_TODO.md` →
   `sourcing-ai-agent/docs/README.md` 路由器 → 模块索引。
2. 校验声称 vs 现实：交接文档里的测试基线、进程状态、数据状态先实测再采信
   （2026-07-21 接管实测推翻了交接的测试数字）。
3. 前提与观察冲突时停下来问，不静默扩付费范围。

## References

- 交接模板族（如需正式 packet）：`ai-assisted-engineering-playbook/templates/`
  （SESSION_TASK_PACKET / COORD_BOARD / PROGRESS / NEXT_TODO）
- 历史交接的归档位置：`sourcing-ai-agent/docs/archive/`
