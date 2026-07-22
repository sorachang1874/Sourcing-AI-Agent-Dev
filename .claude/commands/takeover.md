从上一个会话/agent 接手本工作区。执行 session-handoff skill 的「接手」流程：

1. 按序读入口链：AGENTS.md → PROGRESS.md → NEXT_TODO.md → sourcing-ai-agent/docs/README.md。
2. 实测校验交接声称：git status/log 与快照一致性；本地 PG 可达（`set -a; source sourcing-ai-agent/.local-postgres.env; set +a` 后 SELECT 只读探测）；daemon 进程状态与 PROGRESS.md 记载一致。
3. 汇报差异清单（声称 vs 观察），有前提冲突先停下来问。
4. 然后继续 NEXT_TODO.md 的第一项，除非用户另有指示：$ARGUMENTS
