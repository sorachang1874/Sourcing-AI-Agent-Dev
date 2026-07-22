# 新 Codex Desktop Session 启动 Prompt（Track C / Track D）

> Status: Implementation record (Track C increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

```text
你现在接管 sourcing-ai-agent 的 Track C / Track D；另一个活跃 session 独占 X-First，禁止触碰
x-first-researcher-sourcing/。

工作区：
/Users/changyuyi/projects/Sourcing AI Agent Dev

主目录：
/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent

分支：
governance-phase0-ttl-20260611

固定提交：
- D-3 implementation: 82d69a1
- D-3 evidence / 旧 handoff: f7a642b
- Track C C2.7 current candidate: 4b2370aadfeaf113b28317bae1f7e918fdfb3b3b
- Track D D1b current candidate: 97a81d04db579ef9edb1914d8567ef77af2b07c7

第一步不要改代码。依次阅读：
1. 工作区 AGENTS.md 与 sourcing-ai-agent/AGENTS.md
2. docs/HANDOFF_CODEX_DESKTOP_TRACK_C_D_SPLIT_2026-07-14.md
3. docs/TRACK_C_C2_6_AUTH_OWNER_FENCING_IMPLEMENTATION.md
4. docs/TRACK_C_C2_7_CONDITIONAL_OWNER_CLOSURE_IMPLEMENTATION.md
5. NEXT_TODO 的 Track C / Track D 与 RESIDUAL_LEDGER 的 R-019/R-023/R-027/R-028
6. Track D 开工前再读 TRACK_D_AGENT_RUNTIME_PLAN §5a/§6、DESIGN_INVARIANT_CHECKLIST、D1a、D1b 文档

然后只读报告：HEAD、branch、dirty files、4b2370a/97a81d0 是否存在、R-027/R-028 状态、
~/.codex/config.toml top-level model/effort/tier。共享树可能有另一个 session 的 X-First tracked dirty；
不要 stash/reset/format/stage 它们。

当前唯一实现批是 Track C C2.8：
4b2370a 的 pinned review 为 NO-GO（P0/P1/P2/P3=0/1/0/0）。唯一 finding 是 criteria owner
preflight 被 rerun_retrieval 错误控制。authenticated feedback/recompile/suggestion review 在 flag 缺省或 false
时仍可对 foreign/missing job/source job 写主域数据。

先 Scout 全部调用方。修复原则：
- rerun_retrieval 只控制 rerun，不控制 authorization；
- 任何显式 job_id/baseline_job_id/suggestion source job 都必须在所有主域写之前 exact-owner preflight；
- foreign/missing 返回同一 job_not_found，且 feedback/review/version/compiler/result/derived-job 零写；
- automatic baseline 保持 requester+tenant scoped；
- no-job-ref 合法请求与 open-mode operator 兼容不变。

补 missing/false × foreign/missing 的 feedback、recompile、suggestion 零写矩阵，以及 same-owner、no-ref、
open-mode 正向。同步 backend/tests/docs，独立提交并发起 fresh pinned non-author review。不要重开已闭合的
promotion、typed CAS、CRM 409 或 daemon allowlist，除非新证据直接要求。

4b2370a 作者证据：fast+transport 109+65 subtests、PG 71+4、CRM adjacency 2、CRM runtime 34、
pipeline exact 4 pass/2 clean-97a81d0-identical、lint green、mypy 81 errors/4 files。

97a81d0 D1b 已取得 scope-local advisory GO：D1a+D1b 29、PG adjacent 15、command specs 16，
P0-P3=0；formal review 仍 pending，served Agent tool population 仍为零。C2.8 提交并发出 review 后，可继续
Track D read-only Scout，但必须从 Plan §6/OB-ID 推导下一有界 batch，不得猜 schema/owner/served predicate。

硬约束：
- 禁止 git add .；不删除或纳入四个已知杂散路径。
- 不 reset/revert/amend 其他 Agent 或 X-First 提交。
- 不修改 x-first-researcher-sourcing。
- 禁止 sourcing live provider/model；绝不设置三个 live 环境变量。
- 网络错误只读 make agent-network-preflight；不改 proxy/VPN/Clash/TUN/DNS/System Proxy。
- Python 用 PYTHONPATH=src .venv/bin/python；PG 用 make local-pg-up。
- 永不全量运行 tests/test_pipeline.py。
- 台账外失败在 clean worktree 跑同一精确节点对照，不 stash。
- mypy 上限 81 errors/4 files，只能下降。
- author evidence / local advisory review 不得写成 formal GO。

验证优先于叙述。完成时报告 impact、exact files、exact commands/counts、commit、review status 与剩余风险。
```
