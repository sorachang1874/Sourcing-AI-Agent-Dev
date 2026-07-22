---
name: review-gate
description: 发起或修复独立评审门（canonical review runner、影子 CODEX_HOME、review artifact 判读）。当用户要求发独立评审/re-fire 评审队列、评审 runner 报 invalid_transport/拒发，或提到 pinned review、GO/NO-GO 裁决时使用。
---

# 独立评审门运维

前提：chshapi 配额（先探测再 re-fire；冻结时把评审排进 PROGRESS.md 的 re-fire 队列）。

## 发前检查（每条都是实测学费）

1. **config 被 Desktop 重置了吗**：ChatGPT Desktop 打开即重写 `~/.codex/config.toml`
   （effort→medium、service_tier→"default"，runner 视 "default" 为未设置而拒发）。
   解法：reviewer 专属 CODEX_HOME（`scripts/bootstrap_reviewer_codex_home.sh`；
   设计 `docs/INDEPENDENT_REVIEWER_HOME_PROPOSAL.md`）。
2. **超时**：`REVIEW_TIMEOUT_SECONDS=1800`（默认 420s 对 ultra 档太短）。
3. **双 codex 安装**：`~/.local/bin/codex` 是 wrapper（nvm 优先）；升级 homebrew 份要
   显式 `/opt/homebrew/bin/npm i -g @openai/codex@<ver>`。

## 判读

- 多线程 reviewer run 会让 `turn_identity_exact`/`final_agent_message_exact` fail-closed
  （协议代差）：内容可从 events.jsonl 提取（`item.type=="agentMessage"`）存为
  `*.extracted-reference.md`，明确标注**非 gate 证据**；gate 证据只认单线程有效 artifact。
- `invalid_transport` 先看 events.jsonl 有无完成的 turn，再归因网络。
- 对抗评审循环不收敛是常态（散文层无不动点）：终止靠 owner-accepted exception，
  剩余 findings 转实施批开工义务。

## References

- 门定义与适用范围：`sourcing-ai-agent/docs/INDEPENDENT_REVIEW_GATE.md`
- 设计不变量矩阵（评审前自查）：`sourcing-ai-agent/docs/DESIGN_INVARIANT_CHECKLIST.md`
