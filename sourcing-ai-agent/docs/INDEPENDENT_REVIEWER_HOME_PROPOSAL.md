# Independent Reviewer 专属 CODEX_HOME 提案

> Status: Proposal for owner review（2026-07-13，起草 = Claude Fable 5；trust-root 变更，实施须由
> Codex 走正常批流程 + 独立评审，owner 裁决后生效）。可行性已实证（§3）。

## 1. 问题（2026-07-13 实测的三个故障根因）

1. **共享配置被 ChatGPT Desktop 自动切换**（owner 证实）：打开升级后的 ChatGPT Desktop（原
   Codex Desktop）会重写 `~/.codex/config.toml`——实测顶层 `model_reasoning_effort` ultra→medium、
   `service_tier` priority→default。runner 的哨兵集把 `default` 判为「未显式设置」
   （`run_independent_review_gate.py:24`，`_INHERITED_VALUE_SENTINELS = {"", "auto", "default",
   "inherit"}`），任何人此后发 canonical review 都被 fail-closed 拒发。
2. **codex CLI 双安装漂移**：`/opt/homebrew` npm 份曾停在 0.122.0 且 vendored arm64 二进制缺失
   （spawn ENOENT）；PATH 不含 `~/.local/bin` 的环境（make/非交互 shell）会解析到坏份。
   2026-07-13 已把 homebrew 份修到 0.144.3（注意：PATH 上的 `npm` 是 nvm 的，修 homebrew 份须
   显式 `/opt/homebrew/bin/npm i -g @openai/codex@<ver>`）。
3. **runner 多线程协议代差**：codex 0.144 app-server 的 reviewer 会开并行子线程（subreview），
   `turn/completed` 不再内联 items（`itemsView: notLoaded`）；runner 的 `turn_identity_exact` /
   `final_agent_message_exact` 校验在多线程 run 上 fail-closed（v4 绑定字段已部分适配但不完整）。
   实测：单线程 run 产有效 artifact（3 次），多线程 run 一律 invalid_transport（3 次，模型侧
   评审实际完成、内容可提取但不构成 gate 证据）。

## 2. 提案

- **Reviewer 专属 CODEX_HOME**：仓库内固定一个 reviewer home 目录方案——`auth.json`、`sessions/`
  等运行时文件软链回 `~/.codex/`，唯 `config.toml` 是 reviewer 专用副本（顶层显式
  `model = <最新>`、`model_reasoning_effort = <最高档>`、`service_tier = <显式非哨兵值>`）。
  Makefile 的 `independent-review-gate` target 默认注入 `CODEX_HOME=<该目录>`（runner 已读
  `CODEX_HOME`，`run_independent_review_gate.py:330-331`，零代码改动）。Desktop/开发 session
  切换共享配置不再影响评审。
- reviewer config 文件本身 = operator 拥有的 checked-in 工件（放 `configs/reviewer-codex/` 或
  等价路径；auth 软链在首次 bootstrap 脚本建立，不入库）；模型换代时改这一个文件并记录。
- **补充项**：(a) runner 多线程 transcript 校验补全（支持 subreview 线程树的身份绑定），或在
  reviewer config 里禁用子代理（若 codex 提供开关）二选一；(b) `REVIEW_TIMEOUT_SECONDS` 默认
  420s 过短，建议 target 默认 1800s；(c) Makefile 层同时前置 `PATH` 保障解析到健康 codex。

## 3. 可行性实证（2026-07-13 本 session）

影子 CODEX_HOME（scratchpad 内、结构与提案一致）驱动 canonical runner 共 4 次：全部通过配置
校验并完成评审；其中 2 次（单线程 run）产出**完全有效** artifact（`runtime/reviews/
20260713T122908Z_*`、`20260713T131447Z_*`、`20260713T133324Z_*` 实为 3 份——metadata 含
`reviewer_model: gpt-5.6-sol / effort: ultra / tier: priority`、`reviewer_exit_code: 0`、
hash-bound effective-config/rollout、pinned scope digest），证明：软链 auth/sessions + 专用
config 的组合被 codex app-server 与 runner 的证据链完整接受，rollout 持久化与校验均正常。

## 4. 实施与边界

- 实施 = Codex 正常批（Makefile + bootstrap 脚本 + 文档更新 `INDEPENDENT_REVIEW_GATE.md`
  §Codex Reviewer Command），trust-root 变更走独立评审；owner 裁决 reviewer config 的初始值。
- 不改变 gate 的信任模型：reviewer config 仍是 operator 拥有物；本提案只是把它从「与开发/Desktop
  共享的易变文件」隔离成「评审专用的稳定文件」。
- 相关经验（含 §1 三项发现与提取件流程）待回写 `ai-assisted-engineering-playbook`（独立任务，
  fresh session 执行，先按惯例 pull GitHub 最新）。
