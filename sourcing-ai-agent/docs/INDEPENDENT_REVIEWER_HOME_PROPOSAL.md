# Independent Reviewer 专属 CODEX_HOME

> Status: owner-approved and implemented（2026-07-13 提案，2026-07-15 hardening）。默认 Make 路径
> 使用 reviewer 专属 config，不修改 ChatGPT Desktop 使用的 `~/.codex/config.toml`。

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

## 2. 已实施方案

- **Reviewer 专属 CODEX_HOME**：`scripts/bootstrap_reviewer_codex_home.sh` 构建
  `runtime/reviewer_codex_home_v2/`。`auth.json`、`sessions/` 等顶层运行时状态软链回 source Codex
  home，唯 `config.toml` 是私有生成文件；它从当前 source config 派生，并强制覆盖顶层
  `model`、`model_reasoning_effort`、`service_tier`。Makefile 的 `independent-review-gate` target
  默认先 bootstrap，再注入 `CODEX_HOME=<该目录>`。Desktop/开发 session 切换共享配置不会再
  改变 reviewer policy trio。
- reviewer policy = operator 拥有的 checked-in `configs/reviewer-codex/reviewer.toml`；模型换代时
  改这一个文件并记录。生成目录固定 `0700`、生成 config 固定 `0600`，bootstrap 使用
  `umask 077`。
- source home 保持只读；bootstrap 不删除或替换 target 中的同名真实文件/目录。如果发现这种
  冲突会在创建任何 source-entry 软链前 fail closed，避免 `ln -sfn` 在真实目录内生成 nested
  self-link。operator 可保留原目录并选择新 `REVIEWER_CODEX_HOME`，或在外部人工完成迁移。
- 这是 **config isolation**，不是全状态隔离：登录、session/rollout 与其他顶层 Codex 状态仍通过
  软链共享，避免猜测或复制 Codex 未公开的运行时要求。
- **一次性 legacy cutover（2026-07-15）**：pre-hardening
  `runtime/reviewer_codex_home/` 已存在真实 `mcp-oauth-locks/`，其内有旧 `ln -sfn` 生成的同名
  nested symlink。新 fail-closed bootstrap 不应跳过冲突，也不应删除或改名现有 runtime 状态，故
  canonical default 一次性切到干净且稳定的 `runtime/reviewer_codex_home_v2/`。旧目录保持原样，
  不再被 bootstrap/Make 隐式读取或修复；未来如需清理须作为独立、显式的 runtime retention 操作。
- **补充项**：(a) runner 多线程 transcript 校验补全（支持 subreview 线程树的身份绑定），或在
  reviewer config 里禁用子代理（若 codex 提供开关）二选一；(b) `REVIEW_TIMEOUT_SECONDS` 默认
  已改为 1800s；(c) launcher/PATH 解析仍由 runner 的既有选择与证据链负责，本批不复制 Codex
  安装或猜测额外 runtime requirements。

## 3. 可行性实证（2026-07-13 本 session）

影子 CODEX_HOME（scratchpad 内、结构与提案一致）驱动 canonical runner 共 4 次：全部通过配置
校验并完成评审；其中 3 次（单线程 run）产出**完全有效** artifact（`runtime/reviews/
20260713T122908Z_*`、`20260713T131447Z_*`、`20260713T133324Z_*`——metadata 含
`reviewer_model: gpt-5.6-sol / effort: ultra / tier: priority`、`reviewer_exit_code: 0`、
hash-bound effective-config/rollout、pinned scope digest），证明：软链 auth/sessions + 专用
config 的组合被 codex app-server 与 runner 的证据链完整接受，rollout 持久化与校验均正常。

## 4. 实施与边界

- 已实施范围 = Makefile + bootstrap 脚本 + focused temp-home regression +
  `INDEPENDENT_REVIEW_GATE.md` §Codex Reviewer Command；trust-root 变更仍走独立评审。
- 不改变 gate 的信任模型：reviewer config 仍是 operator 拥有物；本提案只是把它从「与开发/Desktop
  共享的易变文件」隔离成「评审专用的稳定文件」。
- 相关经验（含 §1 三项发现与提取件流程）待回写 `ai-assisted-engineering-playbook`（独立任务，
  fresh session 执行，先按惯例 pull GitHub 最新）。
