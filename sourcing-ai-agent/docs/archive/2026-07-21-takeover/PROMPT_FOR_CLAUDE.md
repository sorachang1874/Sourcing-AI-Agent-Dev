# 发给 Claude 的接管 Prompt（可直接复制）

> Status: Archived (2026-07-22). One-shot takeover prompt of 2026-07-21; superseded by workspace AGENTS.md/CLAUDE.md entry chain.

---

你要接管一个生产中的 AI Lab 人才 sourcing 项目（Harvest/Apify 采集 → 华人分层 → X 账号定位 + pre-train 判断 → CRM CSV）。

**先读这三样，再动手**（都在 `/Users/changyuyi/projects/Sourcing AI Agent Dev/`）：
1. `HANDOFF_TO_NEXT_AGENT.md` — 项目全貌、交付状态、运行时拓扑、工具链、9 条不可违反的合同、全部踩坑记录和挂起项
2. `sourcing-ai-agent/.coord/BOARD.md` — 单一事实源（lane cards、directives、defect log）
3. `sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md` — actor 输入契约、现场纪律、committed 脚本登记

**当前状态**：6 个 Lab（TML/OpenAI/GDM/xAI/Anthropic/Meta-TBD）全流程已交付（各 67/950/1491/454/344/33 行 13 列 CSV，路径在 handoff §2）。代码改动均未 commit（在工作区），测试基线：sourcing-ai-agent 主要电池全绿（144 单测 + 3 pipeline + 127 connector + 38 recovery）；x-first 666 测试绿。

**工作规则（硬性）**：
- 任何付费 API 调用前：盘点本地快照 + 远端 Apify run 历史，只发 delta；salvage 优先；dry-run 展示 payload 后再发；扩大付费范围前先问。
- functionID（engineer "8" / researcher "24"）永远分开不合并；former lane 同样 per-function；广召回默认无 keywords；去重先于 fetch。
- 用 `sourcing-ai-agent/scripts/live_*.py` 这套 committed 脚本驱动 live ops，不写 /tmp 临时脚本；新增 live 动作两次以上必须落成 committed 脚本并在 playbook 登记。
- 停远端 Apify run 用 `/v2/actor-runs/{id}/abort`；改完请求形态代码先重启后端（进程 vintage 检查）；PG schema 必须是 `sourcing_live_tml_path_20260719`。
- judge 一律走 committed 的 DeepSeek binding + supporting_context（raw profile 注入）；别动 v1 citation/pins/reducer。

**优先接手的挂起项**（handoff §7 有细节）：
1. chshapi 额度恢复探测 → 重发 5 个排队 review + former per-function 合同改造的独立 review
2. scoped_search_roster 的 functionIds 合并合同改造（per-function scoped shards + 测试）
3. OpenAI candidate_documents 身份合并（39 重复人 vs 补漏 40 的撞号）
4. hosted smoke harness 腐化诊断（HTTP 410，HEAD 上就红）

遇到指令前提与观察状态冲突时，停下来问，不要静默扩大范围。有任何不确定，先查 `.coord/BOARD.md` 和 handoff，再问我。
