# Sourcing AI Agent

一个面向 Agent 化开发的通用 Sourcing 后端 MVP。当前版本先基于 `Sourcing AI Agent Dev` 中已经沉淀的 Anthropic 调研资产，把更完整的 workflow 跑通：

`用户意图 -> criteria 澄清 -> acquisition plan -> 异步 sourcing workflow -> retrieval -> result artifact`

开始在这个子项目里改代码之前，先阅读 monorepo 根目录的协作规则 [../CONTRIBUTING.md](../CONTRIBUTING.md)。

## Documentation Map

当前有效的文档入口：

- [../ONBOARDING.md](../ONBOARDING.md)
- [PROGRESS.md](PROGRESS.md)
- [docs/INDEX.md](docs/INDEX.md)
- [docs/FRONTEND_API_CONTRACT.md](docs/FRONTEND_API_CONTRACT.md)
- [docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md](docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md)
- [docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md](docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md)

前端集成若需要直接复用类型和调用示例，可参考：

- [contracts/frontend_api_contract.ts](contracts/frontend_api_contract.ts)
- [contracts/frontend_api_adapter.ts](contracts/frontend_api_adapter.ts)
- [contracts/frontend_react_hooks.example.tsx](contracts/frontend_react_hooks.example.tsx)

历史 handoff / retrospective / todo 文档仍保留，但已经在 [docs/INDEX.md](docs/INDEX.md) 中标记为 reference-only。

## 当前目标

- 把历史调研资产转成结构化候选池，而不是继续依赖对话上下文
- 把 Anthropic 特例抽象为通用 workflow engine，可扩展到 xAI 等其他公司
- 把 acquisition 作为第一等公民，而不是只做最后一步检索
- 先跑通本地数据驱动 MVP，再接入外部 source adapters 与 Claude Code

## 当前能力

- 自动发现并读取 `Anthropic华人专项` 解压后的工作簿和 JSON 资产
- 将在职员工、离职员工、投资方成员、Scholar 线索导入 SQLite
- 支持从原始用户请求生成 sourcing plan
- plan 阶段新增 `intent_brief`
  - 显式输出 `identified_request / target_output / default_execution_strategy / review_focus`
  - 用于产品原生第一段交互，也方便后续修正 Claude / Qwen 的意图识别表现
  - `planning_mode=heuristic` 走纯 deterministic planning
  - `planning_mode=model_assisted` 先让模型做请求归一化，再走 deterministic brief / intent / search planning
  - 若要实验模型直接写 brief / search planning，可使用更激进的 planning mode
- 支持 `Plan Review Gate`
  - plan 阶段会生成 `plan_review_gate`
  - 对 scoped roster、investor firm roster、高成本 source 等场景要求先 review 再执行 workflow
  - review 时可补充 `extra_source_families`、确认 `company_scope`、决定是否允许高成本 source
- 支持 `AcquisitionStrategyCompiler`，在 plan 阶段输出目标人群边界、roster 获取策略、低成本优先的 slug resolution 顺序和待确认成本点
- 支持 `PublicationCoveragePlanner`，在 plan 阶段输出 publication / engineering / blog / docs 等 source family coverage 方案
- 支持 `LLM-driven Search Planner`
  - plan 阶段会产出 `search_strategy`
  - 将 query 编译成具名 query bundle，例如 `relationship_web / publication_surface / public_interviews / targeted_people_search`
  - 支持把“公开访谈 / Podcast / YouTube”这类新 sourcing 方法沉淀为 source family，并进入 plan review / execution
  - 新增 source family 前，默认先进入用户交互：
    - 确认目标、覆盖范围、停止条件、成本容忍度
    - 确认是只做 surface-level asset capture，还是要继续做深挖
    - 确认新增链路是否满足数据资产落盘、可审计、可扩展三项约束
- 支持稳定的 `search provider abstraction`
  - low-cost search 已不再绑定到单一 DuckDuckGo HTML endpoint
  - 当前 provider chain 支持 `dataforseo_google_organic -> serper_google -> google_browser -> bing_html -> duckduckgo_html` 的优先级切换
  - `search_seed_discovery / slug_resolution / exploratory_enrichment` 已统一走同一套 provider 接口
  - search raw payload 现在会按 provider 的 `html/json` 形态落盘，便于缓存复用与审计
  - 当前环境下 DuckDuckGo 仍可能出现 TLS EOF，因此 production 推荐显式配置稳定 provider
  - 已接入 `DataForSEO Google Organic`：
    - 同步 lane 可走 `live/regular`
    - 低成本后台批量更推荐 `Standard Queue`
    - `search_planner / public_media_specialist / exploration_specialist` 的 worker 恢复链路已支持 `task_post -> tasks_ready -> task_get`
    - 已提供 `scripts/dataforseo_google_organic.py` helper 与 `docs/DATAFORSEO_PLAYBOOK.md`
  - `google_browser` lane 现已接到 Playwright/Chromium provider
  - 对于 OpenClaw 这类“无需 search API 也能做 Google Search”的 Agent，底层思路更接近 `browser automation / Playwright`，而不是普通 search API
  - 当前这台 WSL 机器上，browser lane 仍受系统共享库 `libnspr4.so` 缺失影响
  - 当前代码已补可执行的缺库报错提示，若 live Google query 失败，会直接指出缺失库与建议安装包
- 支持 `agent runtime`
  - workflow / retrieval 已按 specialist lanes 记录 runtime session 和 trace spans
  - 当前 lane 包括 `triage_planner / search_planner / acquisition_specialist / enrichment_specialist / exploration_specialist / retrieval_specialist / review_specialist`
  - investor / public media 场景会补充 `investor_graph_specialist / public_media_specialist`
  - `search_planner / public_media_specialist / exploration_specialist` 已支持并行 autonomous workers
  - worker 具备持久化 `budget / checkpoint / output / interrupt_requested`
  - 同一 `job_id + lane_id + worker_key` 现在支持 checkpointed resume
  - 已补 lane-aware scheduler：
    - 按 lane priority 和 resume mode 选择下一批 runnable workers
    - 默认优先 `reuse_checkpoint / resume_from_checkpoint`
    - 当前 scheduler 面向 search/public media/exploration 这三类 specialist lane
  - 已补 autonomous worker daemon loop：
    - search/public media/exploration 不再各自管理 retry，而是统一走 daemon loop
    - daemon 会按 lane budget cap 和 retry limit 做 arbitration
    - `failed -> retry` 与 `completed -> reuse output` 已进入执行链
  - 已补跨进程常驻恢复器能力：
    - worker 通过 SQLite lease 协调跨进程 claim / release
    - `stale running` worker 会被降级为可恢复态重新进入 scheduler
    - 可通过独立 CLI 进程运行 `run-worker-daemon` 持续恢复 backlog
  - 已补系统级常驻服务壳层：
    - `run-worker-daemon-service` 提供单实例锁、心跳状态和优雅退出
    - `write-worker-daemon-systemd-unit` 可生成 systemd unit
    - `show-daemon-status` 可读取 `runtime/services/<service_name>/status.json`
- 支持异步 workflow，显式区分 `planning`、`acquiring`、`retrieving`
- acquisition runtime 已开始按 strategy 分叉执行：
  - `full_company_roster` 走 company roster connector
  - `scoped_search_roster / former_employee_search` 走低成本 search-seed discovery
  - `investor_firm_roster` 走 tiered investor-firm workflow，并先生成 firm plan 再做后续筛选
- 支持 live company identity resolve + LinkedIn `company/people` roster acquisition
- 当 live roster connector 超限或返回空结果时，可回退到最近一次成功的本地 company snapshot
- 支持 provider-first 的 LinkedIn enrichment：
  - `web LinkedIn URL search -> search/people fallback -> /api/profile -> /people/profile`
  - 若已从 web search / publication / other source 拿到 slug 或 LinkedIn URL，则优先直接调用 profile detail
  - publication author / acknowledgement / co-author baseline
- 支持 `further exploration`：
  - 对 unresolved lead 或没有公开 LinkedIn 的候选人，继续抓取网页、X、GitHub、个人主页、CV/Resume 链接
  - 若后续发现 LinkedIn URL，可再次进入 profile detail acquisition
  - search-seed acquisition 已能从 `public_interviews / publication_and_blog` 这类低成本 query bundle 中生成 `public_media_lead`
  - 公开视频搜索结果会先保存 `public_media_results / public_media_analysis`，基于标题和摘要做初步关系判断
  - `Roster-Anchored Scholar Coauthor Expansion` 当前默认只产出 evidence-only prospects，不会默认对全量 prospects 自动发起 search follow-up
  - 若要真的跑 prospect follow-up，需要显式提供 `scholar_coauthor_follow_up_limit`
- 支持 model-assisted page analysis：
  - 以通用 `analyze_page_asset` 接口调用页面摘要/校验模型，不把实现写死为 Qwen
  - 当前已有 deterministic fallback，后续可并列接 Claude 等其他模型
  - `analyze_page_asset` 现已扩展输出：
    - `education_signals`
    - `work_history_signals`
    - `affiliation_signals`
    - `document_type`
  - exploration / manual review 现在不仅会保存页面摘要，也会把可结构化的 education/work history 草稿回写到 candidate
  - 已支持：
    - HTML homepage / CV
    - Google Docs CV
    - PDF resume 的文本提取链路
  - 当前 PDF 现为多级文本提取：
    - `pypdf`
    - `pdfminer.six`
    - `pdftotext`
    - `OCR (pdftoppm + tesseract)` 预留
  - `runtime/vendor/python` 可放本机增强解析依赖，不进入 Git
  - 对图片型 PDF，若本机无 OCR 工具，仍可能只能保留证据而无法自动补齐字段
- 支持 criteria evolution persistence：
  - 保存人工 review feedback
  - 持久化 alias / must_signal / exclude_signal 等 pattern
  - 当前 alias pattern 已可影响后续 retrieval
  - feedback 现会额外生成 `suggested patterns`，作为待 review 的建议层，不会直接污染 active patterns
  - suggestion 已支持 review loop：`suggested -> applied/rejected`
- 支持 criteria versioning / compiler run persistence：
  - 每次 plan / workflow / retrieval 都会保存 `criteria_versions`
  - 每次 criteria 编译都会保存 `criteria_compiler_runs`
- 支持 confidence labels 持久化：
  - 检索结果会输出并保存 `high / medium / lead_only`
  - 同时记录 `confidence_score` 与 `confidence_reason`
  - human feedback 现可继续演化 confidence policy：
    - `must_have_signal / false_negative_pattern` 会沉淀为 `must_signal + confidence_boost`
    - `exclude_signal / false_positive_pattern` 会沉淀为 `exclude_signal + confidence_penalty`
    - 后续 rerun 中，这些 pattern 会直接影响 `confidence_score / confidence_label`
    - 同公司历史 feedback 会按 precision / recall 压力自动微调 `high / medium` band 边界
    - confidence policy 现默认优先按 `request-family` 生效，而不是只按公司全局生效
    - 老 feedback 会按时间自动衰减，避免历史噪声长期主导 band 边界
  - 每次 retrieval / rerun 都会保存 `confidence_policy_runs`
  - 支持 manual policy freeze / override：
    - 可对 `request_exact / request_family / company` 三种 scope 显式加 control
    - `freeze_current` 会把当前自动生成的 band 阈值冻结为人工锁定版本
    - `override` 可直接指定 `high_threshold / medium_threshold`
    - retrieval 时会按 `request_exact -> request_family -> company` 优先级选择 active control
    - 所有 control 会持久化到 `confidence_policy_controls`
- 支持 centralized asset logger：
  - snapshot 内统一生成 `asset_registry.json`
  - company roster、search seed、profile payload、exploration page、analysis input/output、publication raw page 都会进入统一 registry
- 支持公司级历史资产物化与可复用候选文档提炼：
  - `build-company-candidate-artifacts` 现会聚合同一公司的历史 snapshot `candidate_documents.json` 与 SQLite 主库，而不是只读取最后一次覆盖进主库的数据
  - 会输出：
    - `materialized_candidate_documents.json`
    - `normalized_candidates.json`
    - `reusable_candidate_documents.json`
    - `manual_review_backlog.json`
    - `profile_completion_backlog.json`
  - `profile_completion_backlog` 会显式列出“已有 LinkedIn URL 但尚未拿到 full profile detail”的候选人，便于后续继续补全
- 支持公司级后处理资产补全：
  - `complete-company-assets` 会先物化公司历史候选池，再执行：
    - known-URL profile completion
    - unresolved lead exploration
    - follow-up profile completion
  - Thinking Machines Lab 当前已有单一 canonical snapshot，并显式拆分：
    - `canonical_merged`
    - `strict_roster_only`
  - 当前 authoritative counts、cloud bundle 和 asset-view 使用规则统一见：
    - `docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md`
    - `docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md`
  - 当前设备上 Harvest 配置入口已恢复到 `runtime/secrets/providers.local.json`
  - `2026-04-07` 新 Harvest token 已重新 smoke test 验证通过
  - backlog 不再在 README 中写死，以 snapshot 内 `normalized_artifacts/*_backlog.json` 和对应 validation note 为准
- 支持 `Manual Review Queue`
  - `lead_only`
  - 缺少 LinkedIn profile 的候选人
  - 需要人工进一步确认 membership 的 corner case
  - retrieval 结果和 `GET /api/jobs/{job_id}/results` 都会附带 `manual_review_items`
- 支持 criteria auto-evolution loop：
  - 人工 feedback 写入后，会自动触发一次 criteria recompile
  - 若 feedback 带 `job_id + candidate_id`，系统会结合 `matched_fields` 与 candidate context 生成 pattern suggestions
  - 若用户审核某条 suggestion 为 `applied`，系统会把它写入 active patterns，并可继续触发 recompile/rerun
  - 新版本会保留 `parent_version_id / trigger_feedback_id / evolution_stage`
  - 若请求里带 `rerun_retrieval=true`，会继续执行 retrieval rerun 并生成 result diff
  - rerun 现支持 `auto / cheap / full` 三档策略
  - `auto` 会按 feedback 类型、预估影响和 baseline request 规模决定是否 rerun，以及执行 cheap 或 full
  - `cheap` rerun 默认收紧 `top_k / semantic_rerank_limit`，并回退到 deterministic summary，避免重复支付模型摘要成本
  - 低信号 feedback 会被自动 gate 掉，避免无意义 rerun
  - baseline job 不再只是“同公司最近一次 job”，而是优先按 request-family 精确匹配
  - rerun 返回会显式带上 `baseline_selection`，说明它是按 exact request、exact family，还是 fallback 选出的
- 将外部 acquisition 结果版本化为 `runtime/company_assets/{company}/{snapshot_id}/`
- 将 company roster 归一化为可检索的 baseline candidate/evidence 文档
- 支持跨 snapshot 复用已拿到的 LinkedIn search/basic profile 资产，避免重复消耗 search 配额
- 支持按 `target_company`、`categories`、`employment_statuses`、关键词等条件运行 retrieval job
- 提供 retrieval strategy 抽象：`structured` / `hybrid` / `semantic`
- 已落地第一版 semantic/vector retrieval：
  - structured hard filters 之后，会进入本地 sparse-vector semantic retrieval
  - 当前使用 candidate multi-field document 的稀疏向量相似度做 recall / rerank
  - `hybrid` 现为 `hard filters + lexical/alias + semantic sparse-vector + confidence banding`
- 已补通用 `semantic provider` 抽象：
  - 默认 `LocalSemanticProvider` fallback
  - 已可切到 DashScope/Qwen 的 embedding + rerank
  - 当前默认配置面向 `text-embedding-v4 + gte-rerank-v2`，并预留 `qwen3-vl-rerank`
- 已补 autonomous worker 控制面：
  - `GET /api/jobs/{job_id}/workers`
  - `GET /api/jobs/{job_id}/scheduler`
  - `GET /api/workers/recoverable`
  - `GET /api/workers/daemon/status`
  - `POST /api/workers/interrupt`
  - `POST /api/workers/daemon/run-once`
  - `POST /api/workers/daemon/systemd-unit`
  - CLI: `show-workers / show-scheduler / interrupt-worker`
  - `GET /api/jobs/{job_id}/trace` 现会同时返回 `agent_workers`
- 预留 Qwen / Claude model provider 接口
- 支持 object storage durable sync：
  - `upload-asset-bundle / download-asset-bundle` 已支持并发 `max_workers`
  - 单对象上传/下载已补 retry/backoff
  - 默认会 resume，并跳过已完成且匹配 manifest 的对象
  - sync summary 会给出 progress / skipped_existing 统计
  - 本地会生成 `runtime/object_sync/bundle_index.json` 与 `runtime/object_sync/runs/*.json`
  - 云端会同步写入 `indexes/bundle_index.json` 与 `indexes/sync_runs/*.json`
  - Thinking Machines Lab handoff bundle 已完成真实 `R2 upload -> R2 download -> local restore`
- 已记录高质量 HarvestAPI 接入策略，供后续 Thinking Machines Lab 等小公司端到端验证使用：
  - intent-driven LinkedIn search，用于按用户意图定向检索在职 / 已离职 / 特定岗位人群
  - company employees actor，用于获取小中型公司的高质量 roster
  - profile scraper actor，用于按 LinkedIn URL 拉取 full profile detail，并利用 `moreProfiles` 做相似经历扩展
  - 已记录成本控制阈值：
    - 有 URL 时优先 profile scraper
    - company employees actor 仅用于批量场景
    - profile search actor 只在低成本 web search 不足时作为 fallback
- 已补 HarvestAPI live connector 基础层：
  - `HarvestProfileSearchConnector`
  - `HarvestCompanyEmployeesConnector`
  - `HarvestProfileConnector`
  - 当前 canonical 配置入口是 `runtime/secrets/providers.local.json`
  - 旧项目 token 自动发现只作为 recovery fallback，不应作为日常依赖
  - 已验证 Harvest search / company-employees actor 可真实调用并落盘 raw asset
  - 已确认参数差异：
    - `linkedin-profile-search` 可接受 `profileScraperMode=Short`
    - `linkedin-company-employees` 的 `profileScraperMode` 必须使用完整枚举，如 `Short ($4 per 1k)`
    - `linkedin-profile-scraper` 的 `profileScraperMode` 也必须使用完整枚举，如 `Profile details + email search ($10 per 1k)` 或 `Profile details no email ($4 per 1k)`
    - search / company 两类 actor 都需要满足最小 `maxTotalChargeUsd`，否则会返回 400
  - 已补 known-profile batch enrichment：
    - roster -> candidate 现会保留 `linkedin_url / metadata.profile_url`
    - 已知 LinkedIn URL 的 detail enrichment 现优先走 Harvest batch profile-scraper，而不是逐人串行调用
    - Thinking Machines Lab 最新 live snapshot 已成功解析 `12` 份 prioritized full profile detail
  - `2026-04-07` 已重新验证新 Harvest token：
    - `company-employees` live smoke test 可用
    - `profile-scraper` live smoke test 可用
  - 当前已确认 `profile-scraper` 的可用输入字段是：
    - `urls`
    - `publicIdentifiers`
    - `queries`
    - `profileIds`
    - 不应继续使用错误的 `profileUrls`
  - 已新增 [docs/HARVESTAPI_PLAYBOOK.md](docs/HARVESTAPI_PLAYBOOK.md)，沉淀 actor 链接、payload 规范、TML live 结论与当前已知坑

## 目录

```text
sourcing-ai-agent/
├── configs/
├── docs/
├── runtime/
├── src/sourcing_agent/
└── tests/
```

## Onboarding

如果你是新的开发者或新的 AI session，建议按这个顺序进入项目：

1. 根目录 [../ONBOARDING.md](../ONBOARDING.md)
2. [PROGRESS.md](PROGRESS.md)
3. [docs/INDEX.md](docs/INDEX.md)
4. [docs/MODULES.md](docs/MODULES.md)
5. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
6. [docs/HARVESTAPI_PLAYBOOK.md](docs/HARVESTAPI_PLAYBOOK.md)
7. [docs/DATAFORSEO_PLAYBOOK.md](docs/DATAFORSEO_PLAYBOOK.md)
8. [docs/TERMINAL_WORKFLOW.md](docs/TERMINAL_WORKFLOW.md)
9. [docs/FRONTEND_API_CONTRACT.md](docs/FRONTEND_API_CONTRACT.md)
10. [docs/QUERY_GUARDRAILS.md](docs/QUERY_GUARDRAILS.md)
11. [docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md](docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md)
12. [docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md](docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md)
13. 需要追旧决策或恢复旧环境时，再看 `docs/` 下的 dated reference 文档

## GitHub Sync Boundary

这个仓库会同步：

- 源代码
- 文档
- 示例配置
- 去敏后的历史方法论资产

这个仓库不会同步：

- `runtime/` 下的 live payload / company assets / profile raw assets / caches
- `runtime/secrets/providers.local.json`
- 历史 `api_accounts.json`

如果后续要在新的电脑上继续做 live test，需要单独恢复 secrets，以及按需恢复高价值 runtime 资产。

跨设备恢复的详细设计见：

[docs/CROSS_DEVICE_SYNC.md](docs/CROSS_DEVICE_SYNC.md)

## 快速开始

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli bootstrap
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_xai.json
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_thinking_machines_lab.json
PYTHONPATH=src python3 -m sourcing_agent.cli show-plan-reviews --target-company xAI --brief
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan --file configs/plan_review_approve.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan --review-id 12 --reviewer sora --instruction "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。" --preview
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan --review-id 12 --reviewer sora --instruction "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。"
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_xai.json
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --plan-review-id 12
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_anthropic.json
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_thinking_machines_lab.json
PYTHONPATH=src python3 -m sourcing_agent.cli run-job --file configs/demo_current_infra.json
PYTHONPATH=src python3 -m sourcing_agent.cli show-progress --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-trace --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-workers --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-scheduler --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-recoverable-workers
PYTHONPATH=src python3 -m sourcing_agent.cli show-daemon-status
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-snapshot-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli build-company-candidate-artifacts --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli complete-company-assets --company thinkingmachineslab --profile-detail-limit 12 --exploration-limit 2
PYTHONPATH=src python3 -m sourcing_agent.cli export-sqlite-snapshot
PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json
PYTHONPATH=src python3 -m sourcing_agent.cli download-asset-bundle --bundle-kind company_handoff --bundle-id <bundle_id> --output-dir /tmp/asset_imports
PYTHONPATH=src python3 -m sourcing_agent.cli restore-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json --target-runtime-dir /tmp/sourcing-agent-runtime
PYTHONPATH=src python3 -m sourcing_agent.cli restore-sqlite-snapshot --manifest runtime/asset_exports/<sqlite_bundle>/bundle_manifest.json
PYTHONPATH=src python3 -m sourcing_agent.cli interrupt-worker --worker-id <worker_id>
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-once
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon --poll-seconds 5
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
PYTHONPATH=src python3 -m sourcing_agent.cli write-worker-daemon-systemd-unit
PYTHONPATH=src python3 -m sourcing_agent.cli show-manual-review --target-company Anthropic
PYTHONPATH=src python3 -m sourcing_agent.cli review-manual-item --file configs/manual_review_resolve.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli configure-confidence-policy --file configs/confidence_policy_freeze.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli review-suggestion --file configs/suggestion_review_apply.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli serve --port 8765
```

## 模型配置

本地 secret 文件路径：

`runtime/secrets/providers.local.json`

示例格式见：

`configs/providers.local.example.json`

其中包括：

- `model_provider`
- `qwen`
- `semantic`
- `search_provider`
- `harvest`

通用模型 provider 读取优先级：

1. `MODEL_PROVIDER_API_KEY`、`MODEL_PROVIDER_BASE_URL`、`MODEL_PROVIDER_MODEL` 等环境变量
2. `runtime/secrets/providers.local.json` 中的 `model_provider`

当前已验证可用的 relay 形态：

1. OpenAI-compatible base URL：`https://tb.keeps.cc/v1`
2. model：`claude-sonnet-4-6`
3. `test-model` 会优先 healthcheck `model_provider`，若未配置则回退到 `qwen`

Qwen 读取优先级：

1. `DASHSCOPE_API_KEY` 等环境变量
2. `runtime/secrets/providers.local.json`

connector 账号读取优先级：

1. `runtime/secrets/providers.local.json` 中的 `connectors.rapidapi_accounts`
2. 自动发现 `Anthropic华人专项` 中已有的 `api_accounts.json`

semantic provider 推荐配置：

1. `semantic.embedding_model`：`text-embedding-v4`
2. `semantic.rerank_model`：`gte-rerank-v2`
3. `semantic.media_rerank_model`：`qwen3-vl-rerank`

Thinking Machines Lab 端到端测试建议入口：

1. 先执行 `plan --file configs/demo_workflow_thinking_machines_lab.json`
2. 完成 `Plan Review Gate`，确认 scoped roster、source families、paid source budget
3. 再执行 `start-workflow --file configs/demo_workflow_thinking_machines_lab.json`

## API

更明确的 Web 层消费约定见：

- [docs/FRONTEND_API_CONTRACT.md](docs/FRONTEND_API_CONTRACT.md)
- [contracts/frontend_api_contract.ts](contracts/frontend_api_contract.ts)
- [contracts/frontend_api_contract.schema.json](contracts/frontend_api_contract.schema.json)
- [contracts/frontend_api_adapter.ts](contracts/frontend_api_adapter.ts)

- `GET /health`
- `GET /api/providers/health`
- `POST /api/bootstrap`
- `POST /api/plan`
- `GET /api/plan/reviews`
- `POST /api/plan/review`
- `POST /api/workflows`
- `POST /api/jobs`
- `GET /api/jobs/{job_id}`
- `GET /api/jobs/{job_id}/results`
- `GET /api/jobs/{job_id}/trace`
- `GET /api/jobs/{job_id}/workers`
- `GET /api/jobs/{job_id}/scheduler`
- `GET /api/workers/recoverable`
- `GET /api/workers/daemon/status`
- `GET /api/manual-review`
- `POST /api/manual-review/review`
- `POST /api/workers/interrupt`
- `POST /api/workers/daemon/run-once`
- `POST /api/workers/daemon/systemd-unit`
- `POST /api/criteria/feedback`
- `POST /api/criteria/confidence-policy`
- `POST /api/criteria/suggestions/review`
- `GET /api/criteria/patterns`
- `POST /api/criteria/recompile`

`/api/criteria/feedback` 与 `/api/criteria/recompile` 支持可选参数：

- `rerun_retrieval`
- `job_id` 或 `baseline_job_id`
- `rerun_request_overrides`

`rerun_retrieval` 目前支持：

- `true` 或 `auto`
- `cheap`
- `full`

## 数据资产规则

- 默认 raw-first：外部 API 返回、web search HTML、profile payload、exploration page、analysis input/output 都先落盘，再进入归一化或模型分析
- Harvest connector 现支持 runtime 级 payload cache：
  - 优先复用当前 snapshot raw asset
  - 其次复用 `runtime/provider_cache/*`
  - 必要时桥接 `runtime/live_tests/*` 下已存在的手工 live 资产
  - 这样在 token 暂不可用或 provider 波动时，workflow 仍可回放既有高价值数据资产
- 公开访谈 / YouTube / Podcast 这类结果也会先落 `public_media_results` 与 `public_media_analysis`
- snapshot 内默认维护统一 `asset_registry.json`，记录每个资产的路径、类型、来源、raw/model-safe 属性和大小
- autonomous worker 的 checkpoint / budget / output 也会持久化到 SQLite，而不是只存在进程内
- daemon loop 还会消费 lane budget caps，避免某个 specialist lane 抢占全部恢复/重试额度
- 默认 compact-context：模型只读取压缩后的 `analysis_input`，不会直接吃整页 HTML、整份 raw payload 或 PDF 二进制内容
- 默认跳过大体积/二进制页面直送模型：
  - 当前 exploration analysis 会跳过 `.pdf` 等 binary-like URL
  - raw 资产仍可留作后续分阶段解析，但不会直接塞进 LLM 上下文
- 当前 compact excerpt 上限已显式固定在共享 policy 中，避免不同 connector 自己扩大上下文
- rerun 同样遵循成本控制：
  - `cheap` rerun 会优先复用既有 retrieval 输入，不触发额外 acquisition
  - 并默认使用 deterministic summary，避免为验证性 rerun 重复消耗模型调用

## Thinking Machines Lab 状态

- current roster：已通过 Harvest `company-employees` 获取高质量 current roster
- current detail：已对优先级最高的一批 current members 拉取 full profile detail
- former fallback：现已改为 `pastCompanies` recall first，不默认加 `excludeCurrentCompanies`
- publication supplement：现已执行 official blog / docs / publication surface 抽取，并把 unmatched authors 作为 lead 保留
- publication lead 的默认顺序已调整为 low-cost first：
  - 先走 `Publication Lead Public-Web Verification`
  - 先补 public-web affiliation evidence 与 LinkedIn URL
  - targeted Harvest name search 不再作为 publication lead 的自动下一步；默认需要逐人确认
- `Roster-Anchored Scholar Coauthor Expansion` 已接入第一版：
  - 当前后端先用 arXiv author-seed search 扩 confirmed roster member 的 coauthor 图
  - 未命中的 coauthor 只会进入 prospect artifact，不会直接升 lead
  - 只有在 public web 确认 affiliation 且拿到 LinkedIn URL 后，才允许继续 profile fetch
- manual review 现已成为正式数据资产入口：
  - 可把人工补充的 LinkedIn / homepage / CV / social links 写回 `candidate + evidence + manual_review_assets`
  - Thinking Machines Lab 当前已完成：
    - `Kevin Lu`：confirmed current employee，已写回 LinkedIn
    - `John Schulman`：confirmed current employee，已写回 homepage evidence
    - `Jeremy Bernstein`：confirmed current employee，已写回 homepage + CV evidence
    - `Horace He`：homepage 未直接确认 TML affiliation，保留为 unresolved lead
- 当前 low-cost search provider 状态：
  - 当前 low-cost search 已经有 provider abstraction
  - 默认顺序是 `dataforseo_google_organic -> serper_google -> google_browser -> bing_html -> duckduckgo_html`
  - 现网环境对 DuckDuckGo HTML endpoint 仍存在 TLS EOF 问题，因此它只保留为 best-effort fallback
  - 更稳定的 Google organic 调用与后台 queue 方案见 `docs/DATAFORSEO_PLAYBOOK.md`

## 设计文档

- `docs/INDEX.md`
- `docs/ARCHITECTURE.md`
- `docs/LEAD_DISCOVERY_METHODS.md`
- `docs/DATA_ARCHITECTURE.md`
- `docs/MODULES.md`
- `docs/PRD.md`
- `docs/BACKEND_MVP.md`
- `docs/DATA_ASSET_GOVERNANCE.md`
- `docs/SERVICE_EVOLUTION_STRATEGY.md`

## 下一步

- 保持 `asset build` 和 `retrieval & delivery` 两条 pipeline 分离，避免 retrieval 默认触发新的 acquisition
- 把 snapshot promotion / canonical pointer / cloud registry 进一步产品化，落实 `docs/DATA_ASSET_GOVERNANCE.md`
- 继续把 Web control plane + local runner 的 hybrid 形态落成真实服务入口，见 `docs/SERVICE_EVOLUTION_STRATEGY.md`
- 用新的公司做真实端到端验证，优先验证意图识别、asset reuse、manual review 和结果交付链
- 将当前本地 sparse-vector retrieval 升级为外部 embedding / vector store，并补 candidate-level semantic attribution
- 引入 specialist lanes / handoff runtime / trace spans，把当前 job engine 继续升级成 fully agentic sourcing copilot
- 再在此基础上做 Demo 前端
