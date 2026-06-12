# Documentation Index

> Status: Current first-party doc. Use this file to distinguish current guidance from reference-only docs, and cross-check with `README.md` and `PROGRESS.md` when runtime contracts change. Restructured 2026-06-11 into tiers; archived docs live under `docs/archive/`.

这份索引用来区分"当前有效文档"和"历史参考文档"，避免新的开发者或新的 AI session 被旧 snapshot、旧数字或旧操作顺序误导。

## Project Framing

新 session 在做仓库总结时，默认应把本项目描述为招聘自动化、公开资料补全、候选人研究和 provider 驱动的产品化工作流。

- 优先使用：公开信息、候选人补全、公司 roster、后台恢复、证据质量、预算控制、人工确认、可审计导出。
- 保持业务上下文：合法 API、公开网页、候选人管理、目标候选人页、运营可观测性。
- 只有在任务本身明确要求时，才引入与应用安全或安全研究相关的专门语境。

## Current Stage Checkpoint (2026-06-11)

- 2026-06-11 起，重构按五条轨道推进（详见 `SERVICE_GRADE_ARCHITECTURE_PLAN.md` 的 2026-06-11 revision 与 `NEXT_TODO.md`）：
  - **A 分解**：orchestrator god class（87k 行）按 Phase 0（测试前置修缮）→ CommandKernel → CommandSpec registry（= 重定义后的 M1）→ 逐域提取 → 纠缠核心重设计。
  - **B 存储/测试**：测试环境契约 v2（PG schema-per-run + teardown + TTL），51 个 SQLite 测试文件迁 PG，按表组 PG-pure 重写后删除 SQLite 影子；Mac 本地 PG 用 Docker。
  - **C Serving**：目标 ~20 并发用户。psycopg 连接池 → 重活出请求线程 → worker 进程分离 → 最小鉴权/租户 → FastAPI + SSE（已批准）→ 对象存储读穿。Redis 此阶段明确不引入。
  - **D Agent**：ModelClient 升级（streaming + tool-calling）→ Agent Session 契约（消费 M1 manifest 作为 tool spec）→ 第一垂直切片：公司身份自验证 loop（替代手动修正 LinkedIn URL）。
  - **E 治理**：文档治理（本次归档/轮转/分层）、`runtime/test_env` TTL 清理（取代 M0.9 独立审批流程；冷备已 sha256 验证）。
- M1 重定义：不是"从 monolith 提取 manifest"，而是"建 CommandSpec registry（落在 durable_runtime），manifest 是它的导出物"。
- M2 Provider Task Runtime 必须承接 provider 级并发预算（HarvestAPI profile-fetch 有 ~8 并发 actor 的隐性限制，是旧 8 槽 API 信号量的真实由来）、API key 池化与 batch 粒度治理。
- Independent Review Gate 适用范围收窄：保留给真正不可重建资产的破坏性操作与 contract-heavy 变更；本地可重建测试产物的清理改走 TTL 工具路径（dry-run + 保护名单 + 活动进程检查仍强制）。
- 前端和 Agent 默认应从 `SERVICE_GRADE_ARCHITECTURE_PLAN.md`、`FRONTEND_API_CONTRACT.md`、`AGENT_OPERATION_CONTRACT.md`、`PRE_AGENT_CONTRACT_REVIEW.md`、`DURABLE_EXECUTION_RUNTIME_CONTRACT.md`、`MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md` 和 `NEXT_TODO.md` 读取当前合同；与归档文档冲突时，以当前合同为准。

## Doc Governance Rules

- 所有一方 Markdown 开头必须有 `> Status:` banner，声明它是 contract / playbook / tracker / reference / archived。没有 banner 视为文档漂移，先修复。
- `PROGRESS.md` 与 `NEXT_TODO.md` 是滚动文件：只保留活动窗口（PROGRESS 保留当月+上月，目标 ≤300 行），更早内容轮转到 `docs/archive/progress/`。
- 完成使命的 session handoff / tracker / incident 复盘 / 一次性 prompt 移入 `docs/archive/`，banner 改为 Archived；移动时修正全库引用路径。
- 标记为 "Current" 的文档若内容已落后于代码（如 ARCHITECTURE/MODULES），在本索引中标注"待刷新"，刷新前以代码与 contract 类文档为准。

## Start Here

1. [../AGENTS.md](../AGENTS.md)（工作区级）与 [AGENTS.md](../AGENTS.md)
2. [../README.md](../README.md)
3. [../PROGRESS.md](../PROGRESS.md)
4. [NEXT_TODO.md](NEXT_TODO.md)
5. [SERVICE_GRADE_ARCHITECTURE_PLAN.md](SERVICE_GRADE_ARCHITECTURE_PLAN.md)
6. [CLAUDE_CODE_PROJECT_HANDBOOK_2026-06-10.md](CLAUDE_CODE_PROJECT_HANDBOOK_2026-06-10.md)

## Tier 1 — Active Contracts（改对应代码前必读）

- [DURABLE_EXECUTION_RUNTIME_CONTRACT.md](DURABLE_EXECUTION_RUNTIME_CONTRACT.md) — durable runtime 顶层合同：OperationRun/WorkflowCommand/ActivityAttempt、event log、command owner registry、completion policy。
- [FRONTEND_API_CONTRACT.md](FRONTEND_API_CONTRACT.md) — Web 层消费 plan/review/workflow/progress/results 的稳定 contract；配套 [../contracts/](../contracts/) 下的 TS/JSON Schema/adapter。
- [CANONICAL_SERVING_PROJECTION_CONTRACT.md](CANONICAL_SERVING_PROJECTION_CONTRACT.md) — projection_id 作为结果资源、run/result 分离、public reader fail-closed。
- [CRM_STATE_CONTRACT.md](CRM_STATE_CONTRACT.md) — person-first CRM record、engagement state、append-only audit、Agent 调用 CRM 的 writer 规则。
- [PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md](PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md) — person 级资产/证据/断言边界。
- [AGENT_OPERATION_CONTRACT.md](AGENT_OPERATION_CONTRACT.md) — Agent 操作合同：AgentAction、approval/budget/idempotency、外部 Agent 边界。
- [MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md](MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md) — `model_native_search` 保留 provider，fail-closed。
- [INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md](INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md) — 人群边界/策略/coverage proof 的 source priority。
- [AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md](AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md) — authoritative pointer 与 coverage proof 生产合同。
- [DISCOVERY_PROVIDER_QUEUE_CONTRACT.md](DISCOVERY_PROVIDER_QUEUE_CONTRACT.md) — discovery queue-first owner contract。
- [WORKFLOW_PROGRESS_CONTRACT.md](WORKFLOW_PROGRESS_CONTRACT.md) — 进度/看板 streaming 字段来源与计数不变量。
- [EVENT_LEVEL_WORKFLOW_RESPONSE.md](EVENT_LEVEL_WORKFLOW_RESPONSE.md) — 事件级响应抽象。
- [EXECUTION_CONTRACT_GUARDRAILS.md](EXECUTION_CONTRACT_GUARDRAILS.md) — planner/runtime/provider/results 不可回退契约。
- [RUNTIME_ENVIRONMENT_ISOLATION.md](RUNTIME_ENVIRONMENT_ISOLATION.md) — runtime namespace、provider cache 与 PG 隔离契约。
- [WORKFLOW_BEHAVIOR_GUARDRAILS.md](WORKFLOW_BEHAVIOR_GUARDRAILS.md) — workflow 预期行为定义。
- [JOB_RESULT_LIFECYCLE_DESIGN.md](JOB_RESULT_LIFECYCLE_DESIGN.md) — 已实现的 job_result_lifecycle 设计说明。

## Tier 2 — Active Playbooks / Ops

- [RUNTIME_PREFLIGHT.md](RUNTIME_PREFLIGHT.md) — 启动任何本地/scripted/hosted 工作流前的统一 preflight 入口。
- [TESTING_PLAYBOOK.md](TESTING_PLAYBOOK.md) — 测试分层、simulate/scripted/live 用法与 regression 规则。
- [TEST_ENVIRONMENT.md](TEST_ENVIRONMENT.md) — 隔离测试环境启动运维（测试环境契约 v2 落地后需同步更新）。
- [WORKFLOW_OPERATIONS_PLAYBOOK.md](WORKFLOW_OPERATIONS_PLAYBOOK.md) / [TERMINAL_WORKFLOW.md](TERMINAL_WORKFLOW.md) — CLI/API 操作手册。
- [LOCAL_POSTGRES_CONTROL_PLANE.md](LOCAL_POSTGRES_CONTROL_PLANE.md) — 本地 PG 自动发现/启动、DSN 优先级（注意：当前实现 Linux 专用；Mac 走 Docker 的方案在轨道 B 落地）。
- [RUNTIME_ASSET_RETENTION_GOVERNANCE.md](RUNTIME_ASSET_RETENTION_GOVERNANCE.md) — 资产保留治理（2026-06-11 起范围收窄，见文内 revision 注记）。
- [DATA_ASSET_GOVERNANCE.md](DATA_ASSET_GOVERNANCE.md) — snapshot/scope/promotion 治理。
- [INDEPENDENT_REVIEW_GATE.md](INDEPENDENT_REVIEW_GATE.md) / [INDEPENDENT_REVIEW_BRIEF.md](INDEPENDENT_REVIEW_BRIEF.md) — 独立审查门（适用范围 2026-06-11 收窄）。
- [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md) — 工程实现约束与 live-test 纪律。
- [QUERY_GUARDRAILS.md](QUERY_GUARDRAILS.md) — query 能力边界与敏感属性禁区。
- ECS/部署：[ECS_PRELAUNCH_CHECKLIST.md](ECS_PRELAUNCH_CHECKLIST.md) · [ECS_ACCESS_PLAYBOOK.md](ECS_ACCESS_PLAYBOOK.md) · [ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md](ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md) · [SERVER_RUNTIME_BOOTSTRAP.md](SERVER_RUNTIME_BOOTSTRAP.md) · [ALIYUN_ECS_TRIAL_ROLLOUT.md](ALIYUN_ECS_TRIAL_ROLLOUT.md) · [HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md](HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md) · [CLOUDFLARE_SAME_ORIGIN_PROXY.md](CLOUDFLARE_SAME_ORIGIN_PROXY.md)
- Provider：[HARVESTAPI_PLAYBOOK.md](HARVESTAPI_PLAYBOOK.md) · [DATAFORSEO_PLAYBOOK.md](DATAFORSEO_PLAYBOOK.md) · [APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md](APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md)
- 其他：[CROSS_DEVICE_SYNC.md](CROSS_DEVICE_SYNC.md) · [CANONICAL_CLOUD_BUNDLE_CATALOG.md](CANONICAL_CLOUD_BUNDLE_CATALOG.md) · [WINDOWS_WSL2_FRONTEND_SETUP.md](WINDOWS_WSL2_FRONTEND_SETUP.md)

## Tier 3 — Plans & Trackers

- [SERVICE_GRADE_ARCHITECTURE_PLAN.md](SERVICE_GRADE_ARCHITECTURE_PLAN.md) — 服务级重构计划（含 2026-06-11 五轨道 revision）。
- [PHASE4_ENTANGLED_CORE_DESIGN.md](PHASE4_ENTANGLED_CORE_DESIGN.md) — Phase 4 纠缠核心重设计提案（2026-06-12，待 owner 审定 §4 决策点）。
- [NEXT_TODO.md](NEXT_TODO.md) — 活跃待办（滚动文件）。
- [../PROGRESS.md](../PROGRESS.md) — 进展日志（滚动文件，月度归档于 `archive/progress/`）。
- [PG_ONLY_CUTOVER_TRACKER.md](PG_ONLY_CUTOVER_TRACKER.md) — PG-only cutover 尾巴。
- [PRE_AGENT_CONTRACT_REVIEW.md](PRE_AGENT_CONTRACT_REVIEW.md) — W10 合同审查矩阵。
- [CLAUDE_CODE_PROJECT_HANDBOOK_2026-06-10.md](CLAUDE_CODE_PROJECT_HANDBOOK_2026-06-10.md) — Claude Code 项目手册（部分条目被 2026-06-11 决策修订，见文内注记）。

## Tier 4 — Design / Background Reference

- [ARCHITECTURE.md](ARCHITECTURE.md)、[MODULES.md](MODULES.md) — 2026-04 内容，**待刷新**；与代码冲突时以代码与 Tier 1 contract 为准。
- [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md) · [PRD.md](PRD.md) · [BACKEND_MVP.md](BACKEND_MVP.md) · [SERVICE_EVOLUTION_STRATEGY.md](SERVICE_EVOLUTION_STRATEGY.md) · [INTENT_PLANNING_BRIEF.md](INTENT_PLANNING_BRIEF.md) · [OUTREACH_LAYERING.md](OUTREACH_LAYERING.md) · [INCREMENTAL_MATERIALIZATION_PLAN.md](INCREMENTAL_MATERIALIZATION_PLAN.md) · [LEAD_DISCOVERY_METHODS.md](LEAD_DISCOVERY_METHODS.md)
- [THINKING_MACHINES_LAB_CANONICAL_ASSET.md](THINKING_MACHINES_LAB_CANONICAL_ASSET.md) — TML canonical snapshot 备忘。

## Archive

`docs/archive/` 存放已完成使命的 session handoff、tracker、incident 复盘、一次性 prompt 和旧 plan（2026-06-11 移入 21 份），以及：

- [archive/progress/](archive/progress/) — PROGRESS 月度归档（2026-05、2026-04 及更早）。
- [archive/NEXT_TODO_2026-06-10_full.md](archive/NEXT_TODO_2026-06-10_full.md) — 重写前的完整 TODO 快照（含 M0.9 SHA 与 W 系列历史）。

读取归档文档时，应同时参照 Tier 1 contract，避免把 dated 数字、旧 provider 策略或旧操作顺序当成当前事实。
