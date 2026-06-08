# Documentation Index

> Status: Current first-party doc. Use this file to distinguish current guidance from reference-only docs, and cross-check with `README.md` and `PROGRESS.md` when runtime contracts change.


这份索引用来区分“当前有效文档”和“历史参考文档”，避免新的开发者或新的 AI session 被旧 snapshot、旧数字或旧操作顺序误导。

## Project Framing

新 session 在做仓库总结时，默认应把本项目描述为招聘自动化、公开资料补全、候选人研究和 provider 驱动的产品化工作流。

- 优先使用：公开信息、候选人补全、公司 roster、后台恢复、证据质量、预算控制、人工确认、可审计导出。
- 保持业务上下文：合法 API、公开网页、候选人管理、目标候选人页、运营可观测性。
- 只有在任务本身明确要求时，才引入与应用安全或安全研究相关的专门语境。

## Current Stage Checkpoint

- 当前活跃阶段是 Agent-native service-grade workflow closure：把 Candidate Acquisition、Profile Fetch、CRM Public Web、Provider Task Runtime、Serving Projection、CRM/Export 和 Operation Workbench 收敛成可被 OpenClaw/Codex/LangGraph 安全调用、可观察、可接管的 typed service boundary。
- 2026-06-08 当前判断：不应先把 OpenClaw/Codex 直接接进现有 runtime 内部，也不应一次性迁移到某个框架。先按 `SERVICE_GRADE_ARCHITECTURE_PLAN.md` 完成底层服务级收口；但 M1-M5 的抽象必须已经面向 Agent tool manifest、event-visible progress、Search/fetch evidence 和 approval/budget/control contract，而不是等 M6 adapter 再补。
- 本地资产治理属于 M0.5：在 M0 docs checkpoint 后、M1/M2 前，用 audit / repair proposal / cold archive manifest / reviewed apply 处理 Google、Reflection AI 等重复本地资产；不要通过手动删除或 broad dirty-tree commit 处理。
- 2026-06-07 UI/contract slice 已通过 Independent Review Gate，并完成本地浏览器截图验证；下一步不是继续堆 UI patch，而是小范围 live-provider validation、service manifest/Provider Task Runtime 收口、以及 Phase 13 前的 Agent-callable contract closure。
- 前端和 Agent 默认应从 `SERVICE_GRADE_ARCHITECTURE_PLAN.md`、`FRONTEND_API_CONTRACT.md`、`AGENT_OPERATION_CONTRACT.md`, `PRE_AGENT_CONTRACT_REVIEW.md`、`DURABLE_EXECUTION_RUNTIME_CONTRACT.md`、`MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md` 和 `NEXT_TODO.md` 读取当前合同。旧 session handoff、事故复盘和长 tracker 只作为历史参考；若与这些合同冲突，以当前合同和最新 checkpoint 为准。
- 不建议在一次 UI/contract slice 中批量删除历史 Markdown。删除或归档旧文档应作为单独 cleanup，通过 Independent Review Gate 确认没有丢失审计证据或仍被测试引用的合同字符串。

## Start Here

1. [../../README.md](../../README.md)
2. [RUNTIME_PREFLIGHT.md](RUNTIME_PREFLIGHT.md)
3. [../../ONBOARDING.md](../../ONBOARDING.md)
4. [../../CONTRIBUTING.md](../../CONTRIBUTING.md)
5. [../README.md](../README.md)
6. [../PROGRESS.md](../PROGRESS.md)

## Status Banner Rule

当前所有一方 Markdown 都应在开头携带 `> Status:` 头，用来显式声明：

- 这份文档是不是当前默认入口
- 它更适合当 tracker、reference 还是 historical snapshot
- 读取时是否需要同时参照 `README.md` / `PROGRESS.md` / 本索引

如果某份 Markdown 没有这个头，优先把它视为文档漂移并先修复。

## Living Trackers

- [../PROGRESS.md](../PROGRESS.md)
  当前已完成事项与最近一次验证结果。
- [NEXT_TODO.md](NEXT_TODO.md)
  活跃待办与下一轮应继续收口的 contract。
- [PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md](PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md)
  将旧 `Public Web Stage 2` 改造成目标候选人页批量 Public Web Search 的产品化 TODO/spec。
- [PG_ONLY_CUTOVER_TRACKER.md](PG_ONLY_CUTOVER_TRACKER.md)
  PG-only cutover 的剩余尾巴与风险点。
- [SEMANTIC_REFACTOR_PROGRESS.md](SEMANTIC_REFACTOR_PROGRESS.md)
  intent/planning/execution 语义收口的阶段进展。
- [ECS_PRELAUNCH_CHECKLIST.md](ECS_PRELAUNCH_CHECKLIST.md)
  试运行或上线前最后一跳的部署/重启/探针清单。

## Current Canonical Docs

- [MODULES.md](MODULES.md)
  当前模块职责与上下游关系。
- [RUNTIME_PREFLIGHT.md](RUNTIME_PREFLIGHT.md)
  本地 dev、scripted/test、local live smoke、hosted/ECS workflow 启动前的统一 preflight 入口，包含持久启动、端口检查、webhook readiness 和边界说明。
- [ARCHITECTURE.md](ARCHITECTURE.md)
  当前系统分层、provider 抽象和 runtime 设计。
- [SERVICE_GRADE_ARCHITECTURE_PLAN.md](SERVICE_GRADE_ARCHITECTURE_PLAN.md)
  Phase 13 / OpenClaw-Codex adapter 之前的服务级收口计划：workflow spec/command spec manifest、Provider Task Runtime、Candidate Acquisition、Profile Fetch、CRM Public Web、Serving Projection、CRM/Export、Frontend/Operation Workbench 和 GitHub checkpoint 纪律。
- [RUNTIME_ASSET_RETENTION_GOVERNANCE.md](RUNTIME_ASSET_RETENTION_GOVERNANCE.md)
  M0.5 本地 runtime/output 资产保留治理：区分 company snapshot consolidation 与 `runtime/test_env` / `output` 历史运行目录 retention inventory，禁止把只读盘点当成删除许可。
- [EXECUTION_CONTRACT_GUARDRAILS.md](EXECUTION_CONTRACT_GUARDRAILS.md)
  planner/runtime/provider/results 不可回退的执行契约与测试约束。
- [INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md](INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md)
  用户请求的人群边界、策略制定、coverage proof、authoritative pointer 和展示语义之间的 source priority 合同。
- [EVENT_LEVEL_WORKFLOW_RESPONSE.md](EVENT_LEVEL_WORKFLOW_RESPONSE.md)
  Provider-backed workflow 的事件级响应抽象：remote completion discovery、local event apply、next submit 和 downstream materialization 的解耦 contract。
- [DURABLE_EXECUTION_RUNTIME_CONTRACT.md](DURABLE_EXECUTION_RUNTIME_CONTRACT.md)
  Durable execution runtime 顶层合同：OperationRun / WorkflowRun / WorkflowCommand / ActivityAttempt 分层、append-only event log、reducer、typed command owner registry、timer/retry、completion policy、Agent 边界、`job_materialization_items` 迁移退役与大 snapshot 资产整理 gate。
- [DISCOVERY_PROVIDER_QUEUE_CONTRACT.md](DISCOVERY_PROVIDER_QUEUE_CONTRACT.md)
  Search-seed discovery query、provider retry、worker envelope、snapshot merge 与 profile prefetch 的 queue-first owner contract，明确 `provider_search_retry` 不能作为独立 drain fallback。
- [WORKFLOW_PROGRESS_CONTRACT.md](WORKFLOW_PROGRESS_CONTRACT.md)
  执行过程页与候选人看板 streaming 的字段来源、计数不变量、前端展示规则和 scripted/browser 观测 guardrails。
- [CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md](CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md)
  面向 Claude Code 的服务级 streaming workflow 重构上下文：失败复盘、产品预期、目标架构、反模式和验收标准。
- [CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md](CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md)
  面向 Claude Code 的当前 PG manual scripted 事故交接：候选人看板 streaming、local apply、board runtime、分页、分层和 scripted/live parity 的修复入口。
- [STREAMING_WORKFLOW_REBUILD_PLAN.md](STREAMING_WORKFLOW_REBUILD_PLAN.md)
  result lifecycle、atomic progress、provider handoff、partial delta board streaming 和 scripted/browser 测试体系的分阶段落地计划。
- [JOB_RESULT_LIFECYCLE_DESIGN.md](JOB_RESULT_LIFECYCLE_DESIGN.md)
  服务级 streaming workflow 重构第一片实现的设计说明：canonical 持久化 `job_result_lifecycle` 表的 schema、写入者归属、读取改造与失败类消除矩阵。
- [CANONICAL_SERVING_PROJECTION_CONTRACT.md](CANONICAL_SERVING_PROJECTION_CONTRACT.md)
  下一轮 post-ECS 架构改造的顶层结果服务合同：`projection_id` 作为结果资源、run/result 分离、collection authoritative projection、public reader fail-closed、CRM 来源解耦、无双轨迁移和未来 Agent 交互边界。
- [CRM_STATE_CONTRACT.md](CRM_STATE_CONTRACT.md)
  CRM 独立模块合同：person-first CRM record、engagement/pipeline state、append-only event audit、target-candidate 迁移、Public Web promotion 到 PersonAssertion 的边界，以及未来 Agent 调用 CRM 的 writer 规则。
- [PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md](PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md)
  Person 级资产/证据/断言合同：LinkedIn identity、raw profile、avatar media、Public Web/DataForSEO evidence、用户确认 contact/social assertion、raw/evidence index 与 projection/CRM 的边界。
- [AGENT_OPERATION_CONTRACT.md](AGENT_OPERATION_CONTRACT.md)
  多轮 Agent 操作合同：AgentConversation、AgentAction、OperationRun、action registry、approval/budget/idempotency、staged acquisition，以及 Temporal/LangGraph 这类执行框架的可替换边界。
- [PRE_AGENT_CONTRACT_REVIEW.md](PRE_AGENT_CONTRACT_REVIEW.md)
  Phase 13 前的 W10 合同审查矩阵：模块 owner/source of truth、Agent-callable surface、fast preflight、migration-only bridge 和未决方向。
- [CLAUDE_CODE_EVENT_WORKFLOW_REVIEW_PROMPT.md](CLAUDE_CODE_EVENT_WORKFLOW_REVIEW_PROMPT.md)
  启动 Claude Code 新 session 审查事件级工作流设计时使用的 handoff prompt、阅读顺序、review 问题和输出格式。
- [APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md](APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md)
  Apify ad-hoc webhook 的 ECS / 本地 tunnel 配置、token 口径、connectivity probe、one-profile round-trip smoke 与排障方法。
- [APIFY_BILLING_INCIDENT_POSTMORTEM_2026-05-07.md](APIFY_BILLING_INCIDENT_POSTMORTEM_2026-05-07.md)
  2026-05-07 scripted smoke 跨 runtime namespace 触发真实 Apify 计费的复盘、证据链、隔离 contract 和 prevention gates。
- [CHANGE_REVIEW_2026-04-21_2026-04-23.md](CHANGE_REVIEW_2026-04-21_2026-04-23.md)
  4/21-4/23 这轮稳定化改动的复盘：哪些改动应保留，哪些仍需继续回退成更干净的 contract。
- [SESSION_HANDOFF_2026-04-25.md](SESSION_HANDOFF_2026-04-25.md)
  当前稳定版本的验证结果、新会话恢复步骤和不可回退 guardrails。
- [SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md](SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md)
  目标候选人级 Public Web Search 的方法探索、后端/前端产品化进度、跨 session review、剩余工作和验证命令。
- [INTENT_PLANNING_BRIEF.md](INTENT_PLANNING_BRIEF.md)
  `plan` 阶段第一段产品原生解释卡片的标准结构。
- [FRONTEND_API_CONTRACT.md](FRONTEND_API_CONTRACT.md)
  Web 层如何消费 `plan / review / workflow / progress / results`，以及 `intent_rewrite` 的稳定 contract。
- [../contracts/frontend_api_contract.ts](../contracts/frontend_api_contract.ts) / [../contracts/frontend_api_contract.schema.json](../contracts/frontend_api_contract.schema.json) / [../contracts/frontend_api_adapter.ts](../contracts/frontend_api_adapter.ts) / [../contracts/frontend_react_hooks.example.tsx](../contracts/frontend_react_hooks.example.tsx)
  与上面 contract 对齐的 TypeScript interface / JSON Schema / fetch adapter / React hooks 示例资产。
- [TERMINAL_WORKFLOW.md](TERMINAL_WORKFLOW.md)
  终端里如何走 `plan -> review -> workflow -> progress/results`。
- [WORKFLOW_OPERATIONS_PLAYBOOK.md](WORKFLOW_OPERATIONS_PLAYBOOK.md)
  当前版本推荐的 CLI / API 调用方式、进度追踪与恢复交互手册。
- [WINDOWS_WSL2_FRONTEND_SETUP.md](WINDOWS_WSL2_FRONTEND_SETUP.md)
  Windows 宿主机通过 WSL2 访问本地前端与 hosted backend 的专用启动/排障手册。
- [TESTING_PLAYBOOK.md](TESTING_PLAYBOOK.md)
  当前测试分层、simulate/scripted/live 用法与 regression 扩展规则。
- [TEST_ENVIRONMENT.md](TEST_ENVIRONMENT.md)
  隔离测试环境的 runtime / port / simulate-scripted 启动与运维入口。
- [RUNTIME_ENVIRONMENT_ISOLATION.md](RUNTIME_ENVIRONMENT_ISOLATION.md)
  production / local_dev / test / simulate / scripted / replay 的 runtime namespace、provider cache 与 PG 隔离契约。
- [LOCAL_POSTGRES_CONTROL_PLANE.md](LOCAL_POSTGRES_CONTROL_PLANE.md)
  本地 `.local-postgres` 的自动发现、自动启动、DSN 优先级、`show-control-plane-runtime` 检查命令，以及 Postgres-first control-plane 约定。
- [MAC_DEV_ENV_MIGRATION.md](MAC_DEV_ENV_MIGRATION.md)
  从 Linux/WSL 虚拟机迁移到另一台 Mac 时，代码、runtime 资产、Postgres control plane 和 Codex 记录的推荐迁移方式。
- [ECS_ACCESS_PLAYBOOK.md](ECS_ACCESS_PLAYBOOK.md)
  连接阿里云 ECS 的 SSH、端口转发、文件同步和最小健康检查入口。
- [ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md](ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md)
  将当前代码和精选 canonical 数据资产迁移到 ECS 的可复用流程，包含 `latest_snapshot.json` 降级、资产 manifest、选择性 rsync/bundle、归档和上线探针。
- [HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md](HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md)
  云端 `serve` 默认路径、前端禁区、GitHub 上传边界（降部署成本）。
- [QUERY_GUARDRAILS.md](QUERY_GUARDRAILS.md)
  用户 query 的能力边界、澄清口径和敏感属性禁区。
- [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)
  工程实现约束与 live-test 纪律。
- [DATA_ASSET_GOVERNANCE.md](DATA_ASSET_GOVERNANCE.md)
  snapshot、scope、promotion state、云端版本治理规则。
- [AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md](AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md)
  `authoritative` serving pointer、full-company coverage proof、exact scoped shard coverage 和 planner local-reuse 策略的当前生产合同。
- [SERVICE_EVOLUTION_STRATEGY.md](SERVICE_EVOLUTION_STRATEGY.md)
  当前推荐的 hybrid 服务形态与后续产品化路径。
- [SERVER_RUNTIME_BOOTSTRAP.md](SERVER_RUNTIME_BOOTSTRAP.md)
  长期在线 server / runner 的最小启动流程。
- [ALIYUN_ECS_TRIAL_ROLLOUT.md](ALIYUN_ECS_TRIAL_ROLLOUT.md)
  当前阿里云 ECS 试用机 + Cloudflare Pages 的选择性迁移与上线方案。
- [CLOUDFLARE_SAME_ORIGIN_PROXY.md](CLOUDFLARE_SAME_ORIGIN_PROXY.md)
  Cloudflare Pages/Workers 与 hosted backend 的 same-origin 代理方案。
- [CANONICAL_CLOUD_BUNDLE_CATALOG.md](CANONICAL_CLOUD_BUNDLE_CATALOG.md)
  当前服务器恢复应使用的 canonical control-plane/company snapshot bundle 清单。
- [CROSS_DEVICE_SYNC.md](CROSS_DEVICE_SYNC.md)
  Git、secret、object storage、local runtime 的边界与恢复方法。

## Provider And Workflow Playbooks

- [HARVESTAPI_PLAYBOOK.md](HARVESTAPI_PLAYBOOK.md)
  Harvest actor 的实际参数、成本口径和已知坑。
- [DATAFORSEO_PLAYBOOK.md](DATAFORSEO_PLAYBOOK.md)
  Google organic SERP 的低成本后台 lane。
- [LEAD_DISCOVERY_METHODS.md](LEAD_DISCOVERY_METHODS.md)
  Publication lead verification 与 `Roster-Anchored Scholar Coauthor Expansion` 的正式方法定义。

## Current Validated Asset Notes

- [THINKING_MACHINES_LAB_CANONICAL_ASSET.md](THINKING_MACHINES_LAB_CANONICAL_ASSET.md)
  当前最该复用的 TML canonical snapshot、asset view 和云端 bundle。
- [THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md](THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md)
  当前 retrieval stack 与 strict-view 精度验证结论。

## Product And Data Model Reference

- [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md)
- [PRD.md](PRD.md)
- [BACKEND_MVP.md](BACKEND_MVP.md)

## Historical / Reference-Only Docs

这些文档保留具体时间点的上下文、故障和迁移路径，但不应该作为当前默认入口：

- [THINKING_MACHINES_LAB_RETROSPECTIVE.md](THINKING_MACHINES_LAB_RETROSPECTIVE.md)
- [HANDOFF_2026-04-06.md](HANDOFF_2026-04-06.md)
- [HANDOFF_2026-04-09.md](HANDOFF_2026-04-09.md)
- [GITHUB_DEV_DIFF_REVIEW_2026-04-10.md](GITHUB_DEV_DIFF_REVIEW_2026-04-10.md)
- [RECOVERY_TUTORIAL.md](RECOVERY_TUTORIAL.md)

读取这些 reference 文档时，应同时参照上面的 canonical docs，避免把 dated 数字、旧 provider 策略或旧 snapshot 当成当前事实。
