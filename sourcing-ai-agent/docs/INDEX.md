# Documentation Index

> Status: Current first-party doc. Use this file to distinguish current guidance from reference-only docs, and cross-check with `README.md` and `PROGRESS.md` when runtime contracts change.


这份索引用来区分“当前有效文档”和“历史参考文档”，避免新的开发者或新的 AI session 被旧 snapshot、旧数字或旧操作顺序误导。

## Start Here

1. [../../README.md](../../README.md)
2. [../../ONBOARDING.md](../../ONBOARDING.md)
3. [../../CONTRIBUTING.md](../../CONTRIBUTING.md)
4. [../README.md](../README.md)
5. [../PROGRESS.md](../PROGRESS.md)

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
- [PG_ONLY_CUTOVER_TRACKER.md](PG_ONLY_CUTOVER_TRACKER.md)
  PG-only cutover 的剩余尾巴与风险点。
- [SEMANTIC_REFACTOR_PROGRESS.md](SEMANTIC_REFACTOR_PROGRESS.md)
  intent/planning/execution 语义收口的阶段进展。
- [ECS_PRELAUNCH_CHECKLIST.md](ECS_PRELAUNCH_CHECKLIST.md)
  试运行或上线前最后一跳的部署/重启/探针清单。

## Current Canonical Docs

- [MODULES.md](MODULES.md)
  当前模块职责与上下游关系。
- [ARCHITECTURE.md](ARCHITECTURE.md)
  当前系统分层、provider 抽象和 runtime 设计。
- [EXECUTION_CONTRACT_GUARDRAILS.md](EXECUTION_CONTRACT_GUARDRAILS.md)
  planner/runtime/provider/results 不可回退的执行契约与测试约束。
- [CHANGE_REVIEW_2026-04-21_2026-04-23.md](CHANGE_REVIEW_2026-04-21_2026-04-23.md)
  4/21-4/23 这轮稳定化改动的复盘：哪些改动应保留，哪些仍需继续回退成更干净的 contract。
- [SESSION_HANDOFF_2026-04-25.md](SESSION_HANDOFF_2026-04-25.md)
  当前稳定版本的验证结果、新会话恢复步骤和不可回退 guardrails。
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
- [HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md](HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md)
  云端 `serve` 默认路径、前端禁区、GitHub 上传边界（降部署成本）。
- [QUERY_GUARDRAILS.md](QUERY_GUARDRAILS.md)
  用户 query 的能力边界、澄清口径和敏感属性禁区。
- [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)
  工程实现约束与 live-test 纪律。
- [DATA_ASSET_GOVERNANCE.md](DATA_ASSET_GOVERNANCE.md)
  snapshot、scope、promotion state、云端版本治理规则。
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
