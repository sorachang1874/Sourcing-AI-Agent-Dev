# Documentation Index

这份索引用来区分“当前有效文档”和“历史参考文档”，避免新的开发者或新的 AI session 被旧 snapshot、旧数字或旧操作顺序误导。

## Start Here

1. [../../README.md](../../README.md)
2. [../../ONBOARDING.md](../../ONBOARDING.md)
3. [../../CONTRIBUTING.md](../../CONTRIBUTING.md)
4. [../README.md](../README.md)
5. [../PROGRESS.md](../PROGRESS.md)

## Current Canonical Docs

- [MODULES.md](MODULES.md)
  当前模块职责与上下游关系。
- [ARCHITECTURE.md](ARCHITECTURE.md)
  当前系统分层、provider 抽象和 runtime 设计。
- [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)
  工程实现约束与 live-test 纪律。
- [DATA_ASSET_GOVERNANCE.md](DATA_ASSET_GOVERNANCE.md)
  snapshot、scope、promotion state、云端版本治理规则。
- [SERVICE_EVOLUTION_STRATEGY.md](SERVICE_EVOLUTION_STRATEGY.md)
  当前推荐的 hybrid 服务形态与后续产品化路径。
- [SERVER_RUNTIME_BOOTSTRAP.md](SERVER_RUNTIME_BOOTSTRAP.md)
  长期在线 server / runner 的最小启动流程。
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
- [RECOVERY_TUTORIAL.md](RECOVERY_TUTORIAL.md)
- [NEXT_TODO.md](NEXT_TODO.md)

读取这些 reference 文档时，应同时参照上面的 canonical docs，避免把 dated 数字、旧 provider 策略或旧 snapshot 当成当前事实。
