# Onboarding

这个仓库的目标是让新的开发者或新的 AI session 能快速接手 `Sourcing AI Agent` 的后续开发，而不依赖之前的对话上下文。

开始写代码前，先看一遍根目录 [CONTRIBUTING.md](CONTRIBUTING.md)。后续分支、PR、AI 使用、API contract 和配置变更都以它为准。

## 先看哪里

先读当前有效文档，再按需回看历史记录：

1. [README.md](README.md)
2. [CONTRIBUTING.md](CONTRIBUTING.md)
3. [sourcing-ai-agent/README.md](sourcing-ai-agent/README.md)
4. [sourcing-ai-agent/PROGRESS.md](sourcing-ai-agent/PROGRESS.md)
5. [sourcing-ai-agent/docs/INDEX.md](sourcing-ai-agent/docs/INDEX.md)
6. [sourcing-ai-agent/docs/MODULES.md](sourcing-ai-agent/docs/MODULES.md)
7. [sourcing-ai-agent/docs/ARCHITECTURE.md](sourcing-ai-agent/docs/ARCHITECTURE.md)
8. [sourcing-ai-agent/docs/DATA_ASSET_GOVERNANCE.md](sourcing-ai-agent/docs/DATA_ASSET_GOVERNANCE.md)
9. [sourcing-ai-agent/docs/SERVICE_EVOLUTION_STRATEGY.md](sourcing-ai-agent/docs/SERVICE_EVOLUTION_STRATEGY.md)
10. [sourcing-ai-agent/docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md](sourcing-ai-agent/docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md)
11. [sourcing-ai-agent/docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md](sourcing-ai-agent/docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md)

## 历史参考文档

以下文档保留具体时间点的执行细节、迁移背景和旧 backlog，不应作为默认入口：

1. [sourcing-ai-agent/docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md](sourcing-ai-agent/docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md)
2. [sourcing-ai-agent/docs/HANDOFF_2026-04-06.md](sourcing-ai-agent/docs/HANDOFF_2026-04-06.md)
3. [sourcing-ai-agent/docs/RECOVERY_TUTORIAL.md](sourcing-ai-agent/docs/RECOVERY_TUTORIAL.md)
4. [sourcing-ai-agent/docs/NEXT_TODO.md](sourcing-ai-agent/docs/NEXT_TODO.md)
5. [sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md](sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md)
6. [sourcing-ai-agent/docs/DATAFORSEO_PLAYBOOK.md](sourcing-ai-agent/docs/DATAFORSEO_PLAYBOOK.md)

## 当前项目状态

- monorepo 根目录保留历史调研资产与当前通用后端工程
- 当前主开发目录是 `sourcing-ai-agent/`
- Thinking Machines Lab 现在已有明确的 canonical snapshot、`canonical_merged` / `strict_roster_only` 两种 asset view 和云端 bundle
- Thinking Machines Lab handoff bundle 已完成真实 R2 `upload -> download -> restore`
- 低成本 search 现已统一到稳定的 provider abstraction
- 自动化 worker runtime、scheduler、daemon、manual review、criteria evolution 已具备可继续扩展的底座
- object storage durable sync 已有本地/云端 bundle index 与 sync run manifest
- 数据资产治理和服务化路线已分别沉淀到 `docs/DATA_ASSET_GOVERNANCE.md` 与 `docs/SERVICE_EVOLUTION_STRATEGY.md`
- server 侧 bootstrap checklist 已落到 `docs/SERVER_RUNTIME_BOOTSTRAP.md`

## 本仓库不会包含什么

以下内容默认不进入 Git：

- `runtime/` 下的 live data / snapshot / cache / db
- `providers.local.json` 这类真实 secrets
- 历史 `api_accounts.json`
- 原始 zip / tar 打包文件

这意味着新开发者接手时，代码和文档是完整的，但本地运行仍需要重新配置 provider secrets，必要时重新生成 runtime 资产，或从单独的安全存储恢复。

## 从 GitHub clone 之后先确认什么

1. `runtime/` 默认不存在或只保留空目录结构，这不是仓库损坏，而是刻意不入库。
2. `runtime/secrets/providers.local.json` 不会随仓库同步，需要手动恢复或改用环境变量。
3. 历史 live payload、company snapshot、profile payload、manual review raw assets 也不会在 Git 中，需要从单独的安全存储恢复。
4. 如果只是继续开发后端逻辑，缺少 runtime 不会阻止阅读代码、跑大部分单测和继续实现。

## 本地启动建议

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_thinking_machines_lab.json
```

## 推荐接手顺序

1. 先确认 provider 配置
2. 跑测试，确认基础功能未损坏
3. 读 `PROGRESS.md`、`docs/INDEX.md` 和最近的验证说明，确认当前 canonical state
4. 在 `docs/MODULES.md` 和 `docs/ARCHITECTURE.md` 里定位要修改的模块
5. 优先保持：
   - 数据资产落盘
   - 可审计
   - low-cost-first
   - unresolved lead retention

## 推荐的跨设备恢复材料

如果你要在新的机器或新的 AI session 上尽可能完整地恢复工作上下文，优先准备：

1. 这个 GitHub monorepo
2. `runtime/secrets/providers.local.json`
3. 必要的 provider tokens 对照表
4. 需要复用的 `runtime/company_assets/` 子集
5. 最近一次 live test 的 retrospective 和 progress

## 当前最重要的工程约束

- 不要把 secrets 或 runtime 资产提交到 Git
- connector 返回必须先落盘，再进入后续处理
- 大网页、PDF、profile payload 不要直接塞进模型上下文
- 默认先 low-cost relation check，再决定是否走高成本 API
- unresolved lead 不能静默丢弃
