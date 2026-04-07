# Sourcing AI Agent Dev

这个目录现在已经整理成一个适合放入 private GitHub repo 的 monorepo 工作区，包含三类内容：

- 历史调研/技能资产
- 新开发的 `sourcing-ai-agent` 后端
- 项目进度与复盘文档

## Start Here

如果你是新的开发者，或是从新的 AI session 重新接手，请先看：

1. [ONBOARDING.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/ONBOARDING.md)
2. [GITHUB_SYNC_PREP.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/GITHUB_SYNC_PREP.md)
3. [sourcing-ai-agent/README.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/README.md)
4. [sourcing-ai-agent/PROGRESS.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/PROGRESS.md)
5. [sourcing-ai-agent/docs/CROSS_DEVICE_SYNC.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/CROSS_DEVICE_SYNC.md)
6. [sourcing-ai-agent/docs/HANDOFF_2026-04-06.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/HANDOFF_2026-04-06.md)
7. [sourcing-ai-agent/docs/RECOVERY_TUTORIAL.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/RECOVERY_TUTORIAL.md)
8. [sourcing-ai-agent/docs/NEXT_TODO.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/NEXT_TODO.md)
9. [sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md)

## 目录

```text
Sourcing AI Agent Dev/
├── Anthropic华人专项/
├── anthropic-employee-scan/
├── biz-visit-onepager/
├── investor-chinese-scan/
└── sourcing-ai-agent/
```

## 主要说明

- `Anthropic华人专项/`
  历史 Anthropic 华人专项的数据资产与过程文档。
- `anthropic-employee-scan/`
  历史 employee scan skill/workflow 沉淀。
- `biz-visit-onepager/`
  相关 one-pager/业务访问支持资产。
- `investor-chinese-scan/`
  投资机构华人成员扫描相关资产。
- `sourcing-ai-agent/`
  当前正在持续开发的通用 Sourcing AI Agent 后端工程。

## GitHub 同步约束

这个 monorepo 现在可以推送到 private GitHub repo，但必须遵守以下规则：

- 不提交任何真实密钥或 provider secret
- 不提交 `runtime/` 下的 live data / search payload / profile payload / company assets
- 不提交历史 `api_accounts.json`
- 不提交原始 zip 包

根目录 `.gitignore` 已经按这个原则配置，可直接用于后续继续推送、换设备 clone 后继续开发。

## 同步边界

当前 GitHub repo 主要同步：

- 代码
- 文档
- 示例配置
- 不含 secrets 的历史方法论资产

不会同步：

- `runtime/`
- provider secrets
- 历史 `api_accounts.json`
- zip / tar 打包副产物

另外要注意：

- `runtime/vendor/` 里的本机增强依赖也不会进入 Git
  - 例如 `pdfminer.six`
  - 例如 browser-search 用到的 `playwright` / browser binaries
- 这类依赖如果要跨设备复用，应跟随 asset bundle/object storage 一起恢复，而不是指望 Git 带上

如果后续需要跨设备复用 runtime 或高价值 profile 资产，应单独放到安全的云端存储，而不是 Git。

## 换设备继续开发

如果你后续切换到公司电脑或新的 AI 开发环境，推荐顺序是：

1. 从 GitHub clone 这个 monorepo。
2. 阅读 [ONBOARDING.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/ONBOARDING.md)。
3. 阅读 [sourcing-ai-agent/PROGRESS.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/PROGRESS.md) 和 [sourcing-ai-agent/docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md)。
4. 单独恢复 `runtime/secrets/providers.local.json` 或重新配置环境变量。
5. 如需复用历史 live data / company assets / profile assets，从单独的安全存储恢复，不要指望 Git 自动带上这些资产。

## 当前建议

1. 继续将本目录作为 monorepo 根目录维护。
2. 只提交代码、文档、示例配置和不含密钥的历史方法论资产。
3. 继续把 live data、runtime snapshot、provider cache 保留在本地或后续云端存储。
4. 将高价值 asset bundle 逐步迁移到 object storage，而不是再创建一个“包含 secrets 和 runtime 的 GitHub repo”。
5. 当前 Thinking Machines Lab handoff bundle 已完成真实 R2 `upload -> download -> restore`，后续优先继续补资产，而不是重复搭基础同步层。
