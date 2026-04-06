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

根目录 `.gitignore` 已经按这个原则配置，用于后续创建私有 GitHub repo 时直接复用。

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

如果后续需要跨设备复用 runtime 或高价值 profile 资产，应单独放到安全的云端存储，而不是 Git。

## 换设备继续开发

如果你后续切换到公司电脑或新的 AI 开发环境，推荐顺序是：

1. 从 GitHub clone 这个 monorepo。
2. 阅读 [ONBOARDING.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/ONBOARDING.md)。
3. 阅读 [sourcing-ai-agent/PROGRESS.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/PROGRESS.md) 和 [sourcing-ai-agent/docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md)。
4. 单独恢复 `runtime/secrets/providers.local.json` 或重新配置环境变量。
5. 如需复用历史 live data / company assets / profile assets，从单独的安全存储恢复，不要指望 Git 自动带上这些资产。

## 当前建议

1. 先创建一个新的私有 GitHub repo。
2. 将本目录作为 monorepo 根目录推送。
3. 只提交代码、文档、示例配置和不含密钥的历史方法论资产。
4. 继续把 live data、runtime snapshot、provider cache 保留在本地或后续云端存储。
