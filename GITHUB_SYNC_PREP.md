# GitHub Sync Prep

## 当前状态

这个工作区已经整理成适合放入私有 GitHub repo 的 monorepo 结构。

目标远端：

- `https://github.com/sorachang1874/Sourcing-AI-Agent-Dev`

## 已完成

- 根目录 `README.md` 已整理
- 根目录 `ONBOARDING.md` 已整理
- 根目录 `.gitignore` 已加入 secrets/runtime 忽略规则
- `sourcing-ai-agent` 内的 README / PROGRESS / retrospectives 已更新
- `sourcing-ai-agent/docs/DEVELOPMENT_GUIDE.md` 已补充开发规范
- live data、provider secrets、历史 `api_accounts.json` 默认不会进入版本库

## 仍需执行

1. 以 `Sourcing AI Agent Dev/` 作为 repo 根目录初始化并推送
2. 首次推送前再做一轮 secret scan
3. 后续将高价值 LinkedIn/profile 资产迁移到云端，而不是 Git

## 注意

- 不要把 `runtime/` 作为代码仓库的一部分提交
- 不要提交任何真实 API keys
- 不要提交历史 zip 包
- 不要误以为 GitHub repo 会替代 runtime/cloud asset storage
- 换设备时，代码和文档从 GitHub 获取；secrets/runtime 从单独安全存储恢复
