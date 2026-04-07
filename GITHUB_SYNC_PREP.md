# GitHub Sync Prep

## 当前状态

这个工作区已经整理成适合放入私有 GitHub repo 的 monorepo 结构。

目标远端：

- `https://github.com/sorachang1874/Sourcing-AI-Agent-Dev`

## 已完成

- 根目录 `README.md` 已整理
- 根目录 `ONBOARDING.md` 已整理
- 根目录 `CONTRIBUTING.md` 已新增，作为协作开发规则入口
- `.github/pull_request_template.md` 已新增，作为 PR checklist
- 根目录 `.gitignore` 已加入 secrets/runtime 忽略规则
- `sourcing-ai-agent` 内的 README / PROGRESS / retrospectives 已更新
- `sourcing-ai-agent/docs/DEVELOPMENT_GUIDE.md` 已补充开发规范
- live data、provider secrets、历史 `api_accounts.json` 默认不会进入版本库

## 仍需执行

1. 以 `Sourcing AI Agent Dev/` 作为 repo 根目录初始化并推送
2. 首次推送前再做一轮 secret scan
3. 在 GitHub 上保护 `main` 和 `dev`
4. 在 GitHub 上启用必经 PR、至少 1 个 approval、status checks、禁止 force push
5. 后续将高价值 LinkedIn/profile 资产迁移到云端，而不是 Git

## 当前 GitHub 限制

仓库改为 public 后，`main` 和 `dev` 的 branch protection 已可正常启用。

当前已启用：

- Require pull request before merging
- Require at least 1 approval
- Require conversation resolution
- Require status check `unit-tests`
- Enforce for admins
- Disallow force pushes
- Disallow branch deletion

后续如果新增前端 CI、lint 或其他 required checks，需要同步更新 GitHub protection 设置。

## 注意

- 不要把 `runtime/` 作为代码仓库的一部分提交
- 不要提交任何真实 API keys
- 不要提交历史 zip 包
- 不要误以为 GitHub repo 会替代 runtime/cloud asset storage
- 换设备时，代码和文档从 GitHub 获取；secrets/runtime 从单独安全存储恢复
