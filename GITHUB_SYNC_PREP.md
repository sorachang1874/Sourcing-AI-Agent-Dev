# GitHub Sync Prep

## 当前状态

这个工作区已经整理成适合放入 GitHub repo 的 monorepo 结构。

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

## 当前建议的提交分组

如果本次准备把当前版本同步到 GitHub，建议至少按下面口径检查 staging，而不是直接 `git add .`。

### 1. 应一起提交的当前核心内容

- repo hygiene 与入口文档：
  - `.gitignore`
  - `README.md`
  - `ONBOARDING.md`
  - `GITHUB_SYNC_PREP.md`
- 当前有效 docs 与验证说明：
  - `sourcing-ai-agent/README.md`
  - `sourcing-ai-agent/PROGRESS.md`
  - `sourcing-ai-agent/docs/INDEX.md`
  - `sourcing-ai-agent/docs/DATA_ASSET_GOVERNANCE.md`
  - `sourcing-ai-agent/docs/SERVICE_EVOLUTION_STRATEGY.md`
  - `sourcing-ai-agent/docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md`
  - `sourcing-ai-agent/docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md`
  - 以及相应更新过的 `ARCHITECTURE / MODULES / DEVELOPMENT_GUIDE / CROSS_DEVICE_SYNC / HARVESTAPI_PLAYBOOK / DATAFORSEO_PLAYBOOK`
- 与这些文档对应的当前后端实现和测试：
  - `src/sourcing_agent/*.py`
  - `tests/*.py`
  - `scripts/dataforseo_google_organic.py`

### 2. 可以提交，但应明确知道它们的定位

- `sourcing-ai-agent/configs/manual_review_thinkingmachineslab_*.json`
  - 这些是 project-specific 的 public review examples，不是通用模板。
  - 保留它们的价值在于：
    - 记录 corner case
    - 让后续 AI / 开发者理解 manual non-member / member resolution 的实际输入形态
  - 提交前应保证：
    - 不含 secret
    - 不含本机绝对路径
- `sourcing-ai-agent/configs/systemd/worker-recovery-daemon.service`
  - 这是示例 unit，不应保留真实用户和真实路径。

### 3. 默认不要直接提交的本地执行痕迹

下面这些更像本地 smoke / runtime-specific 输入，不适合作为默认 repo 内容，已加入 `.gitignore`：

- `sourcing-ai-agent/configs/live_queue_smoke_tml_former.json`
- `sourcing-ai-agent/configs/plan_review_tml_full_roster_approve_20260407.json`

如果后续确实想保留它们，应先转换成无本地状态依赖的 `.example.json` 版本，再入库。

## 当前已修掉的 push 风险

- canonical markdown 文档中的本机绝对路径链接已清理
- 新增 `sourcing-ai-agent/docs/INDEX.md` 作为当前文档入口
- `sourcing-ai-agent/configs/providers.local.example.json` 已去敏，不再包含真实风格的 secret / token
- 已跟踪的 `manual_review_thinkingmachineslab_*.json` 中，本机绝对 `source_path` 已改为 runtime 相对路径
- `.codex`、`TemporaryData/` 和上面两个本地 smoke config 已加入 `.gitignore`

## 推荐的 push 前检查

1. 先运行完整单测
2. 再跑一轮链接/路径审计
3. 检查 `git status --short`，确认没有把本地临时文件和 runtime 输入混进 staging
4. 最后再执行 secret scan

## 注意

- 不要把 `runtime/` 作为代码仓库的一部分提交
- 不要提交任何真实 API keys
- 不要提交历史 zip 包
- 不要误以为 GitHub repo 会替代 runtime/cloud asset storage
- 换设备时，代码和文档从 GitHub 获取；secrets/runtime 从单独安全存储恢复
