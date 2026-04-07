# Contributing

本文件定义 `Sourcing AI Agent Dev` 的协作开发规则。目标很简单：

- `main` 永远稳定、可发布
- 两个人可以并行开发，不互相覆盖
- 前后端通过明确的 API contract 协作
- AI 只能提速，不能绕过人工判断

这套规则默认适用于整个 monorepo。当前活跃工程主要是 `sourcing-ai-agent/`，历史资产目录默认按只读资料处理。

## 1. Overview

协作哲学：

- 小步提交，小 PR，频繁同步
- 所有代码进 `main` 前必须经过 PR 和人工 review
- `dev` 是集成分支，`main` 是稳定分支
- API、schema、配置变更必须显式记录
- AI 生成代码和人工写的代码，审核标准完全一样

## 2. Branching Strategy

受保护分支：

- `main`
  - 始终稳定、可部署、可回滚
  - 只接受来自 `dev` 的 release PR，或来自 `hotfix/*` 的紧急修复 PR
- `dev`
  - 日常集成分支
  - 所有功能开发先合到 `dev`

工作分支：

- 从 `dev` 拉出 feature branch
- 命名规范：
  - `feat/<scope>-<short-desc>`
  - `fix/<scope>-<short-desc>`
  - `docs/<scope>-<short-desc>`
  - `chore/<scope>-<short-desc>`
  - `refactor/<scope>-<short-desc>`
  - `hotfix/<scope>-<short-desc>` 仅用于线上/主干紧急修复，从 `main` 拉出

硬规则：

- 不允许直接 commit 到 `main`
- 不允许直接 commit 到 `dev`
- 功能分支默认生命周期不超过 3 个工作日
- 长时间未合并的分支必须拆小或先同步 `dev`
- `hotfix/*` 合入 `main` 后，必须再同步回 `dev`

## 3. Development Workflow

标准流程：

1. 先拉最新代码：
   - `git checkout main && git pull`
   - `git checkout dev && git pull`
2. 从 `dev` 创建工作分支。
3. 只在自己的 feature branch 上开发。
4. 保持提交聚焦：
   - 一个提交解决一个清晰问题
   - 不把无关重构和功能修改混在一起
5. 如果改动涉及 API、数据库、配置或共享模块，先开 draft PR 或先发出同步说明。
6. 每天至少同步一次 `dev`，冲突尽早解决，不要攒到最后。
7. 提交 PR 之前先跑本地质量检查。
8. PR 默认合入 `dev`。
9. `dev` 验证稳定后，再由 `dev -> main` 发 release PR。

当前后端最少本地检查：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

前端加入仓库后，凡是触达前端代码的 PR，必须额外跑前端的 build / lint / test。

## 4. Pull Request Rules

PR 基本规则：

- 不允许绕过 PR 直接合并
- PR 默认目标分支是 `dev`
- `main` 只接受：
  - `dev -> main`
  - `hotfix/* -> main`
- 一个 PR 只做一件事
- PR 必须小而聚焦

建议尺度：

- 目标是 reviewer 可以在 20 到 30 分钟内看完
- 目标改动量控制在 400 行以内，不含文档、锁文件、生成文件
- 超过 800 行的 PR 默认先拆分，除非是明确批准的迁移或机械性变更

PR 必须写清楚：

- 改了什么
- 为什么改
- 风险点是什么
- 怎么验证
- 是否改了 API contract
- 是否改了 schema / migration
- 是否改了 env/config
- 是否使用了 AI 辅助

下列内容必须拆成独立 PR：

- 功能开发和大规模重构
- schema 变更和无关业务逻辑
- 前端 UI 改动和后端接口重命名
- 文档整理和运行时行为修改

## 5. Code Review Rules

合并要求：

- 每个 PR 至少 1 个真人 approval
- 作者不能用“AI 跑过了”替代人工 review
- reviewer 有权要求拆 PR、补测试、补文档或改成 feature flag

review 重点：

- 是否会破坏 `main` / `dev`
- 是否覆盖了测试
- 是否引入了 API 不兼容变更
- 是否误改了别人的模块
- 是否把 runtime / secret / live asset 带进了版本库
- AI 生成代码是否真的符合仓库约束

评论处理规则：

- 未解决的 review comment 不能直接 merge
- 如果 reviewer 提出 blocker，作者必须修改或明确记录不采纳原因
- 对共享接口、共享 schema、共享配置的 review，默认比样式问题优先级更高

## 6. Frontend / Backend Collaboration Rules

API contract 是前后端协作边界，不能靠口头记忆。

规则：

- 任何 API 变更都必须显式记录
- 任何 breaking change 都必须先同步，再开发
- API 字段名、字段含义、错误结构、认证方式变化，都算 contract change

当前约定：

- API contract 文档统一放在 `sourcing-ai-agent/docs/`
- 如变更影响已有流程，至少更新以下之一：
  - `sourcing-ai-agent/docs/BACKEND_MVP.md`
  - `sourcing-ai-agent/docs/ARCHITECTURE.md`
  - 新增专门的 contract 文档
- PR 描述里必须附：
  - 示例 request
  - 示例 response
  - 兼容性说明

前后端同步规则：

- 后端改 API 之前，先发 draft PR 或 issue 说明
- 前端未确认前，不合并会破坏现有调用方式的改动
- 前端需要的 mock 数据结构，也必须和 contract 一致
- 不允许“后端先随便改，前端之后再猜”

## 7. Ownership Rules

当前默认 ownership：

- `sourcing-ai-agent/src/**`
  - 后端 owner 主责
- `sourcing-ai-agent/tests/**`
  - 改动业务逻辑的人负责补和修
- `sourcing-ai-agent/configs/**`
  - 改对应功能的人负责
- `sourcing-ai-agent/docs/**`
  - 共享 ownership，但涉及 API / runtime / workflow 的改动要通知对应 owner
- `README.md` / `ONBOARDING.md` / `.github/**`
  - 共享 ownership
- `Anthropic华人专项/`、`anthropic-employee-scan/`、`biz-visit-onepager/`、`investor-chinese-scan/`
  - 默认视为历史资产区，非必要不改

跨 owner 修改规则：

- 可以改别人的模块，但必须在 PR 里明确说明影响范围
- 改共享模块前，优先发 draft PR 或先同步
- 不允许为了“顺手整理”改动大量无关文件

当未来前端目录进入仓库时：

- 前端目录默认由前端 owner 主责
- 任何跨前后端 PR，优先拆成两个 PR，或至少拆成两个 commit 区块

## 8. AI (Codex) Usage Guidelines

AI 使用规则：

- Codex 只能在 feature branch 上工作
- Codex 不能直接向 `main` 或 `dev` 提交代码
- 所有 AI 生成代码都必须由人类作者自己读过、理解过、确认过
- AI 不得执行未审阅的大规模跨目录重构
- AI 生成的 schema、配置、脚本、workflow 变更，必须在 PR 里解释

明确禁止：

- 用 AI 一次性改前端、后端、配置、文档、测试多个层面且不拆 PR
- 让 AI 直接处理受保护分支
- 让 AI 未经检查地改 `runtime/`、secret、历史 live asset、导出 bundle
- 让 AI 做“全仓自动格式化 + 顺手重构”后直接合并

额外审慎条件：

- 如果 AI 改动超过 3 个目录
- 如果 AI 同时改动前端和后端
- 如果 AI 改动 API contract 或 DB schema

出现以上情况时，必须拆 PR 或提高 review 强度。

## 9. CI / CD Requirements

GitHub 仓库必须配置以下保护：

- `main` 保护分支
- `dev` 保护分支
- Require pull request before merging
- Require at least 1 approval
- Require conversation resolution
- Require status checks to pass
- 禁止 force push 到 `main` / `dev`

每个 PR 必须通过：

- build
- lint
- test

当前执行口径：

- 后端 PR：
  - 至少通过后端 test
  - 应补后端 lint 和 import/build sanity check 的 CI
- 前端 PR：
  - 必须通过前端 build / lint / test
- 同时改前后端：
  - 两侧都要过

硬规则：

- CI 红灯不能 merge
- 如果某个触达区域还没有自动化检查，PR 里必须写明人工执行的命令和结果
- `main` 必须始终保持 green

当前仓库的实际限制：

- 这个私有仓库当前账号/套餐下，GitHub API 对 branch protection 和 rulesets 返回 `HTTP 403`
- 也就是说，这些保护规则目前不能由平台强制执行

在升级 GitHub 套餐或改为 public 之前，临时执行方式如下：

- 默认所有改动都走 PR，不走直推
- 默认只有 PR 经过另一位开发者确认后才允许 merge
- 默认只把 `main` 当 release 分支使用，不在 `main` 上日常开发
- 默认由合并者在 merge 前手动检查 CI 和 checklist，而不是假设 GitHub 会拦截
- 一旦 GitHub 侧支持 branch protection，应立即按本文件补齐强制规则

## 10. Feature Safety Rules

未完成功能不能直接污染主路径。

规则：

- 半成品功能必须放在 feature flag、配置开关或默认关闭路径后面
- 默认行为不能因为未完成代码而改变
- 没有 feature flag 的实验性代码，不合并进 `dev`

对本仓库的具体要求：

- 新 source family、新 connector、新 workflow 分叉，在未验证前默认关闭
- 影响 live acquisition 成本或行为的路径，必须有明确开关和停止条件

## 11. Database & Config Rules

数据库规则：

- schema 变更必须带 migration
- migration PR 必须写清楚：
  - 升级方式
  - 回滚方式
  - 是否向后兼容
- 不允许只改代码，不补 migration

配置规则：

- 新增 env var 必须更新示例配置或文档
- 不允许在仓库里提交真实 secret
- `runtime/secrets/providers.local.json` 只允许本地存在，不进 Git
- 修改 `configs/*.json` 默认值时，PR 必须写明行为变化

## 12. Sync & Merge Rules

同步规则：

- 每天至少同步一次 `dev`
- 打开 PR 前必须再同步一次 `dev`
- 遇到共享文件冲突要尽早处理，不要拖到 merge 前一刻

冲突处理规则：

- 先理解双方改动，再 resolve
- 不允许直接覆盖对方改动求快
- 涉及 API contract、schema、共享配置的冲突，优先同步后再解
- 如果冲突说明 PR 已经过大，先拆 PR 再继续

## 13. Anti-patterns

以下行为禁止：

- 直接 push 到 `main` 或 `dev`
- 合并红灯 PR
- 一个 PR 混合功能、重构、格式化、文档和数据文件
- 未同步前就大规模改共享文件
- 未记录 contract 就修改接口
- schema 变更不带 migration
- 把 `runtime/`、live payload、provider cache、bundle、secret 提交进 Git
- 用 AI 做失控的大范围改动
- 长时间不合并的超大分支
- 在历史资产目录做无说明的大规模改写

## 14. Example Workflow

示例：后端新增一个 retrieval API 字段，前端要消费它。

1. 后端从 `dev` 拉 `feat/backend-retrieval-field`。
2. 后端先开 draft PR，说明字段变化、示例 response、兼容策略。
3. 前端看到 draft PR 后，从 `dev` 拉 `feat/frontend-retrieval-field`，按同一个 contract 开发。
4. 后端 PR 先合入 `dev`。
5. 前端同步最新 `dev`，完成联调。
6. 两边 CI 通过后，前端 PR 合入 `dev`。
7. 验证 `dev` 稳定，再发 `dev -> main` 的 release PR。

## 15. Future Tightening

如果团队从 2 人扩大，优先增加：

- `CODEOWNERS`
- 更细的 CI matrix
- 必填 release note
- 更明确的 API contract 文件结构
- 更严格的 approval 数量
