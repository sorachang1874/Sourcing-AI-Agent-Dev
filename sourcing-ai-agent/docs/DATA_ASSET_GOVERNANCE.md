# Data Asset Governance

## Goal

这份文档定义 `Sourcing AI Agent` 的数据资产管理规则，目标是解决下面几类问题：

- 新 snapshot 上传后，旧版本如何保留、降级、归档或淘汰
- 同一组织下不同团队、不同 scope 的资产如何隔离，不互相覆盖
- 哪些文件属于“可长期复用的数据资产”，哪些只是一次性执行痕迹
- 本地 runtime、云端 object storage、SQLite 和 retrieval artifact 之间如何分层

这份规则是工程约束，不是建议。

## Core Objects

### 1. Company Asset

定义：某个组织的基础 roster + enrichment + normalization 资产。

例子：

- `thinkingmachineslab / 20260407T181912`
- `anthropic / 202604xxTxxxxxx`

用途：

- 后续 retrieval / filtering / semantic recall 的基础输入
- 可以被多个 query 复用

### 2. Scoped Asset

定义：同一组织下，针对更窄 scope 构建的独立资产，不等同于“query 结果”。

例子：

- `google / team / veo`
- `google / team / nano_banana`
- `google / org / deepmind_gemini`

规则：

- scoped asset 必须显式引用 parent company asset
- scoped asset 不能覆盖 base company asset
- scoped asset 不是 query 的临时过滤结果，而是有自己 acquisition/enrichment 边界的可复用资产

### 3. Retrieval Artifact

定义：某次请求的检索、过滤、排序、总结结果。

例子：

- `runtime/jobs/<job_id>.json`
- `runtime/live_tests/.../summary.json`

规则：

- retrieval artifact 不是 company asset
- retrieval artifact 可以引用 company/scoped asset，但不能反过来替代它们
- query 变化不应产生新的 company asset，除非 acquisition/enrichment 边界真的发生了变化

### 4. Bundle

定义：用于跨设备、云端同步、恢复的可移植导出单元。

例子：

- `company_snapshot bundle`
- `company_handoff bundle`
- `sqlite_snapshot bundle`

补充：

- `company_handoff bundle` 仍是受支持的导出格式
- 但当前服务器/云端恢复默认基线应优先使用 `company_snapshot + sqlite_snapshot`
- canonical 恢复清单见 `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md`

规则：

- bundle 是传输和恢复边界，不是业务语义边界
- 业务上的 canonical/superseded/partial 必须记录在 metadata 和 registry 中，不能只靠 bundle 名称猜

## Hard Rules

### 1. Snapshot Immutable

- 任何已发布到云端的 snapshot 一律视为不可变
- 修复旧 snapshot 时，创建新的 `snapshot_id`
- 不允许“覆盖式修补”已发布 canonical snapshot

### 2. Promotion States Required

每个 snapshot 必须有明确状态：

- `draft`
- `partial`
- `canonical`
- `superseded`
- `archived`

解释：

- `draft`：本地执行中，尚未用于他人消费
- `partial`：上传了，但覆盖不完整或质量未验证
- `canonical`：当前默认消费版本
- `superseded`：已被更新版本替代，但仍保留可回滚
- `archived`：只保留审计价值，不再默认下载或分析

### 3. One Default Pointer Per Scope

默认指针必须是 `(company_key, scope_kind, scope_key, asset_kind)` 维度唯一。

例子：

- `thinkingmachineslab / company / thinkingmachineslab / company_asset` 只能有一个 `latest canonical`
- `google / team / veo / scoped_asset` 只能有一个 `latest canonical`

这意味着：

- `Google Veo` 和 `Google Nano Banana` 必须是两个 scope，不允许都写成 `google/latest`
- 不同 scope 的 `latest` 指针互不影响

### 4. Historical Captures Inherit Forward

以下资产属于高价值长期资产，后续新 snapshot 默认继承，不应每轮 full workflow 后重新丢失：

- explicit profile captures
- manual review confirmed member / non-member decisions
- canonical dedupe identity
- validated LinkedIn URLs
- durable evidence links

如果新 snapshot 中出现冲突：

- 默认保留更高置信度、更新鲜、人工确认过的版本
- 不得因为新的弱信号而回退到更差状态

### 5. API Call Lineage Must Be Stored

凡是高成本或关键外部调用，必须至少持久化：

- request params
- provider name
- provider mode
- run id / dataset id / task id
- raw response path
- normalized output path
- timestamp

目标：

- 不再依赖 payload hash 反推调用参数
- 不再为了确认旧设置而重复打 provider

### 6. Asset View Is Not Snapshot State

`canonical_merged` 和 `strict_roster_only` 是同一 snapshot 下的分析视图，不是两个独立 snapshot。

规则：

- snapshot 生命周期按 snapshot 管
- view 生命周期按 materialized artifact 管
- query 结果必须显式记录使用了哪个 `asset_view`

## Storage Layout Rules

推荐的云端语义布局：

```text
{prefix}/
  company_assets/
    {company_key}/
      company/
        {company_key}/
          {snapshot_id}/
      team/
        {scope_key}/
          {snapshot_id}/
      org/
        {scope_key}/
          {snapshot_id}/
  retrieval_artifacts/
    {company_key}/
      {job_id}/
  manual_review_assets/
    {company_key}/
      {review_id}/
  registries/
    company_asset_index.json
    scoped_asset_index.json
    retrieval_index.json
```

规则：

- `company_key` 是组织身份
- `scope_kind + scope_key` 是边界
- `snapshot_id` 是版本
- retrieval artifact 单独存，不与 company asset 混放

## Version Management Policy

### When A Better Snapshot Is Uploaded

例如 Thinking Machines Lab 有了更完整的新数据后：

1. 上传新的 immutable snapshot
2. 新 snapshot 先标记为 `partial` 或 `draft`
3. 运行验证
4. 验证通过后提升为 `canonical`
5. 上一个 canonical 改为 `superseded`
6. 旧的明显不完整 snapshot 不立即删除，保留回滚窗口

### Monthly Refresh Policy

对于月更组织：

- 每月新跑生成新 `snapshot_id`
- 过去的 monthly canonical 不覆盖，保留时间序列
- 默认保留最近 `12` 个 monthly canonical
- 里程碑 snapshot 可以长期保留
- `partial` / 明显失败的 snapshot 可以在 `30-90` 天后归档或清理

### Old Incomplete Uploads

之前已经上传到云端、但被认定为不完整的资产：

- 不直接 hard delete
- 先在 registry 中标为 `superseded` 或 `partial`
- 默认下载、默认分析、默认 UI 列表不再指向它
- 只有在确认有替代版本且无审计需求时，才进入物理删除队列

## Scope Management Policy

### Company vs Team vs Query

必须分清三层：

- `company asset`
  - 目标是组织级基础资产
- `scoped asset`
  - 目标是组织内某个 team / org / geography / function 的可复用资产
- `retrieval artifact`
  - 目标是针对某个用户请求的结果交付

错误做法：

- 因为一次 query 提到了 `Google Veo`，就把结果直接覆盖到 `google` 的主资产里
- 因为一次 query 需要 `Nano Banana`，就把它当成新的 company

正确做法：

- `Google` 是 identity
- `Veo` / `Nano Banana` 是 scope
- query 产物属于 retrieval artifact

## Retention Policy

建议保留规则：

- canonical company/scoped snapshot：长期保留
- superseded canonical：至少保留 `90` 天，推荐保留 `12` 个历史版本
- partial / failed snapshot：保留 `30-90` 天
- manual review asset：长期保留
- explicit profile raw capture：长期保留
- retrieval artifact：可按 `90-180` 天保留，前提是其引用的上游资产仍存在
- 临时缓存、低价值重复 raw payload：允许清理

## Required Metadata

每个已发布 snapshot 至少应记录：

- `asset_kind`
- `company_key`
- `scope_kind`
- `scope_key`
- `snapshot_id`
- `status`
- `created_at`
- `source_run_kind`
- `parent_snapshot_id`
- `supersedes_snapshot_id`
- `bundle_id`
- `coverage_summary`
- `quality_summary`
- `asset_views`
- `schema_version`

## Publish Checklist

发布前必须满足：

- raw asset 已落盘
- normalized artifact 已落盘
- asset summary 已生成
- snapshot metadata 完整
- object storage upload summary 完整
- latest pointer 更新前已完成验证
- predecessor state 已更新

## Current Recommended Implementation

在当前代码基础上，建议执行规则如下：

- `runtime/company_assets/.../<snapshot_id>/` 仍然是本地主事实源
- 云端以 `company_snapshot bundle` 为发布单元
- 发布后额外维护一份 registry，而不是只靠 object prefix 浏览
- `latest_snapshot.json` 只代表本地默认版本；云端还需要自己的 `latest canonical` registry 记录

## Next Engineering Step

当前规则已经可以执行，但还需要继续产品化为代码约束：

1. 给 bundle metadata 增加 `status / scope_kind / scope_key / supersedes_snapshot_id`
2. 在 object sync 时生成 cloud-side registry index
3. 增加“promote snapshot”命令，而不是手工改 latest pointer
4. 增加 partial/superseded/archive 生命周期操作
5. 把 team-scoped asset 从“query 约定”升级为显式目录与元数据模型
