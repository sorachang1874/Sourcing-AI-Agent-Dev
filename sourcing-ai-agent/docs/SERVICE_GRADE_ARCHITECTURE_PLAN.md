# Service-Grade Architecture Plan

> Status: Design/reference doc. Useful for product or architecture context, but not the source of truth for current runtime behavior.


## Goal

把当前 Sourcing AI Agent 从“本地可用的工作流原型”升级成适合长期迭代的服务级系统，避免以后继续反复做大规模 infra / storage / runtime 重构。

这份方案关注 5 个问题：

- 结果服务仍然隐式依赖整包 JSON。
- 新 query 仍会把资产语义和 runtime 目录强耦合。
- SQLite 还混合承担 control plane 和 blob transport 职责。
- query-time 结果视图和 company-level authoritative asset 没有完全分层。
- 部分能力仍不是 AI-first，而是靠规则、路径约定和后置补丁兜底。

## Current Anti-Patterns

### 1. Runtime-copy as data plane

现在很多流程仍默认把资产“落到 runtime 再消费”。这会带来：

- 同一份 company asset 在多个 query/job 间重复复制。
- ECS / 本地磁盘同时承担真相源和缓存层职责。
- hosted / local / bundle restore 三条路径难以统一。

### 2. Snapshot monolith as hidden dependency

虽然前端已经改成 `dashboard + page + detail`，但后端直到这次 `Phase 2` 之前，分页接口底层还是会先读整份 `materialized_candidate_documents.json` 再切片。

### 3. Query result and canonical asset are not cleanly separated

当前 job 有时直接绑定 query-specific snapshot，有时又绑定 company baseline，但两者都通过 `candidate_source` 和 runtime path 约定来表达，缺少明确的“结果视图层”。

### 4. SQLite is overloaded

SQLite 现在同时承担：

- job state
- review state
- candidate/evidence registry
- materialization bookkeeping
- 某些 serving 回落时的临时真相源

这会让“只改 control plane”与“只改 serving artifact”变得困难。

## Design Principles

### 1. Split planes explicitly

系统应拆成 5 层：

- Control plane
  - job
  - review state
  - execution state
  - asset registry
  - query/result view metadata
- Canonical entity plane
  - candidate
  - evidence
  - profile registry
  - company identity registry
- Materialized serving plane
  - manifest
  - candidate shards
  - list pages
  - backlog views
  - compact overlays
- Blob/object plane
  - raw provider payloads
  - materialized shards/pages
  - exported bundles
- Runtime cache plane
  - local hot cache only
  - never the canonical truth source

### 2. Query should reference assets, not copy assets

每个 query/job 最终应该只记录：

- 它引用的 authoritative snapshot / snapshot generation
- 它叠加的 delta acquisition artifact
- 它选择的 result view / filter projection

而不是在 runtime 下再复制一份 company assets 给这个 job 独享。

### 3. AI-first means AI where ambiguity exists

AI 应该主要放在高不确定性环节：

- company identity resolution
- query intent expansion
- evidence synthesis
- manual review prioritization
- outreach / segmentation reasoning

不该把 AI 用在本应 deterministic 的 control plane / storage orchestration 上。

## Service-Grade Working Rules

这份架构计划不只是“未来可选方向”，而是当前升级阶段的默认开发约束。

1. 优先做根因修复，不做只覆盖单个测试用例的表层补丁。
2. 先做影响面分析，再动代码：
  - 类型
  - schema
  - config
  - shared variable
  - callsite
  - tests
3. 一旦共享契约变更，同步更新所有下游消费者，不留下一轮再补。
4. 默认允许较大的模块化/存储/架构改造，只要能显著减少未来重复 infra 重构。
5. 不再新增新的 runtime-path 双轨约定。
6. 新代码默认不得直接拼接 `runtime/company_assets/...` 作为核心读取契约；应走共享 resolver。
7. 在交付前必须重新跑 simulate-only 验证，不真实调用 provider 也要确保整条工作流可恢复、可重放、可服务。
8. 当前升级阶段先不自动同步 ECS，等架构面收敛后再统一迁移。

## Target Architecture

## 1. Authoritative Asset Registry

新增明确的 authoritative registry 语义：

- `organization_assets`
  - authoritative snapshot / generation pointer
  - available views
  - freshness / completeness watermark
- `snapshot_materialization_runs`
  - snapshot-level materialization health
- `candidate_materialization_state`
  - candidate fingerprint
  - shard path
  - page index
  - materialized_at

这样 job 不必优先绑到 query-specific live snapshot。

full-reuse query 的正确姿势应是：

- 直接钉住 authoritative snapshot
- 只额外记录本次 query 的 intent / projection

## 2. Query Result View Layer

需要显式引入 `job_result_view` 概念。

每个 job 只存：

- `source_snapshot_ref`
- `source_generation_key`
- `view_kind`
  - `asset_population`
  - `ranked_results`
  - `hybrid`
- `filter_projection`
  - function ids
  - employment scope
  - manual review status
  - user-confirmed company linkedin url / slug
- `delta_artifact_refs`

这样：

- 同一公司 baseline 不再为每个 query 复制目录
- query 只构造“视图”，不复制“资产”

## 3. Serving Artifact Contract

统一采用以下 contract：

- `normalized_artifacts/manifest.json`
  - counts
  - pagination
  - shard index
  - backlog index
  - auxiliary index
- `normalized_artifacts/pages/page-XXXX.json`
  - 只放 board/list 所需字段
- `normalized_artifacts/candidates/<candidate_id>.<fingerprint>.json`
  - 单 candidate 的完整详情
- `normalized_artifacts/backlogs/manual_review.json`
- `normalized_artifacts/backlogs/profile_completion.json`
- `normalized_artifacts/publishable_primary_emails.json`
  - 紧凑 overlay，不再通过 strict-view monolith 回补

结果页 API 也应围绕这个 contract：

- `GET /api/jobs/{job_id}/dashboard`
- `GET /api/jobs/{job_id}/candidates?offset=&limit=`
- `GET /api/jobs/{job_id}/candidates/{candidate_id}`
- 后续可继续加：
  - `GET /api/jobs/{job_id}/backlogs/manual-review`
  - `GET /api/jobs/{job_id}/backlogs/profile-completion`

## 4. Object Storage First, Local Cache Second

长期目标不是“把所有东西都放 ECS 本地盘”，而是：

- control plane DB
  - SQLite in dev
  - Postgres in hosted/prod
- object storage
  - OSS / R2 / S3-compatible
  - raw assets
  - materialized serving artifacts
  - exports / bundles
- local disk
  - hot cache
  - temp workspace
  - active run logs

这意味着：

- 本地 runtime 目录只是缓存和工作区
- 不是 canonical storage
- 不是 query 级复制的落点

补充约束：

- canonical store 决定 authoritative snapshot / latest pointer / registry 治理结果
- hot cache 负责 serving/recovery 的本地低延迟读取
- governance / repair / registry / backfill 默认操作 canonical store
- serving / refresh / recovery 默认优先尝试 hot cache，再回落 canonical store

## 5. Materialization V2

### Current foundation

`Phase 1` 已经具备：

- candidate fingerprint
- shard materialization
- page materialization
- backlog materialization
- dirty-set tracking

### Next step

把 materialization 彻底升级为“append/update shards + regenerate indexes”：

- candidate shard only for dirty candidates
- page/index/backlog rebuild only at index layer
- no full snapshot rewrite as the default path

需要继续推进两件事：

1. `materialized_candidate_documents.json` 从 serving dependency 降级成 compatibility artifact。
2. shard/page/backlog 发布直接走 object storage，同步写本地 hot cache。

## 6. Company Identity Resolution

`Safe Superintelligence Inc -> ssi-ai` 这种问题说明当前 slug 解析仍然不够服务级。

目标应改成 4 层 resolver：

1. Deterministic registry
  - builtin
  - bundled seed
  - imported runtime registry
  - user-corrected alias registry
2. Retrieval candidates
  - LinkedIn company search candidates
  - observed company identities
3. LLM adjudication
  - 只在 ambiguity 高时介入
4. Human confirmation
  - review plan 阶段展示识别到的 company LinkedIn URL
  - 允许用户直接修正

用户修正后应写回：

- company identity registry
- alias registry
- future prompt context / observed resolution memory

这样以后同类问题不必重新问模型。

## 7. AI-First Gaps Still Remaining

以下环节还没有完全做到 AI-first：

### Company resolution

- 目前 deterministic seed 不足时，依赖 heuristic slug 和零散 fallback。
- 需要改成“retrieval candidate set + model adjudication + human correction memory”。

### Planning and review

- 计划阶段已有结构化 gate，但缺少“用户纠正后自动沉淀为 resolver memory”的闭环。

### Materialization prioritization

- 现在 dirty rebuild 已经是 candidate-level，但还缺少 AI-assisted priority lanes：
  - first-screen candidates
  - manual-review-needed candidates
  - profile-gap candidates

更合理的方式是：

- 先物化 screen-critical shards
- 再异步补完长尾

### Evidence synthesis

- 目前 evidence 仍偏“采集后再格式化”。
- 后续应更明确区分：
  - raw evidence
  - normalized evidence
  - synthesized candidate narrative

## 8. Execution Roadmap

### Phase 2

这次已经落地的方向：

- serving path 改为优先读取 `manifest/pages/shards`
- strict roster publishable email 改为轻量 overlay artifact
- detail/page path 不再默认依赖 monolith payload

### Phase 3

建议下一轮完成：

- `job_result_view` registry
- job 对 authoritative snapshot 的引用化
- 去掉“每个 query 复制 company assets 到 runtime”
- 让 runtime 只保留 hot cache

当前已进入落地中的部分：

- `job_result_view` 已进入结果服务主路径
- refresh / recovery 已开始优先从 `job_result_view` 恢复 snapshot
- snapshot/source-path 解析正收口到共享 resolver
- canonical-store / hot-cache 分层已开始进入核心读取链路

下一步约束：

- 不再新增直接依赖 `runtime/company_assets` 的热路径代码
- recovery、registry、artifact reader 要继续统一到同一套 resolver / store contract
- runtime copy 语义需要继续下沉为 cache 行为，而不是 job 级真相源

### Phase 4

服务化落地：

- Postgres 作为 hosted control plane
- object storage 作为 blob plane
- local/hosted 共用同一 artifact contract

### Phase 5

AI-first closing loop：

- company identity correction memory
- plan review corrections -> registry
- materialization priority lanes
- evidence synthesis specialization

## What This Avoids

如果按这个方案推进，后面可以避免再做这些高成本返工：

- 为了 hosted 再改一次 runtime path 语义
- 为了 ECS 磁盘压力再改一次 snapshot 存储布局
- 为了前端首屏性能再改一次结果 API contract
- 为了 slug / company identity 识别再堆更多 heuristic patch
- 为了 partial materialization 再推翻现有 shard 方案

## Practical Rule

从现在开始，任何新功能如果涉及“结果如何给前端看”，默认应遵守两个规则：

1. 先问它属于 control plane、canonical plane 还是 serving plane。
2. 新 query 不允许再以“复制公司资产目录到 runtime”作为默认实现路径。
