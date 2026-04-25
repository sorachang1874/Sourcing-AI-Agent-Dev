# Incremental Materialization Plan

> Status: Design/reference doc. Useful for product or architecture context, but not the source of truth for current runtime behavior. Latest implementation notes are in `../PROGRESS.md`.


## 1. Current bottleneck

现在前端慢，不只是因为 provider 慢，更因为结果页消费的资产还是按“整份 snapshot 一次性物化”设计的。

当前热点主要在：

- `src/sourcing_agent/candidate_artifacts.py`
  - `build_company_candidate_artifacts(...)`
  - `_build_artifact_view_payloads(...)`
- 现状是：
  - 先把候选人全集和证据全集重新扫一遍
  - 每个 candidate 再做 profile timeline resolve
  - 最后整份重写：
    - `materialized_candidate_documents.json`
    - `normalized_candidates.json`
    - `reusable_candidate_documents.json`
    - backlog JSON

这会带来 4 个问题：

- 只新增 1 个 baseline candidate，也要重扫整个 snapshot。
- profile completion / manual review 只改少数 candidate，也要重写整份结果文件。
- 前端首屏必须等大 JSON 准备好，无法先拿 summary 再分页拉列表。
- SQLite 现在更像“混合真相源 + 中转站”，而不是轻量 control plane，所以很难做 candidate-level dirty rebuild。

2026-04-24 更新：

- candidate-level shard / page / manifest layout 已是当前主路径，不再只是设计稿。
- strict view 与 canonical view 完全一致时会 alias 复用 canonical serving artifacts。
- profile timeline source JSON 读取已按文件 mtime/size 缓存，并为多候选 `items/results/candidates` source 建立 selector index，避免大 source 文件在 cold build 中被每个 candidate 反复读取和线性扫描。
- materialization write phases 已接入 shared `materialization_writer` global in-flight budget，避免多个 workflow 同时冷物化时互相放大 IO/DB 写压力。
- snapshot candidate-document sync 主入口也已进入 shared `materialization_writer` budget；writer slot 支持同线程 reentrant，避免 outer sync + inner artifact write 在 budget=1 时死锁。
- Harvest profile scraper adapter 已支持 provider batch response callback，enrichment 可以在单 batch 返回后立即更新 registry/fetched cache。
- 仍值得继续做的是跨进程 delta writer queue / partial dataset callback，以及真实大 snapshot benchmark 的持续比较。

## 2. Target architecture

目标不是“换一个更快的 SQLite 写法”，而是把物化边界从 snapshot 级改成 candidate 级。

### 2.1 Truth layers

保留 3 层真相源：

- Raw assets
  - harvest profiles
  - search seed raw payload
  - public web raw payload
  - manual review raw artifacts
- Canonical entities
  - `candidates`
  - `evidence`
  - profile registry / candidate state registry
- Materialized serving artifacts
  - 只用于前端和结果页
  - 不再作为唯一真相源

### 2.2 New serving layout

把现在的单体 `materialized_candidate_documents.json` 拆成：

- snapshot manifest
  - `normalized_artifacts/manifest.json`
  - 只放 snapshot summary、watermark、分页索引、candidate shard list
- candidate shards
  - `normalized_artifacts/candidates/{candidate_id}.{fingerprint}.json`
  - 每个 candidate 一份完整前端消费对象
- list pages
  - `normalized_artifacts/pages/page-0001.json`
  - 每页只放轻量列表字段，不放完整 evidence blob
- backlog views
  - `normalized_artifacts/backlogs/profile_completion.json`
  - `normalized_artifacts/backlogs/manual_review.json`

这样首屏只需要：

- manifest
- page 1

而不是把几百上千人的完整 materialized payload 一次拉下来。

## 3. Candidate-level incremental materialization

### 3.1 Candidate fingerprint

给每个 candidate 维护一个 materialization fingerprint，输入至少包括：

- candidate canonical record
- candidate evidence ids + evidence updated_at
- resolved profile timeline watermark
- manual review resolution watermark
- candidate review status watermark

只要 fingerprint 不变，就不重建该 candidate shard。

### 3.2 Dirty-set registry

新增一层轻量 registry，职责是回答：

- 这个 snapshot 里哪些 candidate 是 dirty 的
- 这次 dirty 的原因是什么
- 上次成功物化用的 fingerprint 是什么

建议新增表：

- `candidate_materialization_state`
  - `snapshot_id`
  - `candidate_id`
  - `fingerprint`
  - `shard_path`
  - `list_page`
  - `materialized_at`
  - `dirty_reason`
- `snapshot_materialization_runs`
  - `snapshot_id`
  - `run_id`
  - `status`
  - `dirty_candidate_count`
  - `completed_candidate_count`

### 3.3 Incremental rebuild flow

新流程应是：

1. snapshot ingest 完成后，先写 canonical candidate/evidence。
2. 计算每个 candidate fingerprint。
3. 只重建 dirty candidates 的 shard。
4. 重新生成 manifest、page index、backlog index。
5. 不重写未变化 candidate 的 shard 文件。

这样：

- 新 baseline 只会影响新增 candidate
- profile completion 只影响被补全的 candidate
- manual review 只影响被审核 candidate 和相关 backlog

## 4. SQLite should become control plane, not blob store

当前痛点不是 SQLite 不能存数据，而是它现在承载了太多“面向前端的一次性重组职责”。

更合理的角色划分：

- SQLite / Postgres
  - job state
  - candidate/evidence registry
  - materialization state
  - page index / backlog index
  - review state
- filesystem / object storage
  - raw assets
  - candidate shards
  - page payloads
  - manifest payloads

如果后面云端要更接近本地，推荐最终形态：

- Postgres 做 control plane
- OSS / R2 / S3-compatible object storage 放 raw assets 和 materialized shards
- ECS 本地磁盘只保留热缓存和最近运行产物

如果短期还保留 SQLite，也应该先把它收缩成 control plane，而不是继续把“大结果页完整产物”当 SQLite 驱动的同步重建副产品。

## 5. Frontend API changes

为了真正把首屏延迟压下去，API 也要跟着拆。

建议把 `GET /api/jobs/{job_id}/results` 拆成：

- `GET /api/jobs/{job_id}/results/summary`
  - candidate count
  - backlog counts
  - stage summary
  - asset source summary
- `GET /api/jobs/{job_id}/results/candidates?page=1&page_size=50`
  - 列表页
  - 只返回 board 所需字段
- `GET /api/jobs/{job_id}/results/candidates/{candidate_id}`
  - 返回候选人详情
  - 再带完整 experience/education/evidence
- `GET /api/jobs/{job_id}/results/backlogs/manual-review`
- `GET /api/jobs/{job_id}/results/backlogs/profile-completion`

结果页默认：

- 先拉 summary
- 再拉 page 1
- 详情按需加载

这比继续优化单体 JSON 更有效。

## 6. Company identity / LinkedIn slug resolution

当前链路是：

1. `domain.JobRequest.from_payload(...)`
   - 若没显式传 `target_company`，先走 `infer_target_company_from_text(...)`
2. `company_registry.infer_target_company_from_text(...)`
   - 用 builtin / seed / local registry alias 做公司名识别
3. `connectors.resolve_company_identity(...)`
   - 先看 builtin / seed / local identity
   - 再看 legacy mapping
   - 失败时才尝试 observed companies + model adjudication
   - 最后退回 heuristic slug
4. `acquisition._resolve_company(...)`
   - 若还是 low-confidence heuristic，才补 observed company search

`Safe Superintelligence -> ssi-ai` 之前失败，本质上是：

- 本地 builtin / seed / runtime registry 里没有这条 deterministic mapping
- heuristic slug 只能从公司名规整出 `safesuperintelligence`
- observed candidate fallback 对 `ssi-ai` 这种“缩写型 slug”不稳定

这次先补了 bundled seed，让：

- `Safe Superintelligence`
- `Safe Superintelligence Inc`

都能稳定落到 `ssi-ai`。

### 6.1 Recommended next step

后续不应只靠手工 seed 增长，建议把 company identity resolver 升级成 4 层：

1. deterministic registry
   - builtin
   - bundled seed
   - runtime imported asset identity
2. observed candidate retrieval
   - LinkedIn company search candidates
   - domain/company page hints
3. model adjudication
   - 只在 deterministic 未命中时使用
   - 输出 canonical label、slug、confidence、rationale
4. learn-back path
   - 高置信度且已验证成功的 identity 自动写回 runtime seed / registry

这样“昨天第一次遇到的新公司”不会永远卡在 heuristic slug。

## 7. AI-first gaps still remaining

现在还不够 AI-first 的点，优先级从高到低建议如下。

### 7.1 Company identity resolution

当前仍是 deterministic + heuristic 主导，模型只在末端兜底。

更好的方式是：

- 先检索 observed company candidates
- 再让模型做 conservative adjudication
- 最后把高置信度结果沉淀成 deterministic registry

### 7.2 Intent normalization

很多主题词、scope 词、must-have facet 仍然靠静态规则切分。

建议把意图层明确拆成：

- deterministic guardrails
- model-generated normalized intent contract
- execution-safe downgraded filters

模型负责：

- 主题词 canonicalization
- 公司边界歧义判断
- “这是 hard constraint 还是 soft keyword” 判断

规则层只负责兜底和防止过拟合。

### 7.3 Selective enrichment

Public Web Stage 2 和 profile completion 现在仍偏“阶段式”。

更 AI-first 的做法应是 candidate-level lane planning：

- 哪些 candidate 需要 deeper web evidence
- 哪些 candidate 只需要 profile completion
- 哪些 candidate 已足够，不再继续补

不要对整池人做统一深挖。

### 7.4 Review reason classification

现在 `needs_profile_completion`、`low_profile_richness`、`manual_review_reason` 仍主要是规则推断。

建议保留 deterministic 主标签，但对边界样本补一层 model adjudication：

- sparse provider payload
- target-company mismatch
- suspicious membership
- profile preview vs true detail

模型输出应该被缓存为 adjudication artifact，而不是每次重跑。

### 7.5 Result serving

当前结果页还是“后端先准备一整份大包，前端再展示”。

更 AI-first 的结果层应是：

- summary-first
- candidate-on-demand
- explanation-on-demand

也就是把“完整前端视图”从同步物化改成按需生成和缓存。

## 8. Rollout plan

建议分 4 步做，不要一次重构到底。

### Phase 0

已完成：

- 稀疏 Harvest payload 不再被误判为完整 profile detail
- `Safe Superintelligence Inc -> ssi-ai` deterministic resolve 已补上

### Phase 1

先不动前端 contract，只新增：

- candidate fingerprint
- candidate materialization state table
- candidate shard writer

并让现有 `build_company_candidate_artifacts(...)` 在内部改成“增量重建 + manifest 汇总”。

### Phase 2

新增 results summary / paged candidates / candidate detail API。

前端从单次 `results` 大包改成：

- summary first
- page fetch
- detail fetch

### Phase 3

把 profile completion、manual review resolution、target candidate updates 全部改成 candidate-level patch materialization。

### Phase 4

云端部署时，把 control plane 与 blob storage 分离：

- Postgres
- object storage
- ECS 本地磁盘只做热缓存

## 9. Recommendation

如果下一步只做一件最值回票价的事，优先做：

- Phase 1 的 candidate fingerprint + shard materialization

如果下一步可以同时做前后端配套，优先做：

- Phase 1
- Phase 2

这是把“本地大 snapshot 能跑”升级成“云端同事可稳定用”的关键分界线。
