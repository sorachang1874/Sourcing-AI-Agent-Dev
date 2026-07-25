# Data Asset Governance

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


> Current default: live/hosted storage is `Postgres control plane + generation-first artifacts`. `sqlite_snapshot` is retired and should not be exported, uploaded, downloaded, imported, or restored.

## Goal

这份文档定义 `Sourcing AI Agent` 的数据资产管理规则，目标是解决下面几类问题：

- 新 snapshot 上传后，旧版本如何保留、降级、归档或淘汰
- 同一组织下不同团队、不同 scope 的资产如何隔离，不互相覆盖
- 哪些文件属于“可长期复用的数据资产”，哪些只是一次性执行痕迹
- 本地 runtime、云端 object storage、Postgres control plane 和 retrieval artifact 之间如何分层

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
- 公司级官网、Research、Engineering Blog、论文、RSS/arXiv/OpenReview/crawl bundle、公司 logo 等不属于某个候选人的 PersonAsset。它们应进入公司级资产层；当前实现已有 `company_public_web_asset_runs` / `company_public_web_assets` 作为 API/CLI-only Public Web read model，并新增 PG-only `CompanyAsset` / `CompanyEvidence` / `CompanyAssertion` foundation 作为 canonical 公司资产事实层。
- 公司 logo 必须是稳定 media asset：`CompanyAsset(asset_type='logo_media')` 或经确认的公司 assertion。前端 initials 只能是 `logo_unavailable` 占位，不是资产完成证据。

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
- `control_plane_snapshot bundle`

补充：

- `company_handoff bundle` 仍是受支持的导出格式
- 但当前服务器/云端恢复默认基线应优先使用 `company_snapshot + control_plane_snapshot`
- `sqlite_snapshot` bundle 已退役；旧包只应作为离线历史材料处理，不能再走 product CLI / import / object sync
- canonical 恢复清单见 `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md`

规则：

- bundle 是传输和恢复边界，不是业务语义边界
- 业务上的 canonical/superseded/partial 必须记录在 metadata 和 registry 中，不能只靠 bundle 名称猜

## Hard Rules

### 0. Large Snapshot Consolidation Before Runtime Legacy Deletion

Durable runtime migration must not preserve every historical large snapshot as an active source candidate.

Early production and test environments were not fully separated, so large companies such as Google and Anthropic may have multiple near-duplicate snapshots created by manual testing, scripted runs, hosted experiments, or mixed runtime states. Those snapshots can be valuable audit evidence, but they should not all remain active authoritative-source candidates.

Before deleting old workflow recovery branches or retiring `job_materialization_items` migration adapters, run a data-asset consolidation pass for large local companies.

Required evidence:

- company-level inventory: company key, collection id, snapshot id, creation time, source runtime, candidate count, profile count, raw profile count, projection references, job/run references, CRM/person-asset references
- overlap/subsumption report: member-key/profile-url overlap, unique candidates retained only by each snapshot, profile richness deltas, scoped-shard membership deltas
- authoritative recommendation: which projection/snapshot should remain the active company-level authoritative source
- scoped-shard preservation: which directional shards should remain first-class scope assets, for example Gemini, Veo, Infra, Agent, OpenAI scoped searches, Lovable roster runs
- archive manifest: no-increment duplicate snapshots, contaminated test snapshots, superseded large overlays, and old job-only artifacts that no normal reader should consume
- cold-backup manifest: archived assets copied to durable cold storage before deletion or default-source exclusion
- dependency proof: CRM records, PersonIdentity, PersonAsset, PersonAssertion, exports, run projections, and collection authoritative pointers do not depend on archived snapshots as their only source

Cleanup result:

- local asset consumption uses a clean `collection_authoritative_projection`
- planner local reuse uses authoritative coverage/shard proof, not every historical snapshot
- projection/public readers never union archived snapshots implicitly
- archived snapshots remain available for audit/backfill only through explicit maintenance tooling

This rule is a gate for durable runtime legacy deletion, not a later nice-to-have.

Current implementation foundation:

- `src/sourcing_agent/asset_consolidation_audit.py` implements `asset_consolidation_audit_v1`, a read-only metadata dependency graph for W5.
- `scripts/audit_asset_consolidation.py` writes JSON and optional Markdown evidence for one or more companies.
- W5a classifications are intentionally conservative:
  - `keep_authoritative_serving`: current authoritative registry source.
  - `keep_reusable_shard_source`: snapshot has completed/cap/skipped-high-overlap shard proof and may still power local reuse.
  - `keep_projection_dependency`: snapshot is referenced by a serving projection or projection provenance.
  - `archive_candidate_no_increment_duplicate`: registry-known snapshot with no active blockers.
  - `review_*`: local-only, missing-registry, or otherwise ambiguous snapshot requiring human/migration review.
- Deletion blockers must be resolved before archive/delete: authoritative registry pointer, selected source snapshot, latest local pointer, reusable acquisition shard, active projection dependency, CRM source projection dependency, and PersonAsset source projection dependency.
- W5a does not compute expensive member-level overlap. Run overlap/subsumption only after W5a narrows the candidate set and proves the snapshot is not a current dependency.
- W5b bounded overlap is opt-in with `scripts/audit_asset_consolidation.py --include-overlap`. It reads candidate identity sets only for current authoritative/source snapshots and W5a archive candidates. It prefers LinkedIn sanity/profile URL identity over unstable historical candidate ids.
- W5b overlap reference identity prefers the canonical `collection_authoritative_pointer` when it exists. In that mode, reference identity is loaded from the active `collection_authoritative_projection` members; stale registry authoritative/selected snapshot ids are still reported as `legacy_registry_reference_snapshot_ids` but do not drive overlap. If no canonical pointer exists, W5b falls back to the organization asset registry reference snapshot ids.
- A snapshot with no dependency blockers is still not delete-approved until overlap evidence says `subsumed_by_reference`. Missing local payloads, truncated candidate reads, missing reference identities, or unique identities keep the snapshot in review.
- For multi-company maintenance runs, use `--progress --output-dir <evidence_dir>/companies` so each company writes JSON/Markdown evidence immediately after it finishes. Long W5 audits must be observable and leave partial evidence; they should not behave like black-box nightly runs.
- If authoritative/source reference identity cannot be loaded, W5b must fail closed as `review_reference_identity_missing` and must not scan large archive-candidate payloads. Without a reference identity set, reading historical duplicates cannot prove safe archival and only adds cost. The 2026-05-22 real local audit found this state for Google, Anthropic, and OpenAI because registry authoritative pointers referenced snapshot ids that were not present as local payload dirs.
- W5c planning is also read-only. `scripts/plan_asset_consolidation.py --audit-json <w5-audit.json>` converts audit evidence into required actions: restore/rebuild missing reference payloads, block archive until overlap proves subsumption, preserve reusable shard sources, review local-only snapshots, and review payload-backed authoritative source candidates. The plan may recommend candidates, but it must not switch registry pointers or delete files.
- A payload-backed replacement candidate is not archive approval. Promoting one requires manual review, payload-count verification, publication of a clean authoritative projection/pointer, and another W5b overlap run against the repaired reference identity set.
- W5c repair proposal is read-only verification. `scripts/propose_asset_consolidation_repair.py --plan-json <w5c-plan.json>` verifies local and source-path payloads for candidate authoritative replacements, counts candidate identities, flags stale source paths and registry/payload count mismatches, and emits a recommended candidate only when payload identity exists and registry count matches. Count-mismatch candidates may be useful evidence but must not be auto-promoted.
- W5c apply is a separate reviewed operation. `scripts/apply_asset_consolidation_repair.py --proposal-json <w5c2-proposal.json> --selection <company>=<snapshot>` defaults to dry-run and must not mutate any pointer, projection, registry row, or historical snapshot file.
- W5c apply may publish a new payload-backed `collection_authoritative_projection` and `collection_authoritative_pointer` only with `--apply --reviewed`. The executor validates `verified_payload_available`, nonzero identity count, non-truncated identity scan, and zero registry/payload count delta before writing through `ServingProjectionWriter`.
- W5c apply does not approve archival. It must leave JSON/Markdown evidence and must be followed by W5b overlap against the repaired reference identity set before any archive/cold-backup manifest is generated.
- W5c cold archive manifest is still read-only. `scripts/build_asset_consolidation_cold_archive_manifest.py --plan-json <w5c-plan.json>` emits `asset_consolidation_cold_archive_manifest_v1` with source dirs, backup keys, file sizes, optional per-file sha256, and manifest digests for snapshots whose plan decision is already `archive_ready_for_cold_backup_review`.
- A cold archive manifest is not a deletion operation. It must report `deletion_allowed=false`; ready rows only mean `normal_reuse_exclusion_recommended=true` after a separate cold-copy verification and reviewed apply step. Unique identities, missing local dirs, deletion blockers, symlinks, or truncated file listings must fail closed. Blocked rows must fail before directory/file/hash scanning, so broad W5 runs do not spend I/O on snapshots that cannot be archived by contract.

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

### 7. Snapshot Selection Is Not Shard Membership

`canonical_merged` 当前表示某个 snapshot 下的 canonical artifact view。它可以在 materialization 时读取多个 source snapshots，但读取范围必须由 `source_snapshot_selection` 明确记录。它不是一个自动吞掉所有历史 snapshot 的 evergreen company index。

必须区分四个对象：

- `candidate_documents.json`
  - 单个 snapshot 的规范候选人与 evidence 输入。
- `normalized_artifacts/<asset_view>/`
  - 单个 snapshot 的 serving artifact view，例如 `canonical_merged` / `strict_roster_only`。
- `job_result_view`
  - 某次 job 最终给前端消费的结果指针，必须指向该 job 当前有效的 snapshot / asset view / generation。
- company canonical serving view
  - 公司级默认消费视图，应该由 authoritative registry / default pointer 明确指向某个 snapshot generation，而不是临时把所有历史 snapshot union 起来。

正确的多 sharding 表达：

- 同一个 candidate 可以属于多个 query/source shard。
- 这种归属必须写入同一 candidate record 的 `source_matches` / `matched_keywords`。
- 一个 candidate shard 文件承载一个 canonical candidate，但里面可以有多条 `source_matches`。
- 不允许用“把所有历史 snapshot 合并进一个旧 job snapshot”的方式模拟多 sharding，因为这会把其他时间点、其他 job 的候选人污染到当前 job 结果。

当前代码边界：

- `preferred_source_snapshot_ids` 存在时，materialization 只合并当前 snapshot 和显式 preferred snapshots。
- `current_snapshot_only_large_org` 已停用为默认路径；代码中没有独立的 `former_snapshot_only_large_org` 模式。大组织不能因为 current snapshot 足够大就静默丢弃历史 source snapshots。
- 新 authoritative serving snapshot 被提升时，`organization_asset_registry.selected_snapshot_ids` 必须继承上一条 authoritative row 中仍有 `acquisition_shard_registry` 证明的 source snapshots。serving primary snapshot 和可复用 shard coverage source snapshots 是两层合同，不能相互覆盖。
- `organization_asset_registry.authoritative` 是 serving pointer，不是 full-company roster proof。Planner 必须通过 `population_coverage` / promoted aggregate proof / legacy full-company proof 判断全量人口边界，通过 `acquisition_shard_registry` 判断 exact scoped shard coverage；详见 `AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md`。
- scoped-only authoritative snapshot 可以服务同一个 scoped shard，但不能服务“全部成员”或其他 unrelated scope。大体量 full-company baseline 可以服务 full-company query，但不能自动跳过新 Gemini/Agent/Infra 等方向性 delta，除非 exact shard coverage 或显式 directional reuse contract 存在。
- 没有显式选择集的小 snapshot 仍有历史 fallback 能力，这只是 legacy / repair fallback，不应作为 scoped provenance rebuild 的默认策略。
- 后续 snapshot 治理应显式标记“干净且有价值”的 source snapshots，例如有 scoped shard、profile coverage、job result view 引用或人工确认 baseline；无增量重复 snapshot 应进入 archive manifest，而不是进入 hosted production 默认 source selection。

推荐方法：

1. 新 scoped query 完成后，其 job result view 指向本次 workflow 的 current snapshot。
2. 如果本次 workflow 复用了 baseline 并产生 delta，current snapshot 的 `candidate_documents.json` 应已经包含 baseline + delta 的可服务集合。
3. 前端 filter 读取 `source_matches` / `matched_keywords`，而不是重新从旧 materialized payload 文本里搜索。
4. 如果需要公司级“更全的一人多 sharding”视图，应发布一个新的 company canonical serving snapshot/generation，并通过 registry/default pointer 推进，而不是修改旧 job snapshot 的历史边界。
5. 旧 job 是否 repoint 到新的 company canonical serving view 必须是明确的产品决策：历史回放保持旧 view，用最新资产服务则写入新的 `job_result_view` 并记录 repair reason。

### Current Asset Flow And Consumption Contract

当前实现不是“每个新 snapshot 自动被一个全局 canonical view 吞掉”。资产流转边界如下：

1. Provider / import / manual inputs 先落到某个 immutable snapshot 的原始和规范化输入层，例如 `search_seed_discovery/`、`harvest_profiles/`、`candidate_documents.json`。
2. `materialize_company_candidate_view(...)` 从当前 snapshot 以及显式选择的 source snapshots 构建一个 materialized view，并在 `source_snapshot_selection` 中记录选择范围。
3. `build_company_candidate_artifacts(...)` 将 materialized view 投影成 `normalized_artifacts/<asset_view>/manifest.json`、`pages/*.json`、`candidates/*.json` 等前端 serving artifacts。
4. Organization asset registry / default pointer 决定“公司级默认资产”当前指向哪个 snapshot/generation；它不会自动把所有历史 snapshot union 成一个 evergreen index。
5. `job_result_view` 决定某次 history/job 的前端看板消费哪个 snapshot/generation。新 workflow 完成时应自动指向本次 workflow current snapshot；旧 job repoint 必须带 policy 和 reason。
6. 前端候选人看板优先消费 `job_result_view` 指向的 `manifest/pages`；召回排序/方向 filter 优先读 `source_matches/matched_keywords`，旧 artifact 缺这些字段时才做文本 fallback。

这意味着：

- “一个人属于多个 scoped query/shard”应在同一个 candidate record 的 `source_matches` 中表达。
- “公司级更全看板”应由显式发布的 company canonical serving snapshot/generation 表达。
- “历史 job 回放”默认不应被新 company canonical view 静默改变，除非产品策略明确选择 `serve_latest_company_asset`。

运维入口：

- `python -m sourcing_agent.cli audit-company-serving-view --company <company> [--snapshot-id <snapshot>] [--job-id <job>]`
  - 只读审计 registry、manifest/pages、`source_matches/matched_keywords` 采样、projection version、build profile、job result view drift。
- `python -m sourcing_agent.cli audit-hot-cache-serving-artifacts [--company <company>] [--snapshot-id <snapshot>] [--limit <n>]`
  - 只读审计 local hot-cache serving artifacts。
  - 报告 manifest 指向但本地缺失的 shard/page/backlog/auxiliary 文件、orphan JSON 文件、canonical serving artifact 是否可作为 rehydrate 来源。
  - 输出 cleanup/rehydrate plan，但不在 public read 中修复；缺失文件通过显式 `rebuild-company-serving-view` 或 generation hydrate/restore 路径处理。
- `python -m sourcing_agent.cli cleanup-hot-cache-serving-artifacts [--company <company>] [--snapshot-id <snapshot>] [--apply]`
  - 默认 dry-run；只有显式 `--apply` 才删除 hot-cache orphan files、兼容 monolith exports 或执行 TTL/size/generation retention。
  - 可配 `--ttl-seconds`、`--size-budget-bytes`、`--max-bytes-per-company`、`--keep-latest-snapshots-per-company`、`--max-generations-per-scope`。
  - 这是 operator/maintenance path，不允许 normal public read 借它做 request-time repair。
- `publish-candidate-generation` / `hydrate-candidate-generation` / generation-first `import-cloud-assets`
  - publish/hydrate/import 后默认运行一次 hot-cache governance cycle，并把 `hot_cache_governance` 写入返回 payload。
  - 如需调试或避免维护动作，可传 `--skip-hot-cache-governance`；默认行为用于防止 hosted/local runtime 在频繁 publish/hydrate 后无限积累 cache 残留。
  - Hot-cache access marker 记录 `access_count`、`first_access_at`、`last_accessed_at`、`heat_score`，runtime summary 的 `hot_cache.hottest_snapshots` 用于判断真实热数据与可清理冷数据。
- `python -m sourcing_agent.cli audit-job-result-view-consistency [--job-id <job>] [--company <company>] [--apply]`
  - 默认只读对比 job summary candidate source、`job_result_view`、authoritative organization registry。
  - `--apply` 只自动 repoint 有 full-local/full-asset reuse proof 的 jobs，例如 `reuse_snapshot_only`、`full_local_asset_reuse`、或 legacy full-company asset-population shape。
  - `delta_from_snapshot`、job-scoped overlay、带关键词但缺 full-reuse proof 的 jobs 只输出 `manual_review_required`，不能批量改成 latest full-company authoritative snapshot。
- `python -m sourcing_agent.cli rebuild-company-serving-view --company <company> --snapshot-id <snapshot> --build-profile foreground_fast`
  - 显式重建 serving projection；如需限制合并范围，用 repeatable `--preferred-source-snapshot-id`，不要用 completed-job reconcile 作为手动 artifact repair 入口。
- `python -m sourcing_agent.cli repoint-job-result-view --job-id <job> --policy historical_replay`
  - 默认 historical replay 是 no-op。
- `python -m sourcing_agent.cli repoint-job-result-view --job-id <job> --company <company> --snapshot-id <snapshot> --policy serve_latest_company_asset --apply --reason <reason>`
  - 只有显式 `serve_latest_company_asset --apply` 才更新旧 job 指针，并把 policy/reason 写入 metadata。

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
- `src/sourcing_agent/asset_governance.py` 现在提供第一版可执行 contract：
  - default pointer key 固定为 `(company_key, scope_kind, scope_key, asset_kind)`
  - canonical replacement plan 会显式生成新 default pointer 与旧 superseded pointer
  - historical retention 会保留新旧 snapshot id，避免覆盖式修补
  - `draft/partial/superseded/archived/empty` 不允许成为 default canonical pointer
- 2026-04-25 起，default pointer 已有持久化 writer：
  - control-plane table: `asset_default_pointers`
  - history table: `asset_default_pointer_history`
  - API: `POST /api/assets/governance/promote-default`
  - CLI: `promote-asset-default-pointer`

## Next Engineering Step

当前规则已经可以执行，但还需要继续产品化为代码约束：

1. 给 bundle metadata 增加 `status / scope_kind / scope_key / supersedes_snapshot_id`
2. 在 object sync 时生成 cloud-side registry index
3. 将 promotion-time `coverage_proof` stamping 自动化，避免调用方手写 proof
4. 增加 partial/superseded/archive 生命周期操作
5. 把 team-scoped asset 从“query 约定”升级为显式目录与元数据模型
