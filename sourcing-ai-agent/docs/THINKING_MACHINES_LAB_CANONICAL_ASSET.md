# Thinking Machines Lab Canonical Asset

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


## Canonical Local Asset

后续本地分析统一使用下面这份资产，不再直接面向 `runtime/company_assets/thinkingmachineslab/` 下的历史实验 snapshot：

- company: `thinkingmachineslab`
- canonical snapshot id: `20260407T181912`
- canonical snapshot dir:
  - `runtime/company_assets/thinkingmachineslab/20260407T181912/`
- canonical analysis payload:
  - `runtime/company_assets/thinkingmachineslab/20260407T181912/normalized_artifacts/materialized_candidate_documents.json`
- strict roster analysis payload:
  - `runtime/company_assets/thinkingmachineslab/20260407T181912/normalized_artifacts/strict_roster_only/materialized_candidate_documents.json`

## Why This Snapshot

- `latest_snapshot.json` 已指向这个 snapshot
- 这份 snapshot 的 `normalized_artifacts` 已重新构建
- `materialized_candidate_documents.json` 已折叠历史有效来源，适合作为单一分析入口
- retrieval 现在默认优先读取这个 snapshot，而不是直接扫全量 SQLite 历史池

## Asset Views

重建后当前 canonical asset 已拆成两种分析视图：

- `canonical_merged`
  - `189` candidates
  - `404` evidence
  - categories:
    - `132 employee`
    - `38 former_employee`
    - `8 investor`
    - `4 lead`
    - `7 non_member`
  - manual review backlog: `4`
  - profile completion backlog: `3`
- `strict_roster_only`
  - `148` candidates
  - `321` evidence
  - categories:
    - `104 employee`
    - `36 former_employee`
    - `8 investor`
  - manual review backlog: `0`
  - profile completion backlog: `3`

`strict_roster_only` 是一个故意偏窄的成员视图：

- 保留 manual-confirmed 成员
- 保留带明确职能信号的 roster / profile captures
- 排除 `lead` / `non_member`
- 排除仅因 company-people / 相似组织噪声混入、但没有明确 functional facet 的成员

## Functional Facets

当前 `strict_roster_only` 中的主要 role bucket / facet 分布：

- role buckets:
  - `research: 38`
  - `founding: 27`
  - `engineering: 26`
  - `leadership: 21`
  - `ops: 12`
  - `investor: 10`
  - `infra_systems: 9`
- facets:
  - `engineering: 110`
  - `research: 69`
  - `leadership: 43`
  - `founding: 31`
  - `ops: 28`
  - `infra_systems: 23`
  - `recruiting: 6`
  - `training: 4`
  - `safety: 3`
  - `multimodal: 2`

## Cloud Copy

已上传的云端 bundle：

- bundle kind: `company_snapshot`
- bundle id: `company_snapshot_thinkingmachineslab_20260407t181912_20260407T192328Z`
- remote prefix:
  - `bundles/company_snapshot/company_snapshot_thinkingmachineslab_20260407t181912_20260407T192328Z`
- local export dir:
  - `runtime/asset_exports/company_snapshot_thinkingmachineslab_20260407t181912_20260407T192328Z/`
- local upload summary:
  - `runtime/asset_exports/company_snapshot_thinkingmachineslab_20260407t181912_20260407T192328Z/upload_summary.json`

## Operational Rule

- 正常检索、默认 workflow、历史折叠分析：使用 `latest_snapshot.json -> 20260407T181912` 的 `canonical_merged`
- 做纯净 roster analysis、功能切片、组织画像时：优先使用 `strict_roster_only`
- 历史 snapshot 仅用于调试、回溯或比较实验差异
- 如果后续出现新的更高质量全量资产，应显式更新 `latest_snapshot.json`，并重新导出新的 `company_snapshot` bundle

## Request / CLI Usage

分析任务现在可以显式选择 asset view：

- request JSON:
  - `"asset_view": "canonical_merged"`
  - `"asset_view": "strict_roster_only"`
- CLI override:
  - `PYTHONPATH=src python3 -m sourcing_agent.cli run-job --file request.json --asset-view strict_roster_only`
  - `PYTHONPATH=src python3 -m sourcing_agent.cli plan --file workflow.json --asset-view strict_roster_only`
  - `PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file workflow.json --asset-view strict_roster_only`
- facet hard filter:
  - request JSON: `"must_have_facet": "infra_systems"` or `"must_have_facets": ["infra_systems"]`
  - CLI override: `PYTHONPATH=src python3 -m sourcing_agent.cli run-job --file request.json --asset-view strict_roster_only --must-have-facet infra_systems`
- primary role bucket hard filter:
  - request JSON: `"must_have_primary_role_bucket": "infra_systems"` or `"must_have_primary_role_buckets": ["infra_systems"]`
  - CLI override: `PYTHONPATH=src python3 -m sourcing_agent.cli run-job --file request.json --asset-view strict_roster_only --must-have-facet infra_systems --must-have-primary-role-bucket infra_systems`

实际效果：

- retrieval summary 会回显 `candidate_source.asset_view`
- match payload 会回显 `role_bucket` 和 `functional_facets`
- request family / confidence baseline 现在会区分不同 asset view，避免 `canonical_merged` 和 `strict_roster_only` 的反馈、基线选择相互污染
- `must_have_facet` 使用更保守的 facet 口径，不再直接由 notes 文本生成 facet，减少 recruiting / ops 这类噪声误命中
- `must_have_primary_role_bucket` 适合做真正的“主职能切片”，例如把 `infra_systems` 查询里的 founding / leadership 候选进一步压掉，只保留 primary bucket 为 `infra_systems` 的成员
