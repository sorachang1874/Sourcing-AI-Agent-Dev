# Canonical Cloud Bundle Catalog

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


> Current default: hosted restore should prefer `control_plane_snapshot + company_snapshot`. `sqlite_snapshot` entries below are legacy backup aliases kept for portability/reference.

## Goal

这份文档定义当前推荐恢复到服务器的 canonical runtime bundle 集合。

目标有两点：

- 让 hosted/server 部署优先复用现有数据资产，而不是开机后重新抓取
- 避免把重复、过时或测试态 snapshot 当成生产基线

当前推荐的恢复基线不是 `company_handoff`，而是：

1. 一份全局 `control_plane_snapshot`
2. 每个 canonical company 一份 `company_snapshot`

这样做更轻、更清晰，也更适合后续按公司增量更新。

## Canonical Bundle Set

### Global registry / state

| Kind | Bundle ID | Notes |
| --- | --- | --- |
| `control_plane_snapshot` | `control_plane_snapshot_*` | 当前推荐的全局 control-plane 恢复基线。旧的 `sqlite_snapshot_*` 仅作 legacy backup alias。 |

### Company snapshots

| Company | Snapshot ID | Bundle ID | Notes |
| --- | --- | --- | --- |
| `anthropic` | `20260411T122319` | `company_snapshot_anthropic_20260411t122319_20260411T121040Z` | 当前 Anthropic canonical snapshot。 |
| `google` | `20260412T022230` | `company_snapshot_google_20260412t022230_20260412T070953Z` | 已修正内部 `latest_snapshot.json` 指向 clean Google snapshot。 |
| `humansand` | `20260411T132527` | `company_snapshot_humansand_20260411t132527_20260411T121114Z` | `Humans&` 的 canonical company key。 |
| `langchain` | `20260410T045101` | `company_snapshot_langchain_20260410t045101_20260410T175508Z` | LangChain canonical snapshot。 |
| `openai` | `20260413T140350` | `company_snapshot_openai_20260413t140350_20260413T061157Z` | OpenAI Reasoning hosted rerun 后的 canonical snapshot；已包含 current + former scoped-search 合并后的 130 条 Stage 1 候选。 |
| `reflectionai` | `20260411T195205` | `company_snapshot_reflectionai_20260411t195205_20260411T121115Z` | Reflection AI canonical snapshot。 |
| `thinkingmachineslab` | `20260407T181912` | `company_snapshot_thinkingmachineslab_20260407t181912_20260410T175539Z` | Thinking Machines Lab canonical snapshot。 |
| `xai` | `20260411T114556` | `company_snapshot_xai_20260411t114556_20260411T121115Z` | xAI canonical snapshot。 |

## Canonicalization Notes

- `Humans&` 不再单独上传一份 `humans` alias bundle。
  - canonical company key 使用 `humansand`
  - 这样可以避免同一组织在云端出现两套重复 snapshot
- Google 旧 bundle `company_snapshot_google_20260412t022230_20260412T062847Z` 已被新的 `...070953Z` 取代。
  - 原因不是 snapshot 内容换了，而是旧 bundle 内部携带的 `latest_snapshot.json` 仍指向旧 Google snapshot
  - 新 bundle 已修正这个恢复入口

## Recommended Restore Order

### 1. Restore control plane first

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --bundle-kind control_plane_snapshot \
  --bundle-id <control_plane_snapshot_bundle_id> \
  --output-dir runtime/asset_imports
```

### 2. Restore company snapshots

按需恢复某个公司，或把上表里的 canonical snapshot 全部恢复。

示例：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --bundle-kind company_snapshot \
  --bundle-id company_snapshot_google_20260412t022230_20260412T070953Z \
  --output-dir runtime/asset_imports
```

`company_snapshot` 导入后会自动执行：

- candidate artifact repair
- organization asset registry warmup
- linkedin profile registry backfill

如果 bundle 已经下载到本地，也可以直接用 manifest：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --manifest runtime/asset_imports/company_snapshot_google_20260412t022230_20260412T070953Z/bundle_manifest.json
```

### 3. Verify before serving

至少检查：

- `runtime/company_assets/<company>/latest_snapshot.json`
- `runtime/company_assets/<company>/<snapshot_id>/candidate_documents.json`
- `runtime/company_assets/<company>/<snapshot_id>/asset_registry.json`
- `runtime/object_sync/bundle_index.json`

### 4. Only then start hosted runtime

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

## Consumption Rules

服务器恢复后，默认应优先消费这些已恢复资产，而不是立刻 fresh run：

- 默认走 hosted 路径
- 默认复用 local runtime + Postgres control-plane registry / restored authoritative generations
- 只有在用户明确要求 fresh run，或现有 snapshot 明显不满足目标范围时，才触发新的外部 acquisition

对前端与 API 调用方也一样：

- 先读 API 返回的 `request_preview` / `workflow_stage_summaries`
- 不要直接把 `runtime/company_assets/*` 作为前端主数据源
- 不要把“启动服务”误解为“重新抓取所有公司资产”

## Remote Indexes

云端恢复和审计主要依赖：

- `indexes/bundle_index.json`
- `indexes/sync_runs/*.json`

本地恢复后可对照：

- `runtime/object_sync/bundle_index.json`
- `runtime/object_sync/runs/*.json`

## Cleanup Rule

当同一 canonical snapshot 重新导出更干净的 bundle 时：

1. 先上传新 bundle
2. 再执行 `delete-asset-bundle` 删除旧 bundle
3. 确保本地/远端 `bundle_index.json` 都只保留新的 canonical entry

不要长期保留“只差一个 pointer 但内容看起来相似”的重复 bundle，这会增加服务器恢复时选错资产的概率。
