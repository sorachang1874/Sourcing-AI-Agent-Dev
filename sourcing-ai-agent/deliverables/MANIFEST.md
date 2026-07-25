# Deliverables Manifest — Layer 1-3 CRM+X-First CSVs

> Status: Current deliverables manifest (governed tier since 2026-07-22, reorg R4).
> CSV payloads live in this directory but are NOT tracked in git（人才个人数据不入
> 版本库—operator decision D3）；本 manifest（校验和+谱系+重生成命令）是 tracked 契约。
> 更新规则：换版本 = 新文件名 + 新行 + 旧行移入 History；绝不原地覆盖同名文件。

## Current deliverables (2026-07-22)

| 文件 | sha256 | 记录数 | X已确认 | pretrain有 | 版本说明 |
|---|---|---|---|---|---|
| gdm_layer123_export.csv | `d7eabca9…ea1dc76f` | 1491 | 668 | 363 | 2026-07-21 raw-profile re-judge 轮 |
| tml_layer123_xfirst_export_v2.csv | `af1d441e…51799641` | 67 | 54 | 42 | v2 = raw-profile 语义（v1 pretrain 16→42） |
| openai_layer123_export_v3.csv | `01521c58…294a083e` | 950 | 372 | 154 | v3 = 冲突清单修正，恢复 14 个被自撞假阳性错抹的 X 账号（v2 372-14=358） |
| anthropic_layer123_export.csv | `0dffdb7c…f5a5e642` | 344 | 161 | 51 | 2026-07-20/21 全 roster 轮 |
| xai_layer123_export.csv | `de203491…01bca9fb` | 454 | 277 | 136 | 同上 |
| meta_tbd_layer123_export.csv | `2d657d93…7668338290`* | 33 | 26 | 10 | Meta-TBD scoped 轮 |

*完整 sha256 见下方 checksums 块。列契约（13 列 = CRM 8 + X-First 5）与语义定义：
[`scripts/live_xfirst_export_csv.py`](../scripts/live_xfirst_export_csv.py) docstring +
[HARVESTAPI_PLAYBOOK](../docs/HARVESTAPI_PLAYBOOK.md) 的 CSV 语义节。

```
d7eabca9971cc3f8f140d061145294d512d9d6008a9a8c37e718a4eaea1dc76f  gdm_layer123_export.csv
af1d441e83709d3ff407968da0051d9cbdbec5eafbb8b21be0a4b75451799641  tml_layer123_xfirst_export_v2.csv
01521c581861051549b067e1d2aa684e4ea23c430932d3f73ead2c41294a083e  openai_layer123_export_v3.csv
0dffdb7c66dee4f979c9f08b79a458a6099e4633c7229123ee90e190f5a5e642  anthropic_layer123_export.csv
de203491abd13f57cb28a648a23a581444c3d7296907e4e504d665df01bca9fb  xai_layer123_export.csv
2d657d935da8053047a0e2f072ef5e27fa39bf121df51b7417191c7668338290  meta_tbd_layer123_export.csv
```

校验：`cd deliverables && shasum -a 256 -c <(grep -E '^[0-9a-f]{64}' MANIFEST.md)`

## Lineage / 重生成（全部确定性；输入在 runtime/，不入 git）

以 OpenAI v3 为例（其余 lab 同构，替换 snapshot/collection/batch 路径）：

```
PYTHONPATH=src .venv/bin/python scripts/live_xfirst_export_csv.py \
  --snapshot-dir runtime/company_assets/openai/20260720T104157 \
  --layered-analysis runtime/company_assets/openai/20260720T104157/layered_segmentation/greater_china_outreach_20260720T051843Z/layered_analysis.json \
  --grok-collection runtime/test_env_live/openai_grok_collection_v2.json \
  --luna-batch runtime/test_env_live/openai_luna_deepseek_v2/luna_batch_final.json \
  --account-conflicts runtime/test_env_live/openai_luna_deepseek_v2/account_resolution_conflicts_v3.json \
  --seeds runtime/test_env_live/openai_seed_inputs_v2.json \
  --seed-ref-prefix openai-l123 --out deliverables/openai_layer123_export_v3.csv
```

判定链谱系：seeds（live_xfirst_seed_build）→ grok collection（live_grok_collection_run）
→ DeepSeek judge（live_luna_judge_run，raw-profile supporting_context，v1 引用契约）→
冲突清单（openai 用 v3 = 仅 karen_li6/kevin_wang3290 两组真冲突；证据
`account_resolution_conflicts_v3.provenance.json`）→ 本导出。身份合并映射：
[`configs/identity/openai_identity_alias_map_v1.json`](../configs/identity/openai_identity_alias_map_v1.json)。

## History

| 文件 | 状态 | 备注 |
|---|---|---|
| openai_layer123_export.csv / _v2.csv | superseded（留 runtime/test_env_live/） | v1=名称哈希占位 id 时代；v2=raw-profile 语义但 17 行冲突抹除（14 行误伤） |
| tml_layer123_xfirst_export.csv | superseded（同上） | v1 语义（pretrain 16） |
