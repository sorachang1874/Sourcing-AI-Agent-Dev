# scripts/ — committed command surface

> Status: Current registry (canonical home of the live-ops script registry since 2026-07-22;
> moved from `docs/HARVESTAPI_PLAYBOOK.md`, which keeps provider knowledge and points here).

规则：live 运维动作凡两次以上出现就必须落成 committed 脚本进本目录并在本文件登记；
/tmp 脚本视为事故温床。新增脚本时在对应家族表加一行。

## Live-ops 家族（付费/外部相关；全部 fail-closed，live 需三重门 env）

| 脚本 | 用途 |
|---|---|
| `live_apify_dataset_salvage.py` | 把已付费的孤儿 Apify dataset（job 取消未物化）采纳为 candidate documents + 合并快照；复用 candidate_materialization ingest/consolidate 与 connector 匹配/缓存键 |
| `live_promote_company_snapshot.py` | 单快照定向晋升（registry 守卫翻转 + 指针同步）或 `--provenance-only` 注册；默认 dry-run 打印守卫预测。存在原因：`rebuild-company-serving-view` 是跨全部历史快照的合并清扫（2026-07-22 google 75 分钟未翻转且混入 simulate 源），晋升本身只需两个 O(1) 操作 |
| `live_profile_fetch_slot_fill.py` | 并发 slot-fill profile fetch（4–8 批；只抓 candidate_documents 减缓存命中的缺失 url；connector 二次缓存防重；断言 live 双闸否则 fail closed） |
| `live_candidate_profile_enrich.py` | 缓存 profile 的 headline/location/experience/education/languages 回填 candidate documents（幂等、先备份） |
| `live_layering_run.py` | 对 salvage 快照跑华人分层（`allow_candidate_documents_source=True`；DeepSeek 复核走 MODEL_PROVIDER_* env） |
| `live_former_lane_run.py` | former lane per-function 分片查询（library 路径绕开 daemon；复用产品 planner 的 per-function shard plan；payload 先验证：`pastCompanies` 单 URL + 单 functionIds、无 keywords） |
| `live_scoped_lane_run.py` | 关键词 scoped 名册（per-status × per-function 单元查询，`searchQuery` 保留；存在原因＝产品两路径合同缺口：full_company_roster 静默丢关键词、scoped_search_roster 合并 functionIds） |
| `live_xfirst_seed_build.py` | Layer 1-3 → X-First seed_inputs（profile envelope 直取完整 profile facts，affiliation facts 带时间性，evidence_ref 绑定快照） |
| `live_grok_collection_run.py` | Grok 采集驱动（CWD 固定 `~/.grok`；`--limit` 先 smoke 再全量；默认 48 workers） |
| `live_luna_judge_run.py` | DeepSeek judge 驱动（committed transport + binding；`extra_source_context` 注入 raw LinkedIn profile 全文 + 字段字典 + 引用白名单；v1 citation/pins/reducer 不动） |
| `live_xfirst_export_csv.py` | 13 列导出（CRM 8 + X-First 5）；待采集/无X账号语义、seed_fact 引用渲染、judge 回填与账号冲突标注；确定性可复现 |

现场纪律（盘点→delta→salvage 优先→abort 走 `/v2/actor-runs/{id}/abort`→进程 vintage 核对）
的完整版在 [../docs/HARVESTAPI_PLAYBOOK.md](../docs/HARVESTAPI_PLAYBOOK.md)。

## 其它家族（一览；用途见各脚本 docstring）

- `dev_*.sh` / `docker_*.sh` — 本地后端/前端/PG/daemon 生命周期（入口 `dev_backend.sh`，停 `dev_stop.sh`，诊断 `dev_doctor.sh`）。
- `audit_*.py` / `apply_*.py` — 资产治理审计与（dry-run 优先的）修复应用。
- `check_*.py` — 守卫检查（`check_canonical_completeness.py` = canonical 完备性门）。
- `backfill_*.py` / `build_*.py` — 一次性回填与 manifest/bundle 构建（保留为可重放证据）。
- `bootstrap_*.sh` — 环境引导（测试环境、reviewer CODEX_HOME）。
- `agent_network_preflight.sh` — 只读网络诊断（不得改 proxy/VPN 状态）。

## WS7 影子路径证据家族（零付费；live schema 只读）

| 脚本 | 用途 |
|---|---|
| `ws7_shadow_divergence_report.py` | 议案①（batch divider）+ 议案③（promote judge）影子路径的**接合证据**与**规则梯 vs scripted 分歧语料**。三个子命令：`engagement`（跑真实生产接缝——`queue_background_profile_prefetch` / `build_company_candidate_artifacts`——在自建并自动销毁的 `sourcing_test_ws7_*` schema 里产出真实 shadow record）、`divider`（只读回放 live registry 的 wave-scoped ready set + 磁盘快照，跑梯子与 scripted 分割，出 V1–V10 电池结果与 Jaccard 重组度）、`promote`（只读回放 `organization_asset_registry` 的 realistic 36 / extended 806 对 + 5 个事故场景）。**fail-closed**：三重门 env 任一存在即拒跑；只读连接强制 `default_transaction_read_only = on`（由 PG 而非纪律保证）；只用 scripted 确定性客户端，绝不走 `build_model_client`。**诚实性红线：scripted 客户端只能证明"路径通了"和"保留电池的行为"，证明不了任何 AI 判断质量；真模型语料是 operator-gated 的独立步骤（S6）。** |
