# Handoff — Sourcing AI Agent（给下一位接管者）

> Status: Archived (2026-07-22). Kimi→Claude takeover handoff of 2026-07-21; superseded by workspace PROGRESS.md/NEXT_TODO.md.

版本：2026-07-21（由 Kimi Code CLI session `005f48d5` 整理，接手自 session `194659cd` 之后）
读者：下一位接管本项目的 Coding Agent（以及需要审计背景的人）。

---

## 1. 这个项目是什么

为内部团队（人才 sourcing）批量召回目标 AI Lab 的研究/工程成员并做分层验证的产品系统：

- **sourcing-ai-agent/**（主产品）：Harvest(Apify) 采集 LinkedIn 名册/profile → 华人线索分层（确定性规则 + AI 复核）→ 导出。
- **x-first-researcher-sourcing/**（X-First）：对 Layer 1-3 的人做 X 账号定位（Grok CLI）+ pre-train 方向经历判断（LLM judge，v1 合同）。
- **ai-assisted-engineering-playbook/**（通用工程方法 playbook，与具体项目无关，GitHub main 是唯一集成线）。
- 单一事实源：`.coord/BOARD.md`（lane cards、directives、defect log）+ `.coord/handoffs/`。

## 2. 当前交付状态（6 个 Lab 全部收口）

| Lab | CSV（runtime/test_env_live/） | L1-3 行数 | Pre-train 有(current+historical) |
|---|---|---|---|
| TML | `tml_layer123_xfirst_export_v2.csv` | 67 | 42 |
| OpenAI | `openai_layer123_export_v2.csv` | 950 | 150 |
| GDM | `gdm_layer123_export.csv` | 1491 | 363 |
| xAI | `xai_layer123_export.csv` | 454 | 137 |
| Anthropic | `anthropic_layer123_export.csv` | 344 | 51 |
| Meta TBD（scoped keyword） | `meta_tbd_layer123_export.csv` | 33 | 10 |

每个 Lab 都有完整链路：名册（current+former 去重）→ 全量 profile → 富化 → 华人分层 → seeds → Grok（X 账号）→ DeepSeek judge（带 raw profile）→ 13 列 CSV（CRM 8 列 + X-First 5 列）。

## 3. 运行时拓扑与凭证

- **后端**：`make test-env-backend-live LIVE_CONFIRM=1`（端口 8777，进程内 watchdog + 外部 worker-recovery daemon）。**PG 是权威控制面**：`postgresql://sourcing@127.0.0.1:55432/sourcing_agent`，schema `sourcing_live_tml_path_20260719`（`jobs/*.json` 是陈旧镜像，不要当真）。启动时必须 export 该 schema 环境变量（缺省会被解析成 `sourcing_test`，job 全部"not found"）。
- **live 契约**（付费驱动都需自检）：`SOURCING_EXTERNAL_PROVIDER_MODE=live` + `SOURCING_LIVE_PROVIDER_CONFIRM=1` + `SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS=1` + `APIFY_API_TOKEN`（当前有效 token 写在 `runtime/secrets/providers.local.json` 的 harvest 三项里）。
- **模型**：plan/review/judge 用 DeepSeek（`MODEL_PROVIDER_BASE_URL=https://api.deepseek.com/v1`、`MODEL_PROVIDER_MODEL=deepseek-v4-flash`、`MODEL_PROVIDER_API_STYLE=openai_chat_completions`、`MODEL_PROVIDER_MIN_MAX_TOKENS=6000`）；x-first judge transport 已 committed（`x_first/deepseek_luna_transport.py`）。chshapi 中继仍冻结（见 §6 挂起项）。
- **Grok CLI**：`~/.grok/bin/grok`，调用时 cwd 必须固定 `~/.grok`（否则 "Device not configured"）。

## 4. 接管者的主要工具：committed live-ops 脚本族（sourcing-ai-agent/scripts/）

全部带 dry-run/自检，**禁止再造 /tmp 脚本**（登记处在 `docs/HARVESTAPI_PLAYBOOK.md`）：

- `live_apify_dataset_salvage.py` — 已付费孤儿 dataset → candidate documents + 合并快照（current/former dataset 都支持，含 profile-search 形状）。
- `live_profile_fetch_slot_fill.py` — 4-8 批并发补缺失 profile（自带去重 + live 双闸断言 + 相对路径 resolve）。
- `live_former_lane_run.py` / `live_scoped_lane_run.py` — former per-function 两片 / scoped keyword per-status×per-function 查询（library 路径，绕开 daemon 依赖）。
- `live_candidate_profile_enrich.py` — profile envelope → doc 的 experience/education/headline/location 回填（分层 L2/L3 的输入）。
- `live_layering_run.py` — 华人分层（salvage 快照需 `allow_candidate_documents_source=True`，此脚本已处理）。
- `live_xfirst_seed_build.py` — L1-3 → seed_inputs（完整 profile facts：全部工作经历含描述/About 全文/教育/projects/patents…；含 url 双形态索引）。
- `live_grok_collection_run.py` — Grok 采集（cwd 固定 ~/.grok，--limit 先 smoke，workers 可调）。
- `live_luna_judge_run.py` — DeepSeek judge（supporting_context 注入 raw profile 全文 + 字段字典 + 引用白名单说明）。
- `live_xfirst_export_csv.py` — 13 列导出（区分 待采集/无X账号/无X账号·据profile，seed_fact 引用渲染进证据列）。

## 5. 不可违反的合同（都是付费事故换来的）

1. **付费派发前盘点**：查本地快照 + **远端 Apify run 历史**（"本地取消 ≠ 没花钱"），算 delta 只发 delta；salvage 优先，重发必须有语义变化理由并记录。
2. **per-function 分片**：engineer `"8"` / researcher `"24"` 永远分开（当前 lane 和 former lane 都是），绝不合并 `["8","24"]`，绝不用推断的 function（`"19"` 事故）；但**关键词聚焦的小名册不要加 function 过滤**（会丢召回，Meta TBD 实测 36→67）。
3. **广召回默认无 keywords**；scoped 关键词任务走 `live_scoped_lane_run.py`（产品 `scoped_search_roster` 会合并 functionIds——合同缺口待修，别用它）。
4. **去重先于 fetch**；dedupe key 用产品的 `candidate_dedupe_keys`/`roster_merge_dedupe_key`。
5. **停止要在远端停**：`POST /v2/actor-runs/{id}/abort` + 本地 job cancel；cancel 不清 lease/worker。
6. **进程 vintage**：改动请求形态的代码合入后，先重启后端再发 job（`ps -o lstart` vs code mtime）——xAI former 曾按旧代码形态跑了一整轮。
7. **指令前提冲突就停下问**：观察到的状态与指令前提矛盾时，绝不静默扩大付费范围（GDM 事故的 RC2）。
8. **judge 输入** = seed facts（引用锚点）+ grok bundle + **raw LinkedIn profile 全文 + 字段字典**（supporting_context，非引用源）；引用白名单 `seed_fact:<ref>` / `x_bio` / `post:<id>`，非法引用整条作废。
9. **CSV 语义**：X账号已确认 列管账号状态，Pre-train 列管判断状态；无账号但 profile 有证据标"（无X账号·据profile）"。

## 6. 踩过的坑（根因与落点）

- **GDM 误重启全量 job**（194659cd session）：只看了本地物化就断定"没抓到"，实际远端已付费成功。教训 RC1-4 记在 `.coord/handoffs/gdm-restart-incident-and-former-lane-handoff-v1.md`（含 binding brief）。
- **zombie workflow_commands**：cancel 不清命令表；2 条 running 付费命令被双闸锁住（already_owned + provider-delta guard），**严禁对它们 retry/resume**（BOARD 有记录）。
- **entitydelta 崩溃杀 daemon**：不可变身份误含 `attempt_id`，重试必崩。已修（幂等重放 + 真冲突仍 fail-closed）+ 回归测试，已部署。
- **blocked job 永不恢复**：daemon 跳过无事件主的 remote-wait worker，事件主随进程重启死掉。已修（>900s 孤儿收数 ≤4/tick）+ E2E 测试，已部署。
- **timeout 公式余量不足**（15/20s 每页，四月设计）：xAI f8 截断 500/675。现为 **40s/页、5400 上限**。
- **dataset 下载静默截断**：短页被当作结束（settle 期瞬时短页）。已修完整性闸（itemCount 校验 + settle 重试 + 耗尽抛 retryable）+ 回归测试。
- **相对路径双重目录**：connector 内部会 re-anchor 相对路径——所有驱动入口必须 `.resolve()`。
- **重复 candidate_id 顶掉正确记录**（TML Songlin Yang：空名+former 行）——按完整度去重；多投影合并的快照要扫 dup id。
- **404 profile 个例**：用 Kimi WebBridge 开 `/details/experience/` 二级页人工补齐（`_supplement` 标记 envelope），只补当事人的 seeds/grok/judge。
- **UI 搜索 vs API 搜索边界**（Meta TBD：UI 152 vs API 67）：隐私设置 + UI 推荐位；锚点记录在 playbook + `meta/registries/meta_tbd_ui_observed_members_v1.json`（150 个可见成员、25 个已解析 URL，用于以后补漏）。
- **OpenAI 中间快照覆盖缺口**：canonical_merged 视图合并历史快照，candidate_documents 会漏（40 人 L1-3 未进 seeds）——**收口时必须跑跨快照完整性检查**（xAI/Anthropic/Meta 已脚本化）。
- **secrets 文件旧 token 失效**（401）：凭证轮换后要更新 secrets 文件并重启。
- **hosted smoke harness 腐化**（HTTP 410 类，HEAD 上就红）：与本次改动无关，待专门 lane 修/退役。

## 7. 挂起与后续（按优先级）

1. **chshapi 额度恢复后**：review 重发队列（S1e2b rerun4、FF-SCHEMA rerun5、fnID roster lane、FT1 rerun4、FT2 rerun7）+ former per-function 合同改造的独立 review。probe：chshapi `insufficient_user_quota` 消失即恢复。
2. **scoped_search_roster function 合并**：合同改造（per-function scoped shards），同类于 former broad 的修复路径。
3. **OpenAI 身份合并**：旧 950 行 CSV 的 39 个重复人 vs 补漏 40 人中 14 对已撞号——candidate_documents 需要一次 identity merge。
4. **尾部**：xAI 2 个顽固 grok 失败（1 embedded null byte）、GDM 1、各 lab ≤4 已标注；Meta TBD registry 里 125 个 headline-only 条目可按需用 WebBridge 补漏。
5. **债**：mypy 81 errors/4 files（HEAD 持平）；hosted smoke harness。

## 8. 关键文件地图

- `.coord/BOARD.md`（最新状态/lane cards/directives）、`.coord/handoffs/`（事故与 lane 交接）
- `docs/HARVESTAPI_PLAYBOOK.md`（actor 输入契约 + 现场纪律 + 脚本登记 + UI/API 边界 + WebBridge 补齐路径）
- x-first：`docs/LUNA_LIVE_BATCH_RUNNER_DESIGN.md`（judge 合同、DeepSeek binding、supporting_context 钩子）
- 数据：`runtime/test_env_live/company_assets/{google,openai,thinkingmachineslab,xai,anthropic,meta}/`
- seeds/collections/batches：`runtime/test_env_live/*seed_inputs*.json`、`*grok_collection*.json`、`*luna_deepseek*/`
- 测试：`PYTHONPATH=src .venv-tests/bin/python -m unittest <module>`（PG 测试需 `source .local-postgres.env`）；机器是 16GB Mac，用项目 `.venv`/`.venv-tests`，别用 Homebrew Python（签名问题）。

## 9. 并发基线（实测）

- Apify profile fetch：4-8 批并发，400-800 url/批；judge（DeepSeek）：64 workers ≈1.8 人/s（可试 96）；Grok CLI：48 workers（内存约束 ~200MB/进程）。
