# WS7.0 侦察批 — 四份实测画像（read-only，2026-07-22）

> Status: ACTIVE W7.0 recon artifact（强 Agent 改造侦察产出；W7.1 设计议案的事实基线与裁决问题清单）。

```
status: active          owner: operator
canonical-path: sourcing-ai-agent/docs/WS7_STRONG_AGENT_RECON_2026-07-22.md
ratified: 2026-07-22
last-verified: 2026-07-22
```

> 依据：`sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md` §6.5（operator 指令唯一权威转录）。
> 本文件所有 `file:line` 以 workspace 根为基准；`enrichment.py` 等未加前缀的指 `sourcing-ai-agent/src/sourcing_agent/`。
> 树：branch `governance-phase0-ttl-20260611`，工作区 clean（HEAD `5f2dd93`）。
> PG 实测走 `SOURCING_CONTROL_PLANE_POSTGRES_DSN`（.local-postgres.env 注释掉的 5432 老 DSN 已失效），schema `sourcing_live_tml_path_20260719`，只读 SELECT。

---

## 1. fetch-profile 状态机 / 调度 / batch 现状画像

### 1.1 合同与实现锚点

权威合同：`sourcing-ai-agent/docs/PROFILE_PREFETCH_SCHEDULER_CONTRACT.md`（R1–R8，2026-06-12 Phase 4 Step 0）。合同锚点基于 fd1debb，当前树行号有漂移（下表为**当前实测行号**）：

| 符号 | 当前锚点（enrichment.py） | 合同旧锚 |
|---|---|---|
| 尺寸常量带 | :465–503 | :464–503 |
| sizer `_recommended_harvest_profile_prefetch_actor_slot_batch_size` | :677–702 | :676–700 |
| roster split `_should_split_roster_ready_set_across_actor_slots` | :711–735 | :710 |
| tiny-coalescing `_coalesce_tiny_profile_dispatch_chunks` | :1007 | :1006 |
| durable wave 继承 `_apply_durable_refill_wave_dispatch_window` | :1162 | :1161 |
| plan 主入口（sizer 调用/roster split/coalesce） | :1395 / :1426 / :1496 | :1393–1500 |
| refill submit 命令 owner `run_linkedin_profile_refill_submit_command_once` | :3042 | :2995 |
| worker budget `_harvest_profile_prefetch_new_worker_budget` | :4082–4110 | — |
| 队列入口 `queue_background_profile_prefetch` | :5046 | :4975 |
| chunk 派发 `_dispatch_prefetch_chunk` | :5882 | :5810 |

### 1.2 当前 batch 划分规则（纯硬规则，`max()` 层叠常量）

常量（enrichment.py:465–503，全部 env 可覆写）：
- `ACTOR_SLOT_URL_TARGET=50` ≤ `DURABLE_UNIT_MAX_URLS=200` ≤ `PROVIDER_ENVELOPE_MAX_URLS=300`；`SCALE_THRESHOLD_URLS=400`；`MAX_BATCH_COUNT_FOR_LARGE_READY_SET=8`。
- coalescing 阈值：`MIN_NON_TAIL_BATCH_SIZE=10`（:465）、`TINY_BATCH_MAX_SIZE=5`（:469）、`LOW_VOLUME_COMPANY_MAX_URLS=20`（:473）。

sizer 四带（enrichment.py:677–702，R1.a–d）：
- count≤50 → batch=50 `actor_slot_item_packing`；
- 50<count≤300 → 单一信封 `single_durable_unit_ready_set`；
- 300<count 且 ceil(count/300)≤8 → 均衡 `max(50, min(300, ceil(count/n)))`（>400 时 reason=`large_ready_set_provider_envelope_target`）；
- 需要 >8 批 → batch=300 封顶 `large_ready_set_provider_envelope_cap`，余量 defer。

覆盖层：R2 roster split（:711–735 + :1426，50<count≤200 且 ≥2 空闲 slot 且 roster 占比 ≥0.5 → 强切 50/slot `roster_actor_slot_fill`）；R3 tiny-coalescing（:1007，≤5 的碎片累进合并，≥10 才刷出，final tail 并入末 chunk）；R4 sub-50 tail guard（<50 非 low-volume 非 final-tail → 全 defer `deferred_coalescing_sub_50_tail`，仅 queue-quiescent 或 retry isolation 放行）；R6 durable wave 继承（:1162，重排既有 wave 继承历史最大 batch_size，防重切）。审计枚举 `batch_size_reason`（R7 八值表）。**这就是 AI-Native 要替换的规则梯——但 R7 的 reason 审计合同与 R5/R6 的 invariant-1 语义是要保留的形状**。

### 1.3 Slot 补充时机（operator 要降的通信成本，实测机制）

**关键实测结论：provider completion 事件是 signal-only，不派发；真正的 submit 归 refill daemon 的 poll tick。**

- worker budget 计算：`_harvest_profile_prefetch_new_worker_budget`（enrichment.py:4082–4110）——`available_new_worker_count = max(0, min(submit_budget, actor_budget) - max(active, scheduler_reserved))`；actor_budget 来自 `resolved_harvest_profile_actor_global_inflight`（runtime_tuning.py:354–360，默认 4；fast_smoke 覆写为 2，runtime_tuning.py:30）。
- completion 事件链：`_handle_harvest_profile_completion_event`（profile_fetch_owner.py:2643）→ `_run_profile_completion_next_submit_opportunity`（orchestrator.py:71622，docstring 明言 "Completion callbacks are not profile-refill executors"）→ `_trigger_profile_prefetch_refill`（profile_fetch_owner.py:2098，`defer_provider_submit=True`，返回 `refill_daemon_signal_only=True`、`next_submit_owner="profile_refill_daemon"`、"max_sync_work: record provider-completion signal only; no registry scan/replan/claim/provider submit"）。
- 实际补 slot 的执行者：`_run_profile_prefetch_refill_queue_once`（profile_fetch_owner.py:2180 起，docstring "closes the slot-release gap where no webhook/event arrives"；orchestrator.py:71619 委托），由 recovery tick 的 profile_refill phase 回调（orchestrator.py:40691/:40789/:41209/:41963）驱动；daemon poll 周期默认 **5.0s**（service_daemon.py:832/:913 `poll_seconds=5.0`；worker_daemon.py:846/:855 同为 5.0s sleep）。
- **通信成本量化**：worker 完成 → slot 实际重新填充的最坏延迟 ≈ 1 个 daemon tick（≤5s）＋ tick 内 phase 序位＋每 tick phase budget（`recovery_tick_budget_exhausted`/`phase_budget_exhausted` 投影键，service_daemon.py:160–166）。plan 记录里已有空转审计字段：`unfilled_available_slot_count`、`refill_saturation` ∈ {no_ready_items, no_available_slots, worker_budget_saturated, underfilled_with_deferred_items, tail_coalescing_wait, ready_items_exhausted, filled_available_slots}（enrichment.py:1253–1277）——AI 改造的 before/after 度量可直接用这组字段。
- 佐证历史：commit `767f77a` "event-signaled recovery wakeup + daemon-guarantee invariant (Phase 4 Step 5a+5b)" ＋ `2000dfb`（NO-GO 后修正）——已有事件唤醒 daemon 的机制，但唤醒后仍走完整 tick，不是 completion 点上的即时 slot 填充。

### 1.4 URL 级失败记录 + 最后统一重试（operator 要求**保留**的形状）

- 状态字汇：registry item `refill_queue_state`——normal 态 `("deferred_budget","deferred_coalescing","dispatch_reserved","dispatch_claimed")`（enrichment.py:520–525）、retry 态 `("retry_wait",)`（:526）、provider-owned 态 `"planned_dispatch"`（:527）。durable 单元 = `linkedin_profile_registry` item（item_store 字段，:1281）。
- 失败记录：worker summary 的 `failed_urls` 在 submit-owner 执行路径提取（enrichment.py:3396–3416）并按 `worker_status in {"backpressure","failed"} → status="retry_wait"` 记 activity（:3447）；per-URL 终态由 `linkedin.profile_url.terminal_record` 命令族记录 fetched/failed 计数（:2930–2995，异常时 `phase="profile_url_terminal_record_retry_wait"` :2960）；dispatch 结果里的 failed_urls 回写 refill 队列（:6046–6083，`flush_reason="retry_isolation"`）。
- 统一重试门：`_profile_refill_retry_gate`（enrichment.py:3900–4020）——**"Retry is a separate wave. It may start only after the normal wave is closed"**：normal 态 open item=0 且 ready=0 且无 first-attempt provider-owned URL 未终态，才放行 retry；被 normal wave 挡下时 reason=`retry_wait_blocked_by_normal_profile_wave`（:5186）。放行后 plan_reason=`retry_wait_isolated_dispatch`（:1537），派发以 `retry_isolation` 作为合法 tiny 批理由（:1939–1940、:1959）。
- PG 实测（只读）：`linkedin_profile_registry` 当前 7513 行空态 / 2 行 `retry_wait` / 1 行 `planned_dispatch`——存在跨 wave 遗留的 retry_wait 尾巴（与 NEXT_TODO「profile_fetched 对账」遗留一致）。

### 1.5 尾部 batch 低效的量化证据

- 合同 §2 R3 标注 **[回归]** 的三件实测（2026-06-12 记录）：47 URL 尾集应作 ONE `queue_quiescent_final_tail` wave 实裂 25+22；4 URL 应 1 worker 实测 2；canonical 路径 4 URL 实测 4 个并行 tiny submit（PROFILE_PREFETCH_SCHEDULER_CONTRACT.md §2 R3、§4 表）。已由 commit `ef74475`（"storeless fail-closed + scheduler refill/tiny regressions"）修复——storeless fail-closed 分支现于 enrichment.py:3104–3117 可证。
- 修复后仍存的结构性尾损（规则本身的代价，正是 operator 指令点 2 的对象）：①R1.a 把 <50 的集合按 50 packing，配合 R4 会把 sub-50 尾巴压进 `deferred_coalescing` 等待 queue-quiescent——吞吐换时延；②R1.c 均衡切分的余数批（如 1600→267×6）与 8 批封顶后的 defer 余量要等下一轮 wave；③`tail_coalescing_wait`/`underfilled_with_deferred_items` 两个饱和态（:1268–1272）就是「slot 有空但尾巴被规则扣住」的显式记号。
- docs/RESIDUAL_LEDGER.md 无 prefetch/tail 专属行（grep 无命中）；历史量化只在合同 §2/§4 与 `9dc6958`（anti-recurrence tests）、`1d66882`（"Prove roster prefetch tail waves dispatch without idle"）、`2266254`（"Scale roster profile prefetch to 4-8 workers"）三个 commit。

---

## 2. materialize→promote-to-canonical 硬规则清单

### 2.1 决策路径

唯一决策函数：`evaluate_organization_asset_registry_promotion`（asset_reuse_planning.py:1217–1415）。调用面：
- 正常物化注册链：`asset_registration.py:279` → `upsert_organization_asset_registry_with_guard`（asset_reuse_planning.py:1518–1550：先 inherit_reusable_source_snapshot_coverage / enforce_reusable_source_snapshot_provenance / ensure_explicit_population_coverage，再 evaluate，`authoritative=decision.promote` 传给 store）；另有 `registry_refresh_mode=="force_upsert"` 逃生门（asset_registration.py:271–276）。
- 候选盘点：`select_organization_asset_registry_promotion_candidate`（asset_reuse_planning.py:1417–1451，按 sort_key 降序试第一个 promote=True 的候选）。
- 运维脚本：`scripts/live_promote_company_snapshot.py:143/:181`（dry-run 默认打印 guard 预测）。
- 存储层第二道闸（PG 写入点）：storage.py:8634–8696 generation/lineage guard（见 2.3）。

### 2.2 硬规则清单（全部阈值，file:line）

常量（asset_reuse_planning.py:149–152）：`MIN_RELATIVE_GAIN=0.05`、`MIN_ABSOLUTE_GAIN_SMALL=20`、`MIN_ABSOLUTE_GAIN_LARGE=100`（≥1000 人档触发 LARGE，:1290–1293）、`MAX_COMPLETENESS_SCORE_REGRESSION=1.0`。

evaluate 的判定梯（promote = 四支 OR，:1350–1355）：
1. **lifecycle 闸**：status ∈ {superseded, archived, …非 promotable 集}→拒（:1224，`organization_asset_lifecycle_promotable` :307–309）。
2. **无 incumbent** → 直接 promote `no_existing_authoritative`（:1232）。
3. **同 snapshot 刷新** → promote `same_snapshot_refresh`（:1240）。
4. **subsumption_higher**（:1280–1288，六条 AND）：candidate_count ≥ existing×0.98；profile_detail ≥ existing×0.98；evidence ≥ existing×0.95；missing_ratio ≤ existing+0.01；profile_gap_ratio ≤ existing+0.02；effective_lane_total ≥ existing×0.98。
5. **completeness_higher**：candidate 分 ≥ existing 分 +1.0（:1289）。
6. **materially_higher**（:1294–1310）：count/profile_detail/evidence/lane_total 各自 ≥ max(existing+絕对底(20|100), existing×1.05)；coverage_materially_higher = lane_total 单独达标 或 三项(count+detail+evidence)同时达标（:1311–1314）。
7. 四条 promote 支：`explicit_baseline_inclusion_promotable`（incumbent snapshot 在 candidate 的 selected_snapshot_ids 里 + sort_key 更高 + (subsumption 且 score_gap≤1.0 或 低分翻身条款：existing<50 分且 candidate≥+10 分且 gap_ratio 改善 0.05 且 missing≤+0.02)，:1327–1341）／`completeness_higher AND subsumption_higher`／`materially_higher_coverage_despite_snapshot_count_bias`（subsumption+材料性增益+incumbent 的 source_snapshot_count 偏多+score_gap≤3.5，:1342–1349）／`materially_higher_coverage_with_stable_quality`（subsumption+材料性增益+score_gap≤1.0，:1319–1323）。
8. **completeness_score 本身是硬公式**（asset_reuse_planning.py:731–754）：`35 + profile_coverage_ratio×45 − missing_ratio×18 − profile_gap_ratio×10 − manual_ratio×5 + min(8, source_snapshot_count×1.5)`，clamp [0,100]；explicit_profile_capture 按 0.35 折价计入 coverage。带位 high≥75 / medium≥50 / low。
9. **存储层 guard**（storage.py:8634–8696，refusal 两形）：①同 `materialization_generation_key` 但 sequence 更低 → `stale_generation_sequence_replay`；②不同 lineage 且 incoming selected_snapshot_ids ⊂ incumbent（非空严格子集）→ `source_snapshot_coverage_regression`。拒绝时 row 仍落库但非 authoritative，返回 `authoritative_promotion_refused` 载荷（:8684–8695）。不带 lineage 的写者保留 pre-guard 行为（记档的 escape hatch）。

### 2.3 历史错误 promote 案例（取证）

1. **2026-07-22 OpenAI generation 回退事故**（commit `bc6b3fd` 正文）：stale-job recovery reconcile 重物化 OLD snapshot，把 OpenAI authoritative 从 generation sequence 6（dual-source superset）翻回 3——`upsert_organization_asset_registry` 当时无条件降级所有 authoritative 同伴行。第一版 guard 跨 lineage 比较 sequence 又误伤 serving-repair 流（3 个测试失败），`86db42c` 改为「同 lineage 低 sequence + 跨 lineage 覆盖严格子集({041551} ⊂ {104157,041551})」双形拒绝。PG 实测现值：OpenAI authoritative=20260720T104157, seq=6 的 lineage 键 `92f5c3fc…` seq=6 ✔。
2. **Google simulate 污染 + 合并式重建事故**（commit `d542b3c` 正文）：旧 authoritative 20260720T152139 含 **40 个 simulate 占位行**仍被当权威；`rebuild-company-serving-view` 跨 snapshot 合并扫把 stale April + simulate 源合成 12,837 行、40 处 placeholder 泄漏的视图（75 分钟，中途 kill、产物隔离）——替换为 guard 保护的单 snapshot promotion 脚本后 Google 切到 20260722T221424（4,297 real，reason=`explicit_baseline_inclusion`）。
3. **TML 诚实拒绝案（guard 正确工作的对照组）**：同 commit——TML promotion 被 guard 按真实指标拒绝，改走 cache-merge（192 Full-mode envelope 并入 183049），未加 override。
4. 周边债：`.coord/BOARD.md` 与 NEXT_TODO「shard lineage backfill、profile_fetched 对账」是 promote 证据链的已知缺口（继承收口跟进，REFACTOR_MASTER_PLAN.md §7 末行）。

### 2.4 今天 per-shard 记录的 HarvestAPI 请求参数（AI promote 判断的可用输入面）

- **acquisition_shard_registry 列**（storage.py:9341–9371 row_payload）：`shard_key/lane/status/employment_scope/strategy_type/shard_id/shard_title/search_query/query_signature/company_scope_json/locations_json/function_ids_json/result_count/estimated_total_count/provider_cap_hit/source_path/source_job_id/materialization_generation_{key,sequence}/materialization_watermark/metadata_json`。metadata 里另有 `query_family`（raw/normalized/canonical_label/signatures，asset_reuse_planning.py:218–246，`refresh_acquisition_shard_registry_query_family_metadata` :266–281）。
- **per-entry 出处**：`annotate_roster_entry_shard_provenance`（connectors.py:162–204）给每条 roster row 打 `source_shard_id/title/filters` + 多值 `source_shard_ids/source_shard_provenance`，并从 shard include filters 反填 entry 级 `function_ids`；跨 shard 合并走 `union_roster_entry_provenance`（connectors.py:219–…，function_ids 有序并集，singular 字段 first-shard-wins）。
- **provider payload 实际映射**（harvest_connectors.py:4881–4901 `_apply_harvest_company_employee_filters`；:4903–4960 `_apply_harvest_search_filters`）：companies/locations/excludeLocations/functionIds/excludeFunctionIds/jobTitles/excludeJobTitles/seniorityLevelIds/excludeSeniorityLevelIds/schools/searchQuery（former probe 有 GDM functionIds['19'] 事故守卫注释 :4908–4920）。
- **缺口（对 AI-native promote 重要）**：shard registry **不落** job_titles/seniority/exclude_* 维度与完整 provider payload 快照；`estimated_total_count` 实测普遍为 0（PG 8 行样本全 0）；`provider_cap_hit` 有列但样本全 f。真实样本（PG 只读，Google 广召回轮）：`profile_search|former|former_employee_search|"Google Multimodal Researcher"|["United States"]|["8","9","19","24"]|result=304`——广召回的 keyword×location×functionIds 组合已可回放，但 total/截断证据薄。

---

## 3. X-First 方向划分规则（动刀前必读的既有方法）

### 3.1 方向划分：runtime taxonomy + 类型化 scope catalog（不是硬编码 pre-train 字段）

- 入口 `x-first-researcher-sourcing/AGENTS.md`（41 行，安全/所有权 8 条：evidence-first、fixture 默认、pp_x_<ULID> 可逆链接、assertions 恒空、经 versioned artifact adapter 集成、禁保护属性推断）。
- **划分规则本体**：`docs/GENERALIZED_RESEARCH_ORCHESTRATION.md`（306 行）——"The target direction is a runtime taxonomy, not a pre-training field. The same request shape can represent pre-training, post-training, …"。每个分析问题 = `analysis_mode(exploratory|verification|hybrid) + dimension_id + target labels`；判定输出四态 `target_core|target_adjacent|ambiguous|out_of_scope`（取代旧 pretraining_core 专用指标）。**operator 例子的映射**：pre-train 方向=research_program/capability 节点、TBD 组织/团队=organization/team 节点、Gemini 模型=model 节点、Codex 产品=product 节点——正是 `x.research_scope.catalog.v1` 的九类 scope_kind：`organization, model, product, application, research_program, capability, industry, team, initiative`（contracts/x.research_scope.catalog.v1.schema.json $defs.scope_node.scope_kind；configs/research_orchestration_policy.v1.json `supported_scope_node_kinds` 同枚举）。
- **维护方法**：catalog 节点带 `aliases/containment_parent_scope_ids/status(active|historical|unknown)/last_verified_at/refresh_after/evidence_refs/source_status`；campaign 可选显式节点或 `complete_under_roots`——后者**fail-closed**：要求每个 root/kind 对恰有一条 fresh `complete` coverage assertion，"the planner will not silently use a stale Project Family"。探索模式发现的新题目只进 `exploratory_findings`，**不静默改写请求 taxonomy**（进人审后再入 catalog）。旧 free-form `project_family` 字符串在此边界退役；source-neutral pre-train lane 保留为 replay-compatible specialization（冻结字段不改名）。
- 判定通道注册表（configs/research_orchestration_policy.v1.json）：`channel_order = candidate_authored_surface(默认开,primary evidence) → project_direct_credit(默认关) → official_source(默认关) → conversation_graph(默认关,low-authority seed)`；启用可选通道不升权威等级。质量指标 7 项（`target_direction_core_rate`、`target_core_unique_account_yield_per_native_call` 等）。Stage 1 发现层另有 8 个 query family（configs/query_families.v1.json：official_lab_output / public_bio_affiliation / first_party_technical_posts / replies_mentions / official_lab_interactions / curated_lists / paper_conference_linkage / one_hop_graph_and_conflict_checks，各带 max_pages/max_observations）。

### 3.2 今天的两层行为分裂（对应 operator 指令点 5 的 ①②）

- **层①（账号/Bio 定位，执行一次足够）**：`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md` Phase 1 compact discovery（Grok 在 configured strategy shard 内自选 native-X 检索；`x_user_search` 在 tool 层禁用防止发现预算被重复 hydration 消耗）→ 独立的 Grok user-search hydration phase 对 union 补 profile 字段；"One account is retrieved once for all questions, so the planner does not duplicate the broad Post/Reply collection per question"（GENERALIZED_RESEARCH_ORCHESTRATION.md 授权面）。campaign 用 versioned target descriptor（aliases/官方 handle/项目族/时间 shard/role scope）驱动，换 lab 只换 descriptor 不换代码。
- **层②（特定方向收集与判断，按需执行）**：authored Post/Reply 语义评审按 dimension_id×target labels 逐问执行；affiliation 时间与 target-activity 时间独立四态（`current|historical|ambiguous|unsupported`）；continuation-chain 逐页可续（`research_in_progress` 合法暂停态，frontier 未耗尽不许报 complete/failed）；正向终止 `target_match_proven` vs 负向低边际增益终止（须覆盖全 label×surface 对 + scope-frontier audit SHA-256）。
- 与主项目的边界：只出 raw observations + evidence proposals，经 `sourcing.x_first.subject_selection.v1` / `x.portable.research_campaign.*` adapter 交接（AGENTS.md 条 6/7），主产品 adjudication owner 接受 link proposal 后才落人档——**semantic scholar 未来纳入同一「定位来源→收集→判断」抽象时，portable seed kinds（x_account|linkedin_profile|professional_profile|name_only）与 channel registry 是现成的扩展点**。

---

## 4. HarvestAPI actors 预算模型（「1~2 轮预算」的量化定义）

### 4.1 Actor 面与计价（docs/HARVESTAPI_PLAYBOOK.md）

- 三个 actor（:40–66 配置样例）：profile_scraper `LpVuK3Zozwuipa5bp`（batch enrichment，known URL）；profile_search `M2FMdjRVeF1HPGFcc`（former/company-scoped recall）；company_employees `Vb6LZkh4EqRlR0Ka9`（roster）。
- 计价：profile 详情 **$4/1k（no email，项目默认 collect_email=false）** vs $10/1k with email（:103–105；`chargedEventCounts.profile_with_email>0` 即漂移告警 :70）；profile-search 每页 ≈$0.10（`_recommended_harvest_profile_search_charge_cap_usd`，harvest_connectors.py:3794–3797）；company fallback $4/1k（:3800–3803）。
- 成本护栏公式（harvest_connectors.py:3782–3803）：`estimate = price_per_1k×items/1000`（价格从 mode 字符串 "$X per 1k" 正则解析，:3783–3784）；profile cap = `max(est+0.25, est×1.25)`；search cap = `max(est+0.25, est×1.15)`；company cap = `max(est+2.0, est×1.5)`。调用点 :693–699/:965–971/:1539–1547（`maxItems` 被 `max_paid_items` 钳住）。
- 当前状态：**月配额已耗尽**，系统性探针轮推迟到配额恢复（HARVESTAPI_PLAYBOOK.md:430；与 §6.5 W7.2 「live 验证等配额恢复」一致）。

### 4.2 并发预算的三层数字

| 层 | 数值 | 锚点 |
|---|---|---|
| provider 隐性上限 | **~8 并发 actor**（旧 8 槽 API 信号量的真实由来） | SERVICE_GRADE_ARCHITECTURE_PLAN.md:23；契约 §1 将其绑定为 `MAX_BATCH_COUNT=8` 的 wave fan-out 上限 |
| DB 级跨进程 limiter | `harvest_profile_scraper_actor` key，**默认 4**（fast_smoke=2） | HARVESTAPI_PLAYBOOK.md:110–111；runtime_tuning.py:354–360（resolver default=4）/:30（fast_smoke 2） |
| 进程内信号量 | `harvest_profile_scrape=4`、`harvest_profile_batch_submit`（submit budget，默认 max(1,actor)） | runtime_tuning.py:31–32/:345–352/:363–374；enrichment.py:4089–4096 |
| roster 侧 | `harvest_company_roster_global_inflight=4`、并行 shard 8 | runtime_tuning.py:34/:24；limiter key `harvest_company_employees_actor`（playbook:191） |
| playbook 推荐 in-flight batch | roster-heavy ≤**3** 并发 batch；profile-search/targeted **2**；non-live ≤**4** | HARVESTAPI_PLAYBOOK.md:373–376 |

编排要求（playbook:114–115、:355–366）：submit/ingest/materialize 是相邻阶段不许退化成单线程瀑布；"任一 actor completed 后，立即消费 completed dataset……只要 budget/lease 允许，就继续提交下一批 deferred URLs"——**这句playbook 目标与 §1.3 实测的 signal-only+5s poll 之间的落差，就是指令点 1 的工程标的**。

### 4.3 「1~2 轮 actor 预算」的量化定义（供 W7.1 设计用）

- **1 轮（round）= 一次填满 actor 并发预算的 wave**：即同时在跑的 profile-scraper actor 数 = `harvest_profile_actor_global_inflight`（live 默认 4；provider 天花板 ~8）。
- AI 划成 **4–8 批**时：4 批 @ inflight=4 → **1 轮**齐发齐收；8 批 @ inflight=4 → **2 轮**；8 批 @ 天花板 8 → 1 轮。即目标合同可写成：`batch_count ∈ [4,8]` 且 `ceil(batch_count / actor_global_inflight) ≤ 2`，每批 ≥50（避免 tiny）≤300（provider envelope cap），批间均衡（余数不产生独立小尾批——尾差摊进各批）。
- 成本核算随批：每批 cap = `max(est+0.25, est×1.25)`，est = `$4×batch_urls/1000`（no-email 模式）；一个 2,000 URL 的 ready set ≈ $8 无 email 全量，划 8×250 每批 est $1.00/cap $1.25。
- 失败重试预算：保留 url 级失败记录 + normal wave 关闭后一次 `retry_wait_isolated_dispatch`（§1.4）——重试批天然是第 2 轮（或 2 轮内的尾波），`retry_isolation` 是 tiny 批的合法理由（enrichment.py:1959）。zero-result transient retry 另计（playbook:150，默认 2 次，属 actor 内部不占 wave）。

---

## 5. 设计问题清单（W7.1 前 operator 裁决必须回答）

**A. AI batch 划分（指令点 2）**
1. AI divider 的**座次**：替换 enrichment.py:677 的 sizer（plan builder 内，输出 batch_size）还是升格为独立 plan 阶段直接产出 `dispatch_item_specs`（批数+每批成员+理由）？后者才能表达「4–8 批不均等切分」，但触碰 R6 durable wave 继承与 characterization 基线。
2. AI divider 的**输入面**：ready URL 集 + source_mix + worker_budget 是现有输入（enrichment.py:1422–1437）；是否加：per-URL 历史失败/attempt 数、registry queue_state、actor 计价与 charge cap、job 优先级、company 规模先验？输入快照是否入 plan record 供回放？
3. **失败语义**：AI 划分方（模型调用）不可用/超时/输出非法时 fall back 到现行 sizer 还是 fail-closed defer？（provider fail-closed 红线是否延伸到「内部规划用模型」——今天 model_provider 有 circuit，model_provider.py:179–238。）
4. **保留哪些硬护栏**：建议保留为 AI 输出的验收器——每批 ≤300（provider envelope）、总批数 ≤8（provider 天花板）、tiny 批仅允许 R7 合法理由、R5 wave bounding、R6 同 wave 不重切、R7 reason 审计字段（AI 需产出可解释 reason）。operator 是否确认「规则退位为验收器，AI 在界内自由」这个形状？
5. 合同 §6 遗留裁定 D1（50/55 active/deferred floor 是否意图）在 AI 化后是否直接作废？

**B. Slot 即时填充（指令点 1）**
6. completion event 从 signal-only 升格为「同步执行 bounded replan+submit」还是「事件唤醒 refill daemon 立即 tick」？前者违反现行 "completion callbacks are not executors" 设计决定（orchestrator.py:71632–71637 docstring），需要重新裁决 owner/预算归属与 callback 时延上界；后者保留 owner 但要把 5s poll 变成事件驱动优先。目标时延数字是多少（<1s？）？
7. slot 空转的验收指标是否就用现有 `refill_saturation`/`unfilled_available_slot_count`（enrichment.py:1253–1277）+ event_metrics 时间戳（profile_fetch_owner.py:2678–2701）做 before/after？

**C. AI promote 判断（指令点 3）**
8. **判定模型消费什么**：candidate/incumbent 的 registry 指标对（现 evaluate 的输入）+ per-shard 请求参数（search_query/locations/function_ids/query_family/result_count）+ lane coverage + lineage/selected_snapshot_ids？是否要求先补齐记录缺口（§2.4：完整 provider payload 快照、estimated_total、provider_cap_hit、shard lineage backfill）作为**前置批**？
9. **两道闸的分工**：storage 层 lineage guard（stale replay/coverage 子集，storage.py:8639）是否保留为 fail-closed 硬前置，AI 只在 guard 放行的候选内做 promote/reject？（事故史支持保留：bc6b3fd/86db42c 两轮才校准，AI 不应重新拥有这类回放安全性。）
10. 现行 evaluate 的哪些成分留作 AI 的 feature 而非规则：completeness_score 公式（:731–754）留作特征还是弃用？subsumption 六阈值（0.98/0.95/0.01/0.02）是否全部退役？
11. AI promote 判定的**审计产物**：决策记录（evidence + 理由 + 可回放输入 hash）落在哪（organization_asset_registry.metadata？新表？）；`force_upsert` 逃生门（asset_registration.py:271）保留否；scripts/live_promote_company_snapshot.py dry-run 语义如何对 AI 判定复用？
12. 判定失败/模型不可用时 promote 默认 fail-closed（不晋升、保 incumbent）——确认？

**D. 行为层两层抽象（指令点 5）**
13. 主项目层② 的方向 taxonomy 是否直接采用 X-First `x.research_scope.catalog.v1` 的九类 scope_kind + coverage assertion 维护法（fail-closed on stale root），还是主项目自建注册表再经 adapter 对齐？谁是 catalog owner？
14. 层①「执行一次足够」的幂等记录放哪：person asset 上的 per-source completion marker（类似 X-First `research_in_progress`/terminal 态）？重复触发的判定归 recovery 还是行为层自身？
15. semantic scholar 进入同一「定位来源→收集→判断」抽象时，是作为 X-First portable seed/channel 的新 kind，还是主项目行为层的新 provider？（影响 AGENTS.md 条 6/7 的 assertions-empty 边界。）

**E. 预算与补偿（指令点 2/4）**
16. 「1~2 轮」入合同的正式定义采用 §4.3 的 `ceil(batch_count/actor_global_inflight)≤2` 吗？actor_global_inflight 以 4 为准还是探针轮验证 8 天花板后调高？预算 owner 归 M2 provider runtime 还是调度器（合同 R8 现状：sizer 层 8 上限 + plan 层 min(specs, available)）？
17. 补偿机制的原子单位与命令族：补 acquire/补 fetch/补 materialize/补 promote 各自成为 typed durable command（复用现 command registry/owner 模式）？谁产生「缺口清单」（对账器 vs 各环节 owner 自报）？与现有 NEXT_TODO 遗留（shard lineage backfill、profile_fetched 对账、8 条 stalled running）如何并轨？
18. characterization 顺序（W7.2 红线）：先钉哪条基线——scheduler plan record 形状（含 refill_saturation）与 promote decision record 形状是两条独立 oracle？tick oracle（10/10 byte-identical）在 AI 化后如何降级为「界内验收」而不失去防回归力？

---

*侦察方法附注：全部只读（Read/grep/git log/PG SELECT）；PG 经 `SOURCING_CONTROL_PLANE_POSTGRES_DSN`（55432 Docker），旧 5432 DSN 已失效；未跑任何测试、未触碰 live provider。合同文档行号漂移已按当前树重测（§1.1 表）。*
