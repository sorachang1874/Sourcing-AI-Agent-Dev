# Profile-Prefetch Scheduler 契约

> Status: Active contract (2026-06-12). Phase 4 Step 0 deliverable.
> 锚点树：`governance-phase0-ttl-20260611` @ fd1debb；全部 file:line 指 `src/sourcing_agent/enrichment.py`，除非另注。
> 上游设计：`docs/PHASE4_ENTANGLED_CORE_DESIGN.md` §3 Step 0 + §4 决策 #1/#2（owner 2026-06-12 批准）。
> 守卫不变量来源：`docs/WORKFLOW_BEHAVIOR_GUARDRAILS.md`（invariant 1 = :31 同窗口不重复派发；invariant 7 = :180 区分"查找失败" vs "确认不存在"）。

## 1. 目的与范围

scheduler 把一组 ready/refill 的 LinkedIn profile URL 切成 **provider envelope**（remote-run 信封）并按 actor budget 限流派发；durable item 才是 retry/dedupe/progress/recovery 单元（参见 `docs/DISCOVERY_PROVIDER_QUEUE_CONTRACT.md`、`docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`:441/534/579）。链路位置：`queue_background_profile_prefetch`（:4975）build plan → dispatch loop（:6027）→ `_dispatch_prefetch_chunk`（:5810）→ `run_linkedin_profile_refill_submit_command_once`（:2995）。recovery tick 的 profile_refill 链（orch:38589 起）直接消费本调度器输出，故 Step 1 特征化前契约必须先冻结（设计 §2(e) E2）。

与 M2 provider 预算对齐方向：**优先更少更大的 provider envelope（在 actor 预算内），避免碎片化，durable refill wave 一旦成形要被保留**。HarvestAPI ~8 actor 并发隐性上限 = `HARVEST_PROFILE_PREFETCH_MAX_BATCH_COUNT_FOR_LARGE_READY_SET`（=8，:492）这一 wave fan-out 上限的真实由来（`NEXT_TODO.md`:63、`SERVICE_GRADE_ARCHITECTURE_PLAN.md`:23）。本契约只定调度形状语义；预算本体随 M2 provider runtime，不在此改。

尺寸阶梯（`max(...)` 层叠保证序不变，:484–503）：`ACTOR_SLOT_URL_TARGET=50` ≤ `DURABLE_UNIT_MAX_URLS=200` ≤ `PROVIDER_ENVELOPE_MAX_URLS=300`；`SCALE_THRESHOLD_URLS=400`；`MAX_BATCH_COUNT=8`。coalescing 阈值：`MIN_NON_TAIL_BATCH_SIZE=10`（:464）、`TINY_BATCH_MAX_SIZE=5`（:468）、`LOW_VOLUME_COMPANY_MAX_URLS=20`（:472）。

## 2. 契约规则（编号、可测试）

核心 sizer = `_recommended_harvest_profile_prefetch_actor_slot_batch_size(total_urls)`（:676–700），已被 `tests/test_enrichment.py`:6388–6446 参数化特征化。每条标注：**[符合/回归]** · 覆盖测试 · stale(改测试) / regression(改代码)。

**R1 envelope sizing — 四带：**
- R1.a `count<=50` → `(50,"actor_slot_item_packing")`（:681）。小集合一个 actor-slot 信封，不碎片化。**[符合]**。
- R1.b `50<count<=300` → `(count,"single_durable_unit_ready_set")`（:684）。51..provider-cap **保持单一信封**（"fewer larger envelopes"）。注意上界是 `provider_envelope_max=300` 而非 `durable=200`。**[符合]** · `test_scripted_prefetch_uses_live_like_window_for_profile_search_tail`（247→单 envelope）· **stale**（测试要旧 124×2 半切）。
- R1.c `300<count` 且 `target_batch_count=ceil(count/300)<=8` → balanced `max(50,min(300,ceil(count/n)))`，reason = `large_ready_set_provider_envelope_target`（`count>400`）否则 `bounded_ready_set_balanced_provider_envelopes`（:689–699）。**[符合]** · `test_profile_prefetch_batch_plan_replans_large_late_shard_without_historical_budget`（1600→267×6）· **stale**（测试要旧 200-cap）。
- R1.d `target_batch_count>8` → `(300,"large_ready_set_provider_envelope_cap")`（:700），余量 defer。**[符合]**。

**R2 roster split 覆盖 R1.b：** `_should_split_roster_ready_set_across_actor_slots`（:710）当 `50<count<=200`、`available_new_worker_count>=2`、roster ratio `>=0.5` → 强制 `batch_size=50` reason=`roster_actor_slot_fill`（:1393–1409）。roster wave 用开放 actor slot 而非一个大 shard（docstring :716；invariant 1 的"large company roster 走更保守 tail"）。**[符合]** · `test_concurrent_profile_prefetch_replan_respects_reserved_actor_budget`、`test_queue_background_profile_prefetch_reserves_same_wave_urls_before_submit`、`test_queue_background_profile_prefetch_records_batch_envelopes_and_underuse` 断言旧"每 50-slot fan-out" → **stale**（按新 packing 重算 envelope 数；其中 tail-state / underuse 语义是真不变量，更新而非删除）。

**R3 tiny-batch coalescing：** `_coalesce_tiny_profile_dispatch_chunks`（:1006–1058）。`<=1` chunk 或 low-volume（requested 与 candidate 均 `<=20`，:1021–1026）跳过；`<=tiny(5)` 累进 `pending_tail`，达 `>=min_non_tail(10)` 刷出（:1042），final tail 并入末 chunk（:1037）。provider 不应见碎片化 tiny actor run。**[回归]** · `test_profile_prefetch_refill_daemon_releases_ready_sub_50_tail`（47 应作 ONE `queue_quiescent_final_tail` wave，实测裂成 25+22）· `test_profile_prefetch_refill_dispatch_uses_durable_item_ownership_without_worker_scan`（4→应 1 worker，实测 2）· `test_enrich_full_roster_prefetch_uses_canonical_scheduler_without_tiny_parallel_submit`（4→应 1，实测 4 并行 tiny submit）· 全部 **regression(改代码)**：tiny-coalescing 未在 refill/canonical 热路径生效。

**R4 sub-50 tail guard：** `normal_count<50` 且非 low-volume、非 final-tail → 早返回 `plan_reason=deferred_coalescing_sub_50_tail`，全 defer（:1428–1451）。tail item 进 `deferred_coalescing`（PROFILE_REFILL_NORMAL_QUEUE_STATES :519），除非 `allow_under_target_final_tail_dispatch`（queue-quiescent，:1499–1500 = `queue_quiescent_final_tail`）或 retry isolation。**[符合]**（状态语义是真不变量，stale 测试更新时须保留）。

**R5 wave bounding：** 每 tick fan-out = `min(dispatchable_specs, available_new_worker_count)`，余量进 `deferred_urls`（:1477–1486）。这是 invariant 1 的 durable 表达（`WORKFLOW_BEHAVIOR_GUARDRAILS.md`:31「同 job/stage/payload 同窗口不重复派发」；guardrail "queued 不是可忽略尾巴…新 URL 超 budget 进 deferred_urls"）。`available_new_worker_count<=0` 时全 defer（:1477–1479）。**[符合]**。

**R6 durable refill wave 继承（赢过新算窗口）：** `_apply_durable_refill_wave_dispatch_window`（:1161–1209）。重排既有 deferred wave（非 append-replan/非 retry）时，继承 registry 记录的最大 `refill_plan_batch_size`；若超过当前则覆盖窗口，`batch_size_reason="durable_refill_wave_batch_size"`、`batch_size_contract="profile_actor_slot_durable_wave_item_packing"`（:1203–1204）。**[符合]**：同一 wave 跨 tick 形状稳定，防止重排导致碎片化（与 invariant 1 一致：同 wave 不应被拆成新派发）。

**R7 `batch_size_reason` 取值语义（可测试枚举）：**
| reason | 触发 | 锚点 |
|---|---|---|
| `no_ready_urls` | count<=0 | :680 |
| `actor_slot_item_packing` | count<=50 | :682 |
| `single_durable_unit_ready_set` | 50<count<=300 | :685 |
| `bounded_ready_set_balanced_provider_envelopes` | 300<count<=400, n<=8 | :697 |
| `large_ready_set_provider_envelope_target` | count>400, n<=8 | :695 |
| `large_ready_set_provider_envelope_cap` | n>8 | :700 |
| `roster_actor_slot_fill` | R2 命中 | :1402 |
| `durable_refill_wave_batch_size` | R6 继承覆盖 | :1204 |

并附 `batch_size_contract` ∈ {`profile_actor_slot_ready_item_packing`（:1369/:1401）, `profile_actor_slot_durable_wave_item_packing`（:1203）} 与审计字段 `actor_slot_url_target/durable_unit_max_urls/provider_envelope_max_urls/large_ready_set_threshold_urls/large_ready_set_max_batch_count`（:1371–1381）。

**R8 actor-budget 上限作用点（两层）：** ① sizer 层 wave 数上限 `MAX_BATCH_COUNT=8`（R1.d）；② plan 层 fan-out `min(specs, available_new_worker_count)`（R5）。HarvestAPI 8-actor 上限只在 ① 影响 envelope 数，不在 ② 重复扣减。M2 落地前预算本体仍寄居 recovery tick（设计 §1 :38719–38726、§4 #6），调度器只读 `worker_budget` 的 `available_new_worker_count`。

**与 invariant 1 关系：** R5+R6 联合保证同一 ready/refill wave 在派发窗口内只消耗一次 new-submit budget；R6 阻止重排把已成形 wave 重切成新派发。任何把 deferred wave 重新当 fresh dispatch 的改动都违反 invariant 1（`WORKFLOW_BEHAVIOR_GUARDRAILS.md`:31/:189）。

## 3. storeless / 配置缺失语义（决策 #1）

**现状（fail-open，要修）：** `self.store is None` 与 `not command_id/not job_id/not profile_url_chunk/not snapshot_dir` 被同一行混并（:3055），统一返回 `_owned_result(status="skipped", reason="profile_refill_submit_command_payload_invalid")`（:3056–3059）。`_owned_result`（:3021）硬编码顶层 `"status":"queued"`（:3031），把 `status` arg 只塞进 `worker_status/small_batch_reason/deferred_reason`（:3049–3051），并在 observed command 上置 `runtime_command_contention=True`（:3029）；消费侧（:5879）见 contention 即返回 `queued_urls=真 chunk`、`dispatched_url_count=0`；聚合（:6161）因 `queued_worker_count>0` 报 `status="queued"`、`dispatched_url_count=0`。净效果：**config/lookup 失败被伪装成正常 queued 终态**，与"零派发即静默满足"无法区分——违反 invariant 7（:180 查找失败必须 fail-closed）。

**契约（fail-closed 硬错误）：**
- `self.store is None` **必须**从混并条件中拆出，走独立 loud-failure 分支：返回 `status="blocked"`、`reason="profile_refill_store_unavailable"`，且 **不得**置 `runtime_command_contention=True`。
- 真正的空载荷（`not command_id/not job_id/not profile_url_chunk/not snapshot_dir`）保留 `reason="profile_refill_submit_command_payload_invalid"`，但顶层 `status` 须如实反映（不再硬编码 `queued`）。
- `queue_background_profile_prefetch` 消费契约：blocked/store-unavailable 信号 **不得**计入 `queued_worker_count`、**不得**汇报 `status` 为 `completed` 或 `queued`、**不得**以 `dispatched_url_count=0` 冒充满足；须冒泡为可观测的 blocked 终态（与 invariant 7 的 `workflow_run_absent`/`workflow_state_unavailable` 双信号同形），让上游 recovery 链区分"基础设施缺失"与"确认无可派发 URL"。
- "确认不存在/空 ready 集合"仍走正常分支（sizer R1 的 `no_ready_urls`、:680），不触发 fail-closed。

## 4. 15 个预算失败处置清单

**test_enrichment.py（12，本契约族）：**
| 测试 | 处置 | 依据 |
|---|---|---|
| test_scripted_prefetch_uses_live_like_window_for_profile_search_tail | 改测试 | R1.b：247 单 `single_durable_unit_ready_set`，旧 124×2 半切已 stale |
| test_profile_prefetch_batch_plan_replans_large_late_shard_without_historical_budget | 改测试 | R1.c：1600→267×6，旧 200-cap stale |
| test_concurrent_profile_prefetch_replan_respects_reserved_actor_budget | 改测试 | R2/R1.b：[223,74] 取代 4×50；nested reserved/dispatched 断言随 shape 重算 |
| test_queue_background_profile_prefetch_reserves_same_wave_urls_before_submit | 改测试 | R2：115→单 envelope；保留 `deferred_coalescing` tail 不变量 |
| test_queue_background_profile_prefetch_records_batch_envelopes_and_underuse | 改测试 | R1.b：180→1 envelope；underuse 语义保留，重算 envelope 数 |
| test_queue_background_profile_prefetch_records_refill_item_state_for_active_and_deferred_urls | **改代码 + owner** | mocked tiny window 下 actor-slot repack（R1.a/R5）未派发任何项（empty），疑 plan/window reconciliation 回归；50/55 floor 是否为意图须 owner 裁定 |
| test_queue_background_profile_prefetch_reports_profile_queue_snapshot | **改测试-infra (stub gap)** | 手写 `_Store` stub 缺 durable typed-command（refill-item ownership）面 → 不派发；修 stub 而非产品码 |
| test_profile_prefetch_refill_daemon_releases_ready_sub_50_tail | 改代码 | R3/R4：47 应作 ONE `queue_quiescent_final_tail`，实测裂 25+22 = 碎片化回归 |
| test_profile_prefetch_refill_dispatch_uses_durable_item_ownership_without_worker_scan | 改代码 | R3：4→1 worker，实测 2 = refill 热路径未应用 tiny-coalescing |
| test_enrich_full_roster_prefetch_uses_canonical_scheduler_without_tiny_parallel_submit | 改代码 | R3：4→1，实测 4 并行 tiny submit |
| (审计 2 第 11、12 项 enrichment envelope/coalescing 族) | 见审计 2 (a)/(b) 同类规则 | 与上同：stale 改测试 / regression 改代码，按 R1–R3 归位 |

分类锚：旧"全切 50-slot" = stale(a)；coalescing/更大信封/no-fragmentation/durable-wave-preservation = 新契约仍回归的 regression(b)；stub 面缺失与纯投影计数另列。

**test_results_api.py（3）— 不属本契约，移交 Phase 4 B 带（projection 计数族）：** 实跑确认（2026-06-12，全量 487s）失败为：
- `test_board_runtime_state_current_snapshot_serving_does_not_infer_profiles_from_row_shell`：断言 `卡片详情已合入看板 112/297`，实测 `186/297` — board-merge 投影计数。
- `test_lovable_board_visible_patches_extend_canonical_projection_membership` — canonical projection membership 扩展计数。
- `test_partial_current_snapshot_overlay_uses_current_snapshot_population_as_expected_floor` — current-snapshot overlay population floor。

三者均为 board-visible / projection membership **投影计数**，下游于调度器，不含 envelope/packing/coalescing 断言（按 `_resolve_board_visible_delta_apply_chunk_size`、`background_reconcile.harvest_prefetch.status` 等投影面，非 scheduler `batch_size_reason`/envelope shape）。**移交设计 §2(b) B2 ProjectionCommandOwner / ServingReadModel 族**（设计 §3 Step 4 边界冻结、§4 #5）；Step 0 不动这 3 个。供 owner 复核此移交。

## 5. 实施清单（主会话 Step 0 执行）

代码改动点（仅 `enrichment.py`）：
1. 拆 :3055 混并条件：`self.store is None` → 独立 fail-closed 分支（§3：`status="blocked"`、`reason="profile_refill_store_unavailable"`、不置 contention）。
2. `_owned_result`（:3021）顶层 `status` 改为如实回传 `status` arg（不再硬编码 `queued`）。
3. `queue_background_profile_prefetch` 聚合（:6161）/消费（:5879）：blocked 信号不计 `queued_worker_count`、不冒充 completed/queued/dispatched=0。
4. R3 tiny-coalescing 在 refill/canonical 热路径生效（修 test_profile_prefetch_refill_daemon_releases_ready_sub_50_tail 等 3 个 regression）；排查 test_..._refill_item_state 的 plan/window reconciliation（mocked tiny window 下不派发）。

特征化测试新增点：
5. sizer R1.a–d 四带 + R7 reason 枚举（扩 `tests/test_enrichment.py`:6388–6446 参数化）。
6. R2 roster split、R3 coalescing、R4 tail guard、R5 wave bounding、R6 durable-wave 继承各一组 io 特征化（输入 url 数 + source_mix + worker_budget → batch_size/reason/contract/dispatch_specs/deferred_urls 形状）。
7. §3 storeless：`self.store is None` → blocked，聚合不报 completed/queued（覆盖原 fail-open）。

契约守卫测试：
8. invariant 1 守卫：同 wave 跨 tick（R6）不产生重复 dispatch_specs。
9. invariant 7 守卫：store-unavailable 不被任何上游当作满足终态。
10. stale 测试改写（§4 8 个 a 类）匹配 R1–R2，保留其中真不变量（tail-state、underuse、reserved budget 语义）。

## 6. 开放给 owner 的剩余裁定

- **D1（(c) 类）** `test_queue_background_profile_prefetch_records_refill_item_state_for_active_and_deferred_urls`：mocked tiny window 下 actor-slot repack 未派发任何项，确认是 plan/window reconciliation 回归（改代码）后，**50/55 active/deferred 切分是否为意图 floor** 须 owner 确认（影响是改代码修回归还是同时改测试期望）。
- **D2（stub gap）** `test_..._reports_profile_queue_snapshot`：确认归 test-infra（补手写 `_Store` stub 的 durable typed-command 面），不计入产品码 regression 预算。
- **D3（移交）** results_api 3 个投影计数失败移交 Phase 4 B 带（§4 #5 边界冻结期处理）是否批准；若 owner 要求随 Step 0 一并清账，需把 B2 ProjectionCommandOwner 计数语义提前定义（超出本契约范围）。
