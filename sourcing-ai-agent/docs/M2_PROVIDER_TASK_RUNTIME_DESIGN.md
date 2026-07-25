# M2 — Provider Task Runtime 设计提案（Scoping Design Proposal）

> Status: **RATIFIED (2026-06-16)**. Owner approved decisions A–E as recommended
> (A separate / B one-first+opt-in / C webhook-primary+poll-fallback / D conservative
> deletion deferred to Phase 4 / E no 8-slot raise) + the §5 increment order
> (M2.0→M2.6) with **R1 go/no-go abstraction spike front-loaded before any execution
> migration**. M2 = typed "Provider Task
> Runtime" on the M1 durable command substrate. Consolidation-first (Track C house style:
> 先钉死再迁 / characterize-first, delete-don't-dual-track, explicit owner decision points).
> Built from a 6-reader read-only understand-workflow over the provider / task / budget
> surface; line refs anchored to `src/sourcing_agent/` and code-verified. **设计 only — 未改码。**
>
> **接地核验（vs. 初始图谱，已读码确认）**：(1) drain registry 现有 **16** bindings（非 14）。
> (2) `operation_native_profile_fetch` 命令族（`durable_runtime.py:253-278`：`LINKEDIN_PROFILE_
> FETCH_ACTIVITY_RUN/_PROVIDER/_TERMINAL_ADMIT` + `PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_
> QUARANTINE` + drain phase `operation_native_profile_fetch_activity_owner`）**已经是一个 provider
> 的 durable-task 实现**。**M2 = 抽出这条已证明血脉的 shape，泛化成 ProviderTaskSpec，再逐个收编其余
> provider——而非从零发明 runtime。** 这把 R1（抽象漏判）从「发明」降级为「泛化」，显著降险。

---

## 0. 一句话定位

M1 给了一个 durable command 基底（CommandTypeSpec registry + event/reducer/outbox + lease claim
+ recovery drain registry）。**M2 不是新基底，是基底之上一个 typed leaf 层**：把「调用一个外部
provider」做成 typed durable task，由 **唯一一处** owner 并发/预算/退避/回退/idempotency。今天这件事
散落在 ≥4 条互不相干的 submit→poll→fetch 私有循环里，与 durable 基底零相关。

---

## 1. 问题陈述（现状碎片化）

### 1.1 Provider-call 生命周期分裂在 ≥4 条平行私有 async runtime

每条自带 checkpoint dict、自己的 retryability 判定、自己的 attempt 计数，与 durable 基底零相关：

| 平行 runtime | 入口锚点 | 私有 retry/backoff |
|---|---|---|
| **Apify Harvest actor** | `harvest_connectors.py:4295` `_run_harvest_actor` → `_run_harvest_actor_via_async_dataset:4258`（submit `:3855` → poll `:4274-4281` → fetch `:3929`），deadline `:4273` | `HarvestRetryableRequestError`，page-fetch retry `:3999-4024` 与 submit/poll retry 分开调用 |
| **DataForSEO 三相** | `search_provider.py:1088` submit → `:1348` poll（`tasks_ready` + ThreadPool direct-probe `:1388-1402`）→ `:1600` fetch | per-item retry `_dataforseo_batch_item_retry_count()` 硬编码 **MAX=1** `:1124` |
| **seed_discovery 零结果重试** | `seed_discovery.py:740` `_execute_query_spec` → `search_provider.execute_with_checkpoint`，紧循环 `sleep(backoff)` | **inline sleep——崩溃即丢失** |
| **LLM** | `model_provider.py` `_call_openai_compatible_api`，inline retry | circuit-breaker `_MODEL_PROVIDER_CIRCUITS` cooldown 900s，**无 idempotency_key、无 command 关联** |

直接后果：`connectors.py:539` / `enrichment.py:9276+` 直 HTTP connector 用 `time.sleep`——未计量、崩溃
不安全。orchestrator 认识每个 command type 与 owner，但对 provider-call 的内部 retry/backoff/limiter-wait
是 **盲点**——只看见 success/failure。**这就是 M2 要插入的拦截点。**

### 1.2 Bypass：命令计划了，执行却逃逸到基底外
- `enrichment.py:3033` `run_linkedin_profile_refill_submit_command_once` 计划 durable 命令，执行立刻转到
  `harvest_profile_connector.execute_batch_with_checkpoint()`——checkpoint 是 provider-side
  （run_id/dataset_id），命令在基底里 running，真正 submit→poll→fetch 在基底**外**。
- `seed_discovery` discovery query 已部分接入（`:392-450` retry post `CommandPlanRequested`），但 item
  创建仍 ad-hoc（`:305-483`）。

### 1.3 并发/预算/退避护栏散在三层，无单一 owner
1. **HTTP 层**：`_RequestConcurrencyMiddleware` 两车道 `api.py:389-434`，shared=8（`api.py:62-64`，护
   HarvestAPI 隐藏 ~8-actor 上限），light_reserved=`min(4,max(2,//4))`。
2. **Runtime inflight 层**：`runtime_inflight_slot` `runtime_tuning.py:705`，per-(lane,budget)
   `BoundedSemaphore` 缓存于 `_GLOBAL_INFLIGHT_SEMAPHORES`；`resolved_harvest_profile_actor_global_inflight=4`
   `:354` 等 5 个 per-lane budget。
3. **Worker 层**：`resolved_lane_budget_caps` `:764`（search=8 / public_media=6 / exploration=5）。

`build_provider_backpressure_budget_report` `:394-483` 算 `recommended_action` 但 **不强制**——caller 须
自觉看 `defer_provider_submit`（`orchestrator.py:64164/64205/64287`）。「建议但不执行」的脆弱缝。

### 1.4 Ad-hoc worker loop vs durable 基底（双状态机）
`AutonomousWorkerDaemon`/`PersistentWorkerRecoveryDaemon`（`worker_daemon.py:114/293`）跑 lane-specific
`_resume_*`（`:870-1011`），与基底命令路由并行。**profile refill 双状态机**：control-plane queue item +
durable workflow command（`LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH`）——同一笔活两套状态。

### 1.5 async_task_contract（C1）与 durable 基底的关系
`async_task_contract.py:79-180` 是 **纯投影层**（~30 domain status → 6 canonical），**不拥有 idempotency/
retry/budget**——`idempotency_key` 只是 pass-through。坐在 client 与 durable 基底之间。**是 envelope，不是
substrate。** 见 §6 决策 A。

---

## 2. M2 北极星：typed Provider Task Runtime

### 2.1 它「是」什么
单一 typed 抽象——把「以 durable task 调用外部 provider」固定为生命周期：
```
submit ──▶ pending ──▶ ready (poll OR webhook) ──▶ fetch ──▶ apply ──▶ terminal
                │                                                          ▲
                └──── retry/backoff (durable not_before_at, 非 sleep) ─────┘
```
跑在 M1 基底（`upsert_workflow_command` → claim → mark_running → append_event → enqueue_outbox →
mark_succeeded/failed），**唯一一处** owner：provider 并发 / 预算-backpressure / retry-backoff（durable
`not_before_at`，崩溃安全）/ fallback / identity-idempotency。**不是从零发明**：`operation_native_profile_
fetch` 命令族已是这个形状的一个 provider 实现，M2 = 抽其 shape 泛化。

### 2.2 ProviderTaskSpec（类比 CommandTypeSpec，frozen 纯数据，进 registry，golden 钉死）
```python
@dataclass(frozen=True)
class ProviderTaskSpec:
    provider_task_type: str        # "harvest.profile_batch" | "dataforseo.serp_batch" | "discovery.query" ...
    provider_family: str           # "apify_harvest" | "dataforseo" | "model" | "html_search"
    submit_mode: str               # "sync_run" | "async_submit_poll" | "batch_submit_poll_fetch"
    readiness_signal: str          # "webhook_primary_poll_fallback" | "poll_only" | "synchronous"
    after_start_mode: str          # 复用 PROVIDER_AFTER_START_CONTROL_MODE_*（durable_runtime.py:84-86）
    inflight_budget_key: str       # → runtime_tuning resolver（不复制常量）
    cost_budget_key: str           # cost_policy / lane_budget_cap 键
    retry_policy: RetryContract     # {max_attempts, backoff_strategy, jitter_ms, deadline_unix_seconds}
    retryable_classifier: str      # 解析为分类函数名（getattr，spec 不持绑定方法）
    identity_key_recipe: str       # 解析为 idempotency_key 构造函数名
    fallback_chain: tuple[str, ...]
    command_type: str              # 必须对应一个已注册 CommandTypeSpec（M2 不绕过命令 schema）
    terminal_admit_handler: str    # 复用 terminal-admission（类比 LINKEDIN_PROFILE_TERMINAL_ADMIT）
```
Spec 纯数据（getattr 在 dispatch 解析，与 `CommandTypeSpec` cancel/resume_handler 机制一致
`durable_runtime.py:131-133`）。每个 ProviderTaskSpec **必须**绑一个已注册 `CommandTypeSpec`。

### 2.3 单一 ProviderScheduler（吸收 §1.3 三层）
输入 ProviderTaskSpec + cost_policy + priority；查 backpressure（active/queued/per-provider limit）；满 →
claim slot + submit；不满 → **durable defer**（写 `not_before_at`，recovery 自动晋升，**删 ad-hoc
`defer_provider_submit` flag**）。`_GLOBAL_INFLIGHT_SEMAPHORES` 移入 scheduler 内部实现细节。

---

## 3. 合并 vs 保留 vs 删除

### 3.1 折叠进 M2（CONSOLIDATE）
| 现有路径 | 锚点 | 折叠为 |
|---|---|---|
| Harvest profile/company/search actor | `harvest_connectors.py:442/1152/4295` | `harvest.profile_batch` / `.company_roster` / `.profile_search`（复用 profile-fetch 血脉） |
| DataForSEO 三相 batch | `search_provider.py:1088/1348/1600` | `dataforseo.serp_batch`（batch_submit_poll_fetch），task_key identity 保留 |
| profile refill 双状态机 | `enrichment.py:3033` + control-plane queue | **单一** command owner（store source-of-truth，worker checkpoint 降为 shadow） |
| discovery query item | `seed_discovery.py:285-483` | 接入既有 `LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE`，零结果重试改 durable `not_before_at`（删 inline sleep） |
| 三层 retry 常量 | `search_provider:1124` / model env / worker retry_limit / runtime_tuning backoff | 单一 RetryContract |
| `_utc_now_iso`/`_coerce_int`/`_parse_timestamp` 三重复制 | command_kernel / acquisition_command_owner / orchestrator / worker_daemon | 抽到 shared util |

### 3.2 逐字保留（PRESERVE，引用不重写）
8 槽 shared HTTP 信号量 `api.py:62-64`（**M2 把重活 wall-clock 移出槽，不抬高 8 槽天花板本身**）；两车道
`:389-434`、threadpool≥2×lane budget `:410-414`；per-lane inflight budgets `runtime_tuning.py:354-391`
（默认值/env 优先级不改）；runtime_inflight_slot 重入 `:705-761`；DataForSEO MAX 100/req
`dataforseo_client.py:14`；provider_execution_policy primary_only `:28-72`；remote-event 唤醒上限（default
4/tick）；**16 drain binding 固定顺序** `recovery_drain_registry.py:68`（test 钉死，新 owner 插正确 phase，
不重排现有 16）；PINNED 文字源缝（crm_public_web_* drains、纠缠 cascade INLINE、crm_writer
include_in_result=False）。

### 3.3 删除（DELETE，不 wrap）
`defer_provider_submit` ad-hoc flag → ProviderScheduler durable defer；inline `sleep(backoff)`（seed_discovery
零结果 / `connectors.py:539` per_page_delay / enrichment lease poll）→ durable `not_before_at`；重复 retry
常量。**legacy bridge flags / 直 HTTP connector 的删除绑 Phase 4 / 迁移 cutover——不在 M2 早增量**（§6 决策 D）。

---

## 4. M2 不可破坏的 invariants（逐条 + 今天在哪强制）

I1 8-actor HTTP 上限（`api.py:62-64`）· I2 threadpool≥2×lane budget（`:410-414`）· I3 per-lane inflight≥1
+ env 优先级（`runtime_tuning:354-391/322-342`）· I4 runtime_inflight_slot 重入查 `acquired`（`:705-761`）·
I5 DataForSEO ≤100/req（`dataforseo_client.py:14`）· I6 backpressure 须被尊重（`:461-462`）· I7
idempotency_key 确定性派生 + `(workflow_run_id, idempotency_key)` UNIQUE（`storage.py:14754/15214`）· I8
lease claim 原子（`storage.py:15097-15153`）· I9 terminal finality（`mark_*:15194-15297`）· I10 terminal-event
写先于 recovery dispatch（`orchestrator.py:38300-38305`）· I11 provider slot acquire/release 原子 w.r.t. 并发
recovery（`enrichment:3558/3758`）· I12 refill queue 状态一致（planned_dispatch 持有到 terminal 释放）· I13
causality（parent_command_id / readiness，`durable_runtime.py:716-755`）· I14 lane 并发限 + priority
（`worker_scheduler:144-249`）· I15 checkpoint 嵌套 dict 存活（`worker_daemon:870-1011`）· I16 prerequisite DAG
reawaken· I17 tick budget 强制（`worker_daemon:451-574`）· I18 崩溃安全 checkpoint（`:816-868`）· I19 drain
固定序 16 binding（`recovery_drain_registry.py:68`）· I20 per-lane rate limit cooldown 默认 15s
（`search_provider:69-75`）· I21 offline/scripted 隔离不发 live API（`assert_live_provider_access_allowed`）。

---

## 5. 增量交付序（characterize-first，最小先行，每步独立可发 + curated contract lane CI 绿）

- **M2.0 — Characterize（前置，不改行为）**：pin 四条 provider 私有循环的可观察形状（task_key 映射、harvest
  deadline、DataForSEO pending codes 40601/40602、retry MAX=1、retryability 判定）、backpressure 报告、16-binding
  drain 序、§4 所有 invariant 现观察值。产出 `tests/test_provider_task_characterization.py`（pinned）。**无 char 即静默破坏。**
- **M2.1 — ProviderTaskSpec registry（数据层，零执行变更）— 最小**：抽 frozen `ProviderTaskSpec` + registry，
  **先只形式化已存在的 `operation_native_profile_fetch` 血脉**（行为不变），golden sha1 钉表（类比
  `test_command_type_specs`）。**R1 go/no-go spike 在此内**（见 §7）。
- **M2.2 — 单一 RetryContract + durable backoff**：首迁 **seed_discovery 零结果重试**（崩溃不安全、价值最高、
  范围最小）：inline sleep → durable `not_before_at`，证等价 + 崩溃安全。
- **M2.3 — ProviderScheduler gate**：三层并发/预算折叠成单一 gate；删 `defer_provider_submit`，改 durable defer；
  **严格保留 I1-I6**（characterization 守）。M2「唯一 owner」承诺兑现点。
- **M2.4 — 首个真 provider 收编：Harvest profile_batch**：收 `enrichment.run_linkedin_profile_refill_submit_
  command_once` 的 bypass，复用 profile-fetch 命令族 + POLL_CANCEL_QUARANTINE + webhook/poll；**refill 双状态机合一**。最大单步。
- **M2.5 — DataForSEO serp_batch 收编**：task_key identity + 100/req + MAX_ITEM_RETRIES 进 RetryContract（不改默认值）。
- **M2.6 — webhook-vs-poll 统一为 EventDeliveryStrategy**（§6 决策 C）：`{primary, fallback, dedup_window}`，统一 terminal-event 去重（I10）。
- **M2.7+（非首发）**：LLM/html_search 收编、legacy connector/bridge 删除（绑 Phase 4 cutover，§6 决策 D）。

---

## 6. Owner 决策点（真分叉 + 推荐）

- **决策 A — async_task_contract 并入 durable 基底 vs 分离？** 推荐 **保持分离**（envelope vs substrate）；但
  **M2 接管 idempotency_key 碰撞检测**（今天 async_task_contract 只 pass-through），envelope 不变。
- **决策 B — 现在统一所有 provider vs 先迁一个？** 推荐 **先迁一个**（seed_discovery 零结果 → 然后 Harvest
  profile_batch；先做一 search 类 + 一 actor 类证明抽象，再批量）。**M2 应 opt-in**：新 provider 走 M2，legacy 迁移期暂留。
- **决策 C — webhook-vs-poll 统一策略？** 推荐 **webhook-primary + bounded local-watcher fallback + 统一 dedup
  window**（建议 5min grace post-terminal），保留 I10，放 M2.6 不阻塞前期。
- **决策 D — 删 legacy 多激进？** 推荐 **保守**：早增量只删「纯冗余 + 崩溃不安全」（inline sleep / defer flag /
  重复常量）；legacy bridge flags / 直 HTTP connector 删除绑 Phase 4 cutover（动 Phase 4 cancel/resume golden +
  governance 迁移契约——属 cutover 决策，非 M2 leaf 清理）。
- **决策 E — M2 是否抬高 8 槽 HTTP 天花板？** 推荐 **不抬高（M2 范围内）**：8 槽护 HarvestAPI 隐藏上限（I1）；M2
  只把重活 wall-clock 移出槽。HTTP 入口并发与 provider 并发解耦留给后续里程碑（需 load test 证 HarvestAPI 真实上限）。

---

## 7. 风险 / 未知（需 spike）

- **R1 抽象漏判（go/no-go，放 M2.1 内）**：ProviderTaskSpec 是否覆盖四类 provider 差异？在 M2.1 用
  `operation_native_profile_fetch` + DataForSEO 两个最异质 provider 各填一份 spec；字段集打架 → 抽象需调整。**先于任何执行迁移。**
- **R2 refill 双状态机合一并发正确性（I12）**：store 升 source-of-truth 时 planned_dispatch 持有/释放 vs 并发
  recovery 的原子性；`_profile_prefetch_reserved_or_owned_worker_count` 不回归。需并发 reclaim 竞态 spike。
- **R3 全局信号量缓存失效**：`_GLOBAL_INFLIGHT_SEMAPHORES` 缓存 `(lane,budget)` 即复用；budget 变更不重建——
  ProviderScheduler 内化须定缓存失效策略。
- **R4 durable backoff vs tick budget 交互**：inline sleep 改 `not_before_at` 后重试项重进 recovery tick 队列——
  不得与 16 drain binding 抢 tick budget/lease（I17/I19）。
- **R5 characterization 完整性**：四循环 retryability 判定极细（DataForSEO 40800/42900/≥50000、
  HarvestRetryableRequestError、model HTTPError）；M2.0 钉不全 → 迁移后静默改 retry 行为。
- **R6 scripted/offline 隔离（I21）**：M2 统一 submit gate 后须保证 simulate/replay/scripted 仍不发 live API。
- **U1（未知，非 M2 范围）**：8 槽与 provider-type budget 解耦后 HarvestAPI 真实上限——决策 E 前提，需 load test。

---

## 8. 请 owner 批准的最小集
1. **§6 决策 A-E** 各裁定。
2. **§5 增量序**（M2.0 characterize → M2.1 spec registry → M2.2 单 provider durable backoff → M2.3 scheduler →
   M2.4 Harvest → M2.5 DataForSEO → M2.6 event-delivery）+「最小先行 + opt-in + legacy 删除 defer」是否接受。
3. **R1 go/no-go spike 放 M2.1 内**（两异质 provider 填 spec 验证抽象）先于任何执行迁移，是否同意。

批准后方进入实现。本提案只设计、不改码。

---

## 9. R1 spike 结果（go/no-go）— **GO，带 3 处 spec 精化**（2026-06-16，已读码）

用两个最异质 provider 各填一份 ProviderTaskSpec：**Harvest profile fetch**（既有
`LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE`，`durable_runtime.py:261-272`，provider_attempt
+ POLL_CANCEL_QUARANTINE）vs **DataForSEO serp_batch**（`search_provider.py` 三相）。核心 shape
（typed spec / 单 scheduler / durable backoff / 绑 CommandTypeSpec / 以 profile-fetch 血脉为模板）**成立**。
但 §2.2 字段集在 3 个轴上「打架」，**M2.1 冻结 registry 前须精化**：

1. **`submit_mode` 须能表达 sync→async 复合回退**。Harvest 先 `run-sync-get-dataset-items`
   （`harvest_connectors.py:4229`），失败才转 async submit→poll→fetch（`:4258`）；DataForSEO 是纯
   `batch_submit_poll_fetch`。单 enum 不够 → submit_mode 改为有序策略元组（如 `("sync_run",
   "async_submit_poll")`）或新增 `sync_run_with_async_fallback`。

2. **retry 不是统一 `{max_attempts, backoff}`——有两种 granularity + 一种 reshape**：
   - Harvest = **work-reshape retry**：把未解析 URL 重新切成递减批次（50→25→10→5，`:699-720`）+
     单 URL direct fallback（`:718`）。retry 单元是「未解析子集」，重新分批。
   - DataForSEO = **per-item retry**（MAX=1，`:1124`），与 batch 无关。
   → RetryContract 须加 `granularity` 轴（`batch` | `item` | `work_reshape`），work_reshape 引用一个
     reshape-ladder 函数名。扁平 `{max_attempts, backoff_strategy}` 无法表达 batch-shrink 阶梯。

3. **「fallback」一词被超载在 ≥3 个轴上**，单 `fallback_chain`（provider 降级）无法表达：
   - Harvest **submit-strategy fallback**（batch → 单 URL direct，`:718`）；
   - DataForSEO **readiness-mechanism fallback**（`tasks_ready` poll → direct-probe 逐 task ThreadPool
     fetch，`:1370-1392`）；
   - provider-downgrade chain（字段当前建模的那个）。
   → 拆成三轴：`submit_fallback`（归入 submit_mode）/ `readiness_fallback`（归入 readiness_signal）/
     `provider_fallback_chain`（降级）。**不要混为一谈。**

**结论**：GO。R1 没有否定设计——它精确告诉我们 M2.1 的 ProviderTaskSpec 必须把 **submit 策略（含 sync→async +
work-reshape）、retry granularity（batch/item/reshape）、三类 fallback** 显式分离。M2.1 即用此精化后的字段集建
registry，并以这两个已验证 provider 各填一份作为首批 golden。其余增量（M2.2+）不变。

> 下一步（M2.1）：用精化字段集落 `ProviderTaskSpec` + registry（先形式化 profile-fetch 血脉 + DataForSEO 两份），
> golden sha1 钉表；零执行变更。M2.0 characterize 与之并行/前置。

---

## 10. M2.1 DONE + M2.2 重新定界（2026-06-16，characterize-first 修正）

**M2.1 已交付（commit acfeed3，CI 绿）**：`src/sourcing_agent/provider_task_runtime.py` —
frozen `ProviderTaskSpec` + `RetryContract` + `DEFAULT_PROVIDER_TASK_SPECS`（2 份：`harvest.profile_batch`→
`LINKEDIN_PROFILE_FETCH_PROVIDER`、`dataforseo.discovery_query`→`LINKEDIN_DISCOVERY_QUERY_RUN`），R1 精化字段集，
纯数据。`tests/test_provider_task_specs.py` golden sha1 + 结构守卫（每个 command_type 已注册且为 provider_attempt、
work_reshape 须带 ladder、R1 三轴保真），入 CI lane。

**M2.2（migrate seed_discovery 零结果 inline sleep → durable not_before_at）= MOOT，已重新定界。**
characterize-first 核验发现 **seed_discovery 没有任何 `sleep` 调用**——其 zero-result / retryable-failure 重试
**早已是 durable not_before_at 模式**（`seed_discovery.py:1833` `_utc_timestamp_after_seconds` → status=
`failed_retryable` / phase=`retry_wait` / `not_before_at`，由 recovery 在到点后重新 claim；`:412`
`CommandPlanRequested` 重排）。且该契约**已被现有测试钉死**：`test_seed_discovery.py:2767`
`test_provider_people_search_retryable_failure_enters_discovery_query_retry_wait`（断言 status/phase/not_before_at）。
即：M2.2 想引入的崩溃安全 durable backoff，在 seed_discovery **已存在且已测**——无 inline sleep 可删。
（初始 workflow 图谱把它误标为 inline-sleep；这是图谱误差，characterize-first 捕获。该 retry 测试目前也在
**已知 genuine 失败集**内 `discovery_items` 空——属 mid-refactor debt，**不在 M2 范围**、不在 curated CI lane。）

**真正残留的 inline sleep**（harvest_connectors 7 / enrichment 3 / connectors 1 / public_web_search 1）**都在
同步 provider 调用内部**（Apify run-status poll loop、分页、lease poll）——把它们改 durable 需要 provider-task-
on-substrate 的完整迁移，即 **M2.3 ProviderScheduler + M2.4 Harvest 收编**，而非一个独立的 M2.2。

**重新定界结论**：**M2.2 折叠进 M2.3**（durable-backoff 模式已由 seed_discovery + operation_native_profile_fetch
两处证明 + 测试；RetryContract 已在 M2.1 形式化）。下一真实步骤 = **M2.3 ProviderScheduler**（把 §1.3 三层并发/
预算折叠成单一 gate + 删 `defer_provider_submit` + durable defer），保留 I1-I6。这是一处实质执行变更，宜独立、
characterize-first 起步。

---

## 11. M2.3 — characterize 完成 + 「fold」重新定界为 INADVISABLE（2026-06-16，characterize-first 第三次纠偏）

**M2.3 characterize 已交付（commit 132b937，CI lane）**：`tests/test_provider_budget_characterization.py` 钉
I3（5 个 per-lane inflight budget 默认值 + env 优先级）+ I6（`build_provider_backpressure_budget_report` 的
recommended_action 三态转移）。纯函数、零行为变更。

**M2.3 的「fold」（把三层折叠成单一 gate + 删 `defer_provider_submit` → durable defer）= INADVISABLE**，
characterize-first 读码核验推翻了它的三条前提：

1. **per-lane 并发已被强制执行，不是 advisory。** `runtime_inflight_slot`（`runtime_tuning.py:705`）是一个
   **真正阻塞的 `BoundedSemaphore.acquire(blocking=True)`**（含同线程重入），被**每一条真实 provider 路径**消费
   （harvest_connectors / enrichment / seed_discovery / acquisition / candidate_artifacts / snapshot_materializer /
   orchestrator）。它就是 §1.3「runtime inflight 层」——而且**已经是那个 enforced 单点 gate**。
2. **backpressure report 是 observability-only。** `build_provider_backpressure_budget_report` 的**唯一**消费者是
   `workflow_smoke.py`（smoke/可观测），**不在 serving 提交路径**。即:不存在「算了 recommended_action 但不强制」的
   serving 缝——`recommended_action` 从不进 orchestrator 提交决策。设计 §1.3 把它当成待修的脆弱缝是图谱误判。
3. **`defer_provider_submit` 已是 durable。** 它不是崩溃不安全的 advise flag，而是「completion callback 不自己提交、
   交给 runtime-owned refill daemon」的**所有权**标志（`orchestrator.py:64231-64237` 文档；路由到 durable refill
   daemon）。无 inline-sleep / 丢失语义可修。

**结论（durable-foundation 原则）**：重写/替换这套**已强制执行、已重入、已正确**的 load-bearing 并发 gate
（保护 HarvestAPI 隐藏 ~8-actor 上限）= **高风险 churn、零行为收益**——正是不该做的改动。M2.3 的真实交付 = 上面的
characterization（已完成）。「单一 ProviderScheduler」若要存在，应是 M2.4 接入第一个真实 caller 时的**薄 facade**
（`ProviderTaskSpec.inflight_budget_key` getattr→runtime_tuning resolver → 复用既有 `runtime_inflight_slot`），
**由真实 caller 塑形、加 facade 不重写底层**，而非现在凭空建一个无人调用的 gate（YAGNI）。

**重新定界**：M2.3「fold」**不做**（前提不成立）。M2 真正剩余价值集中在 **M2.4 — Harvest 收编**：把 harvest
provider-call（含 §1 残留的 7 处 poll-loop inline sleep）接入 `harvest.profile_batch` ProviderTaskSpec + 既有
`operation_native_profile_fetch` 命令族，届时按需建薄 ProviderScheduler facade。这是下一真实步骤。

> characterize-first 三次纠偏累计结论:初始 workflow 图谱系统性高估了碎片化/advisory 程度——真实代码比图谱所述
> **更 durable、更 enforced**。这是好消息(地基稳),并把 M2 价值重定向到 M2.4+(接真实 provider call 进 typed spec
> + durable-ify 真正的 poll-loop inline sleep),而非重新铺设已稳的并发层。

---

## 12. M2.4（Harvest 收编）— LARGELY MOOT + 修正一处真实 M2.1 误绑（2026-06-16，characterize-first 第四次）

**决定性 crash-safety 裁定:harvest fetch 进程重启时 RESUME,不丢。** 生产 refill/prefetch 路径是 code-verified
checkpoint-resume 状态机:`_execute_harvest_actor_with_checkpoint`（`harvest_connectors.py:3682-3731`)单步提交后
**立即返回 `pending=True` + run_id/dataset_id 入 checkpoint**(不 inline poll);caller `_execute_harvest_profile_
batch_worker`(`enrichment.py:7526-7594`)把 run_id/dataset_id 经 `complete_worker(status="queued")` →
`store.checkpoint_agent_worker` → `UPDATE agent_worker_runs`(`storage.py:12451-12482`)写入 **substrate**,**在等待之前**;
重启时 `enrichment.py:6403-6442` 重载 checkpoint、`resume_remote_run=True`、跳过 submit 直接 poll 既有 run。即
设计「plans durable command 但 off-substrate 执行」之说被代码推翻。

**7 处 inline sleep = 非问题。** 5/7 已 durable(poll-interval/scripted/zero-result-retry,均在 deadline-bounded 或
checkpoint-advanced 循环内);另 2 处(dataset page-fetch retry `:4018`、dispatch-guard spinlock `:5986/5990`)在
**`_run_harvest_actor` 这条不同路径**上(非 durable batch worker `_execute_harvest_actor_with_checkpoint`),且被 durable
worker 幂等重入覆盖——属效率细节,非 durability gap。

**真实发现(已修):M2.1 一处误绑。** `LINKEDIN_PROFILE_FETCH_PROVIDER` 命令族(`profile_fetch_owner.py:987-1037`)
**调的是同步 RapidAPI `LinkedInProfileDetailConnector.fetch_profile`,不是 Apify Harvest actor**;Apify-harvest-actor
batch 实际属 `LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE`(`enrichment.py:7355` `execute_batch_with_checkpoint`)。
M2.1 把 `harvest.profile_batch`(provider_family=apify_harvest / sync_run→async / work_reshape ladder)误绑到了 RapidAPI
那条命令。**已修(commit 见下):`harvest.profile_batch.command_type` → `LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE`**,
golden 重算,并加 cross-check `test_after_start_mode_matches_bound_command` + `test_harvest_binds_the_apify_harvest_batch_command`
(spec↔command 一致性守卫,正是能抓住此类误绑的断言)。

**裁定:M2.4-as-designed(收 bypass + durable-ify 7 sleep + merge 双状态机,「最大单步」)= largely MOOT**(第四次纠偏)。
不做收编(harvest fetch 已 substrate-backed + recovery-safe;命令族走 RapidAPI 同步、re-run 幂等/缓存去重);不 durable-ify
7 sleep(非关键路径);不 merge refill 双状态机(I12 race 未证实、store 已 durable+atomic,merge 反增 I12 风险)。
**真实交付 = M2.1 误绑修正 + spec↔command cross-check。** thin ProviderScheduler facade 仍 YAGNI(无真实新 caller)。

**下一真实价值 = M2.5(DataForSEO serp_batch)**:design R1 标的**异质** provider(batch_submit_poll_fetch、per-item retry、
100/req、retryable codes 40800/42900/≥50000),其 durability/spec-binding **尚未** characterize 验证——是抽象真正受检、
真实价值所在之处。Harvest 已完;停止打磨。

> characterize-first 四次纠偏(M2.2 moot / M2.3 fold inadvisable / M2.4 moot)累计强信号:**M2 的「把 provider call 收上
> substrate」前提大体不成立——substrate 早已 durably 拥有 provider 调用**。M2 真实价值 = (a) registry 作诚实文档(M2.1,
> 已含本次误绑修正);(b) 逐 provider 验证 spec↔reality(harvest 已验,DataForSEO = M2.5);(c) 可选 M2.6 event-delivery。
> 「重铺 runtime」式框架被代码证伪。建议 M2.5 后重估 M2 是否还有 M2.6 之外的实质工作。

---

## 13. M2.5（DataForSEO 验证）+ M2-COMPLETE 评估（2026-06-16）

**M2.5 裁定:dataforseo.discovery_query 的 command 绑定 ACCURATE(与 harvest 误绑相反);behavioral 字段已验证;
ref 字符串已修正诚实。**

- **绑定正确**:`LINKEDIN_DISCOVERY_QUERY_RUN` 的 owner(`linkedin_acquisition_owner`)经注入的
  `search_provider`(`build_search_provider(settings.search)`,`acquisition.py:711`)运行;DataForSEO
  (`DataForSeoGoogleOrganicClient`,`provider_name="dataforseo_google_organic"`)是其可配置 provider 之一,走
  3-相 batch(`seed_discovery.py:2853` `submit_batch_queries` → ready-cache refresh → fetch)。**nuance:该命令
  provider-polymorphic**(default `DuckDuckGoHtmlSearchProvider`),故 spec 是「DataForSEO 行为视图」,寄宿在共享
  discovery 命令上——与 harvest(误绑到从不调 harvest 的 RapidAPI 命令)本质不同,**无需改绑**。
- **behavioral 字段已验证**:`batch_submit_poll_fetch` ✓、`readiness_fallback=direct_probe`(tasks_ready→ThreadPool
  probe)✓、retry `granularity=item` / `max_attempts=1`(`DATAFORSEO_BATCH_ITEM_RETRY_COUNT` default 1)✓、
  100/req(`MAX_TASK_POST_BATCH_SIZE=100`)✓、retryable codes 40800/42900/≥50000 ✓。
- **crash-safety**:DataForSEO 3-相的 task_key/search_state 写入 manifest + discovery item 的
  `prefetched_search_state`(寄于 durable `LINKEDIN_DISCOVERY_QUERY_RUN` 命令)→ 重启可 resume,与 M2.4 一致。
- **ref 字符串修正(M2.5 + 顺修 harvest)**:`retryable_classifier` → 真实 `_dataforseo_error_message_retryable`
  (`search_provider.py:85`,原 `_dataforseo_retryable_status_code` 不存在);dataforseo 的 inflight/cost/backoff key
  与 harvest 的 `cost_budget_key`(`resolved_harvest_profile_lane_budget_cap` 不存在)均为 M2.1 投机性 forward-ref
  →置 `""`(dispatch-time TBD;discovery 并发由 worker search-lane cap `resolved_lane_budget_caps` 治理)。新增
  **self-verifying guard** `test_budget_keys_resolve_in_runtime_tuning`(非空 budget key 必须是真实 runtime_tuning
  resolver),防再现投机 ref。golden 重算。12 tests 绿。

---

## M2-COMPLETE 评估(grounded)

**M2 实质已完成**,交付 = **typed ProviderTaskSpec registry(诚实 + self-verifying 文档) + 两 provider 的
spec↔reality 验证**。逐项:

| 增量 | 结果 |
|---|---|
| design / R1 | 批准 + GO(带 3 字段精化) |
| **M2.1** registry | 已建(frozen spec + RetryContract + golden);经 M2.4/M2.5 修正后**绑定与 ref 均诚实** |
| **M2.2** seed_discovery durable backoff | MOOT(早已 durable not_before_at + 已测) |
| **M2.3** ProviderScheduler fold | INADVISABLE(`runtime_inflight_slot` 已 enforced/reentrant;backpressure report 仅 observability;`defer_provider_submit` 已 durable)— 仅交付 I3/I6 characterization |
| **M2.4** Harvest | 收编 MOOT(已 checkpoint-resume crash-safe);**修正真实误绑** |
| **M2.5** DataForSEO | 绑定 ACCURATE;behavioral 已验证;**修正投机 ref + 加 resolver guard** |

**核心结论(4 次 characterize-first 纠偏的累计):M2 设计的「provider 调用碎片化/崩溃不安全、需收编上 substrate」前提
被代码证伪——durable command substrate + checkpoint-resume(`agent_worker_runs` / discovery item)+ enforced
`runtime_inflight_slot` 早已 durably 拥有 provider 调用。** 故 M2 不是一次「重铺 runtime」的 build,而是「钉死 + 验证 +
诚实文档」——这部分已做完。

**M2.6(webhook-vs-poll 统一)= 很可能也 moot**:M2.4 已证 harvest 的 webhook-primary + poll-fallback + terminal-event
dedup(I10,terminal-event 写先于 dispatch)已 crash-safe 运作;「统一」多为形式化。建议 M2.6 仅作一次轻量验证(若发现
真实 dedup/投递缺陷再做),否则**判定 M2 完成**。

**建议**:判定 **M2 实质完成**。下一里程碑应是别的实质 build(per [[project-direction]]:Track B PG-pure store 重写 /
Track D agentic streaming),而非继续打磨 M2。M2 的持久价值 = `provider_task_runtime.py` registry 作为「每个外部 provider
如何以 durable task 运行」的**单一、已验证、self-verifying 事实源**。

---

## §14 — M2.6 收尾验证(VERIFIED-MOOT,非假设)

按 owner 指示「先验证 M2.6 再宣布 M2 完成」,对 harvest 的 **webhook-primary + poll-fallback + terminal-event dedup**
投递做了 read-code 全链路验证(非 M2.4 时的旁证判断)。结论:**该投递设计 crash-safe,且已被既有测试全面钉死;不存在
需要修补的真实缺陷。** M2.6 的「统一」是形式化的,**判定 verified-moot**。

**投递链与 crash-safety 设计(代码事实):**
1. webhook(`POST /api/providers/apify/webhook`,`api.py:1558`)、sync-recovery、local-watcher poll 三入口**全部汇聚**到
   `orchestrator.handle_remote_provider_event`(`orchestrator.py:38018`)——单一去重/投递点。
2. terminal 事件先**写 marker 再 dispatch recovery**(`_mark_remote_provider_terminal_event_on_workers`,38300 →
   recovery dispatch 38320):marker 写 `force_scripted_terminal_fetch=True`、保留 worker `status=queued`(**非 completed**)
   → marker 含义是「terminal 已见,去 drain 做 scripted terminal fetch+admit」,**不是「已完成」**。
3. **handoff 契约**(`workflow_event_response.py:288` docstring):**无 marker 的 remote-wait worker 归 webhook/watcher
   路径所有;有 marker 后归 generic recovery daemon 所有。** 故 crash-after-mark 落在 daemon 域内,daemon 独立 drain 完成——
   事件路径的去重(对已标记 worker 抑制重复 recovery)之所以安全,正因 daemon 拥有 drainage。
4. admission 期间 crash 从 partial-persist checkpoint resume(`stage=persisting_terminal_harvest_profiles` +
   `terminal_persist_progress`),`_consumed` 幂等。

**已钉死的覆盖(既有,跨两文件):**
- ordering(mark 先于 recovery)→ `test_remote_provider_events.py::..._marks_terminal_checkpoint_before_recovery`
- 事件去重(重复 webhook / webhook+watcher 不重复 recovery、不重开 durable queue)→ `..._does_not_rewake_checkpointed_terminal_event`、
  `..._dedupes_recoverable_worker_with_terminal_marker`、`..._late_duplicate_does_not_reopen_durable_queues`、`..._records_late_watcher_event_after_webhook_completion_without_recovery`
- handoff 完成端(有 marker 的 worker 被 daemon claim→execute→**completed**)→
  `test_worker_recovery_daemon.py::test_terminal_remote_event_marker_allows_remote_wait_recovery_owner_to_resume`
- handoff 方向(无 marker 的 remote-wait **不**被 generic recovery 轮询)→ `..._non_explicit_submitted_remote_wait_is_not_polled_without_terminal_event`
- admission 期间 crash 可立即 resume → `..._terminal_profile_persist_stage_is_immediately_recoverable_after_partial_yield`

**验证中发现并修补的一处潜在脆弱点(本次唯一新增工件):** 该 handoff 由**两个不同的 marker 检测函数**把守——事件去重用
`orchestrator._worker_has_remote_provider_terminal_event`(仅 terminal),daemon 归属用
`workflow_event_response.worker_has_remote_provider_terminal_event_marker`(更宽)。被事件路径去重的 worker 之所以不会
stranded,**完全依赖 daemon 检测是事件检测的超集**(daemon-positive ⊇ event-positive)。当前代码满足该超集关系(任一让
`_worker_has...` 为真的 worker,其 checkpoint 必含 `remote_provider_terminal_event` dict,从而 `..._marker` 也为真),但此跨模块
不变量此前**只是隐式、无测试守护**——若将来有人把 daemon 检测收窄到事件检测之下,被去重的崩溃 worker 会**静默 strand**。新增
`test_terminal_marker_handoff_is_strand_safe_daemon_check_superset_of_event_dedup`(纳入 CI 合同 lane)钉死该方向的不变量:
对真实写入的 marker 形状及一组 worker shape 矩阵,断言 `event-positive ⟹ daemon-positive`,并断言「daemon 更宽」这一安全方向确实
存在。这是 M2.6 的唯一实质交付——验证 + 一个保护 stranding 缝的小不变量,而非假造一个「统一」build。

**最终判定:M2 完成(M2.1–M2.6 全部 settled)。** 下一里程碑交 owner 选(Track B / Track D)。
