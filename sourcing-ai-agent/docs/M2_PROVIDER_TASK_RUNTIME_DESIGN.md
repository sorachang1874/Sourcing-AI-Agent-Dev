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
