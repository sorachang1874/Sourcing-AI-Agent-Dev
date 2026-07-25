# Recovery Takeover Intent — Durable 设计（Option B）

> **HISTORICAL — IMPLEMENTED（已落地，勿按本文操作）**：Option B 已实现——`workflow_recovery_intents` 表由版本化迁移 `src/sourcing_agent/migrations/0001_baseline.sql` 创建，typed store 方法在 `storage.py` / `control_plane_live_postgres.py`，`workflow_takeover_intent_drain` phase 在 `orchestrator.py`（已 pin 进 `tests/test_recovery_tick_characterization.py`）。§2/§6/§7 的 dual-path 实施机制（storage.py SQLite literal DDL、guard replay）已随 Track B B4.3 退役：storage.py 现为 PG-pure，schema 唯一来源是版本化迁移 runner。当前事实以代码为准。
>
> Status: Design for owner-ratified Option B (2026-06-14). Phase 4 Step 5e read-path takeover via a durable recovery-takeover-intent.

适用分支 `governance-phase0-ttl-20260611` @ `62e0149`。本文件只是设计稿（READ-ONLY 调查产物），不含任何代码改动。所有 file:line 引用基于 `src/sourcing_agent/` 当前快照。

---

## 1. 问题与决策

read-path 的「立即接管 *这个* 已分类为 dead-runner 的 job」意图，目前被硬塞进 shared daemon 的 **coalesced scalar wake file**。两次 async Codex NO-GO 证明这是结构性破裂：

- **F1（注入）**：`_signal_shared_recovery_wakeup`（orchestrator.py:2355）把调用方 `payload` 里的 8 个 bounded-recovery 控制字段 forward 进 `callback_payload`（forwarding 循环 2400-2412）。该 payload 最终被 daemon 经 `_workflow_recovery_settings`（orchestrator.py:37418）消费。问题在于这些 recovery 控制（`workflow_stale_scope_job_id` + 两个零 stale 阈值）属于**内部 recovery 调度语义**，一旦它们可由请求 payload 携带，就等于把内部接管控制暴露给不可信的 `/api/workflows` 请求体注入。
- **F2（clobber）**：wake file 是 **coalesced scalar**（last-non-None-writer-wins，经 `_merge_service_callback_payload`）。一个 pending 的 scoped takeover（带 `workflow_stale_scope_job_id`）与随后一个 generic 全局 nudge（scope 为空）共用同一标量槽：后写者会把前者收窄/抹掉，或反之。两个 intent 互相覆盖。

**Owner DECISION（2026-06-14）= Option B**：把 **NOTIFICATION** 与 **INTENT** 彻底分离。wake file 退回为纯「wake now」nudge；接管意图改为 **durable 的 per-job recovery-takeover-intent**，由 read path 经 typed store 方法写入、由 daemon tick claim/consume。遵循 owner 的解耦/durable 原则：建**架构正确的 durable 基座**，不是在不合理设计上反复打补丁。

---

## 2. 载体选择

**推荐（1st）：新建专用表 `workflow_recovery_intents`，`job_id TEXT PRIMARY KEY`。** 复用既有 primitive 全部被否决：

- `runtime_outbox` — grain 是 per-event append-only（`INSERT OR IGNORE` first-write-wins），身份列是 `workflow_run_id` 单向哈希（无法反推 daemon 的 `job_id` scope）。要承载 per-job upsert-latest 等于重建它，且重新混入它正是 wake-file 同类的关注点耦合。
- `workflow_commands`（DDL control_plane_live_postgres.py:5121）— 有最好的 lease/claim 状态机，但 grain 错（`(workflow_run_id, idempotency_key)` 多命令、claim 是 lease 非 consume-clear）。
- `jobs` / `job_result_lifecycle` 行内加字段 — 最轻，但**违反解耦原则**：domain 请求行 / serving 投影行不是 control-plane recovery 面，把 daemon-claimed 控制字段塞进去会把 recovery 调度耦合到这些行的写路径与锁竞争 = band-aid。

**为什么新表正确**：grain 恰为「每个 job 一条逻辑 pending intent」。`job_id PRIMARY KEY` → read path 经 `ON CONFLICT(job_id) DO UPDATE` upsert-latest-wins；daemon 经条件 UPDATE 单赢 claim/clear；durable PG 行抗 crash、跨进程可见。形状与 `workflow_job_leases`（storage.py:2341，`job_id TEXT PRIMARY KEY`）一致 —— daemon 已在 job_id grain 上操作 leases，intent 表自然紧挨它。

**最小 schema（写入 storage.py 的 literal DDL，让 guard 的 `derive_sqlite_table_schemas` replay）：**
```
CREATE TABLE IF NOT EXISTS workflow_recovery_intents (
    job_id TEXT PRIMARY KEY,
    classification TEXT NOT NULL DEFAULT '',     -- 例: 'runner_not_alive'
    status TEXT NOT NULL DEFAULT 'pending',      -- pending | claimed | consumed
    requested_at TEXT NOT NULL DEFAULT '',
    requested_by TEXT NOT NULL DEFAULT '',
    params_json TEXT NOT NULL DEFAULT '{}',      -- 8 个 bounded-recovery 参数（见 §3）
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT NOT NULL DEFAULT '',
    claimed_at TEXT NOT NULL DEFAULT '',
    schema_version TEXT NOT NULL DEFAULT 'workflow_recovery_intent_v1',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```
- 用单个 `params_json` 而非 8 列：参数是被 `_workflow_recovery_settings(payload)` 整体消费的 opaque 契约，JSON blob 保持 claim 路径 schema 稳定、避免 8 列 churn。`classification`/`status`/`requested_at`/`lease_*`/`claimed_at` 留作一等列（驱动 claim 谓词与可观测性）。`workflow_stale_scope_job_id` 与 PK `job_id` 冗余 —— claim 时派生，不双存。

**PG-bootstrap / guard 义务（新表关键结论）**：唯一性是 SQLite literal DDL 里声明的**单列 PRIMARY KEY**，由 guard 的 `derive_pg_bootstrap_unique_sets` path 1（test_pg_onconflict_guard.py:219-221，`_build_create_table_sql` 把 SQLite PK 携带为 PG PK）自动覆盖。因此：
- **不需要** `_CONTROL_PLANE_UNIQUE_INDEXES` 条目，也**不需要** `_CONTROL_PLANE_UNIX_INDEX_DEDUPE_RECENCY_SQL` —— 那些只为 snapshot 路径会丢弃的 `UNIQUE(...)` 约束（criteria_patterns/job_result_views 缺陷类）准备。裸 `job_id TEXT PRIMARY KEY` 不属于该 gap class。
- **必须做**：(a) 把 literal `CREATE TABLE IF NOT EXISTS workflow_recovery_intents (...)` 加进 storage.py（dual-path SQLite 侧）；(b) 在 control_plane_live_postgres.py 加对应 PG literal DDL 并在三处 registry 注册该表名 —— 复制 `workflow_job_leases` 先例：replicated table 列表（:82）、primary-key registry `("job_id",)`（:169）、`_RUNTIME_COORDINATION_TABLES`（:234）。

---

## 3. 写入（read path → 解决 F1）

写入点在 `_maybe_auto_recover_workflow_on_progress`（orchestrator.py:36559）构造 scoped `recovery_payload` 之处（36631-36650），落地于 `_run_progress_auto_takeover`（36780）—— 把其中 `_signal_shared_recovery_wakeup(..., payload=recovery_payload)` 调用（36795-36801）替换为一个 **typed store 写**，例如 `store.upsert_workflow_recovery_intent(job_id=..., classification=..., params=...)`，keyed by `job_id`，`ON CONFLICT(job_id) DO UPDATE`。

**F1 结构性解决（内部路径证明）**：route 是 `GET /api/jobs/{job_id}/progress`；handler 调 `orchestrator.get_job_progress(request.path_params["job_id"])`，`payload` **从不**被 forward 进 `get_job_progress` → `_maybe_auto_recover_workflow_on_progress`（`events=[]`）。`job_id` 是受 `sourcing_ident` converter 约束的 path token，只作 `store.get_job(job_id)` 查找键。8 个 recovery 控制全是 server-side **硬编码 literal**（36632-36650），不是请求字段。故任何 `/api/workflows` 请求体都无法注入 `workflow_stale_scope_job_id`/零 stale 控制 —— wake file 提供的不可信 forwarding 通道被消除。

**记录的参数**（`params_json`，即 `_workflow_recovery_settings` 消费的 8 项契约 + 派生 scope）：`workflow_resume_explicit_job=True`、`workflow_resume_stale_after_seconds=0`、`workflow_resume_limit=1`、`workflow_auto_resume_enabled=True`、`workflow_queue_resume_stale_after_seconds=0`、`workflow_queue_resume_limit=1`、`workflow_queue_auto_takeover_enabled=True`；scope = PK `job_id`（派生）。per-classification 的 `stale_after_seconds=0`（36650）一并记录。

---

## 4. 消费（新 recovery phase：drain takeover intents）

在 `run_worker_recovery_once` 中新增 phase `workflow_takeover_intent_drain`，**置于 generic `workflow_resume` phase 之前**（即 `post_event_level_profile_prefetch_refill` 与 `workflow_resume` 之间；`workflow_resume` 调用点 orchestrator.py:39379-39396，settings settle 38808-38827）。它作为 `CallbackRecoveryPhase` 经 `run_registry_phase`（recovery_phases.py:236）运行。

**claim/clear（无双处理）**：复用 `claim_job_materialization_item`（control_plane_live_postgres.py:2355）的 `UPDATE ... RETURNING *` 单赢先例 —— 原子地 `UPDATE workflow_recovery_intents SET status='claimed', lease_owner=%s, lease_expires_at=%s WHERE status='pending' AND (lease_expires_at='' OR lease_expires_at <= now) RETURNING *`。`RETURNING *` 保证两个并发 daemon tick 不会双处理同一 intent。对每条 claim 到的 `job_id`，执行 stale=0 scoped resume：调 `_resume_blocked_workflows_after_recovery(..., stale_job_scope_job_id=<claimed job_id>, stale_after_seconds=0, resume_limit=1)`。成功后标 `status='consumed'`（或删行）；失败则让 lease 过期重 claim（resume 以 job state 为键，幂等重接管安全）。其后的 generic `workflow_resume` phase 保持 DEFAULT stale 窗口（unscoped settings，38809-38827：空 `workflow_stale_scope_job_id` + env 默认 60s）。

**ORACLE 加性更新（明确：只加不弱化）**：新增 phase 会改变 `recovery_phase_metrics` 插入顺序，被 `CHARACTERIZED_PHASE_SEQUENCE` pin（test_recovery_tick_characterization.py:66-131）。**加性**插入一条 tuple `("workflow_takeover_intent_drain", "workflow_takeover_intent_drain")`，位置紧贴 `("workflow_resume", "workflow_resume_controller")`（line 87）之前。不删除/重排/弱化任何现有条目；`CHARACTERIZED_PHASE_NAMES`（:133）自动派生。若该 phase 在 summary 以自身 key 可见，则 `PHASES_ABSENT_FROM_SUMMARY`（:151-160）不动；若仅 metrics-only，则把其名加入该 set。oracle 仍逐字节 pin 所有先前 phase。

---

## 5. 纯 nudge 回退

`_signal_shared_recovery_wakeup`（orchestrator.py:2355-2428）**删除全部 recovery-control forwarding**：移除 `forwarded_payload` 循环（2400-2412）及 `payload` 参数的契约-forwarding 角色。`callback_payload` 收敛为恰好 `{"source": reason}`（+ 有 job id 时 `{"request_path_job_id": job_id}`，2387-2389）—— 一个不携带任何 `workflow_stale_scope_job_id` / stale / limit / enable 键的纯「wake now」nudge。

请求路径**保持不变**：`queue_workflow`（2430+，原 2232-2259 形态）继续 `payload=payload` 调用，该 payload 现仅在 control-forwarding 上被忽略；start-workflow 仍是纯全局 nudge。`_run_progress_auto_takeover`（36795-36801）的 intent-bearing 调用改为 §3 的 durable store 写。

**F2 解决**：intent durable 且 per-job（PK `job_id`），job A 与 job B 的并发 progress-poll takeover 各 upsert 自己的行，无共享标量槽，互不收窄。drain phase 独立 claim 每行（每行一次 stale=0 scoped resume）。generic nudge 不携带任何 recovery scope，故全局-sweep wake 与 scoped takeover 处于**两套机制**，结构上不可能 clobber。

---

## 6. 不变量与验证计划

- **Invariant 1（无重复 dispatch）**：claim 的 `UPDATE ... WHERE status='pending' ... RETURNING *` 单赢 + lease。两个并发 daemon tick 不会双接管同一 job；幂等重接管（lease 过期重 claim）因 resume 以 job state 为键而安全。
- **Invariant 3（已分类 dead-runner job 被及时接管）**：经 durable intent（stale=0 scoped resume）+ 纯 nudge 触发，而非等 generic 60s 默认窗口。intent 写入即 commit，daemon tick（5b event wake / 5c 30s backstop poll）在下一 tick drain。
- **F1 回归测试**：断言 `GET /api/jobs/{job_id}/progress` 不把请求 payload forward 进 recovery 控制；断言 intent 的 8 参数全为 server-side literal；断言 `_signal_shared_recovery_wakeup` 的 `callback_payload` 不含任何 recovery-control 键。
- **F2 回归测试**：并发为 job A/B 写 intent → 各自独立行存活、互不覆盖；scoped takeover 与 generic nudge 共存不互相收窄。
- **ORACLE 加性更新**：`CHARACTERIZED_PHASE_SEQUENCE` 在 `workflow_resume` 前新增一条 pinned phase（§4），其余逐字节不变。
- **characterize-first 排序**：先更新/运行 characterization oracle（确认仅加性），再落 phase 实现与 store/DDL 改动，最后 F1/F2 回归。dual-path（SQLite literal DDL + PG literal DDL + 三处 registry）+ `test_pg_onconflict_guard` 全绿。

---

## 7. 实施步骤 + 非目标

**步骤**：
1. storage.py 加 `workflow_recovery_intents` literal DDL（SQLite 侧）；control_plane_live_postgres.py 加 PG literal DDL + 三处 registry 注册（:82 / :169 / :234，复制 `workflow_job_leases` 先例）。
2. 加 typed store 方法 `upsert_workflow_recovery_intent`（read path 写，`ON CONFLICT(job_id) DO UPDATE`）与 `claim_workflow_recovery_intents` / `mark_workflow_recovery_intent_consumed`（daemon 单赢 claim/consume，仿 `claim_job_materialization_item`）。
3. characterize-first：先加性更新 `CHARACTERIZED_PHASE_SEQUENCE`（§4）并确认仅顺序加性。
4. 在 `run_worker_recovery_once` 注册 `workflow_takeover_intent_drain` phase（generic `workflow_resume` 之前），实现 claim→stale=0 scoped resume→consume。
5. 改写 `_run_progress_auto_takeover`（36795-36801）为 durable store 写；`_signal_shared_recovery_wakeup` 删除 forwarding 循环（2400-2412），收敛为纯 nudge。
6. F1/F2 回归 + dual-path guard 全绿。

**非目标**：cross-process LISTEN/NOTIFY 仍属 Track C，不在本设计范围。本设计提供的是 **durable-intent 基座**：它让当前 read-path 转换正确（解决 F1/F2），同时与未来 worker/API split 前向兼容 —— durable PG 行天然跨进程可见，Track C 接入时只需把「daemon 轮询 claim」升级为「NOTIFY 唤醒 + claim」，intent 表与 claim 语义不变。
