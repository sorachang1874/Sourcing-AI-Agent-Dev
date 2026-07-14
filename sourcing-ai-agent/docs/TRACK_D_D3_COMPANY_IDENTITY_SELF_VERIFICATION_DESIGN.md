# Track D — D3 批级设计：公司身份自验证 loop（第一垂直切片）

> Status: Cross-model design input for owner review（v6 2026-07-13，作者 = Claude Fable 5；只设计、不改码）。
> **v6 修订**：v5（round-4 修复）基础上按 round-5 有效 artifact `runtime/reviews/20260713T133324Z_*`
> （NO-GO 8 findings）修订，并首次执行 `DESIGN_INVARIANT_CHECKLIST.md` 九类不变量自查
> （报告 = `TRACK_D_INVARIANT_SWEEP_2026-07-13.md`，8 blocker 已修、52 obligation 转实施义务）。
> 修订史与覆盖映射见 §10。
> 上层计划：`TRACK_D_AGENT_RUNTIME_PLAN.md` §2 D3。TD-2/5/6/7 裁决见上层 §5。
> **不依赖 model_native_search、不依赖 D0 tool-calling、不依赖 D2。**
> 实施前按 handbook 纪律重新 Scout（基准 HEAD `656b368`）。

## 1. 现状精确描述

**身份解析优先级**：manual override **前置 pass**（非 `resolve_company_identity` 内分支）→
runtime/builtin（读全局注册表）→ legacy slug（`connectors.py:219`）→ observed exact-match
（`resolver="observed_candidates_exact_match"`，medium，`:354-391`；`normalize_company_key` 剥全部
非字母数字，`X.AI/X-AI/xai` 碰撞、首个命中即胜）→ observed 模型判定（只收 same_company +
high/medium）→ heuristic slug（low）。

**执行期付费路径 + 既有文件持久化**：`acquisition.py:970-1051` `_resolve_company` 在「非 manual
override + 低置信 + 无 local asset」时（守卫 `:985-990`）search（`:4078-4100`，异常静默吞、
fail-open）+ 模型判定；成功后写 `identity.json` 并 upsert 全局文件注册表
`company_identity_registry.json`（`:1017-1031`）。**注册表的完整读写面（v4 按 round-3 证据扩充）**：
写入方不止 acquisition——`company_registry.py:492-539` 是另一个完整 writer，被
`asset_registration.py:160-166`、`cloud_asset_import.py:480-485`、`organization_assets.py:1716-1723`
调用；`company_registry.py:554-594` 也是 upsert writer（v3 误标为读族）；`:641-681` 独立重扫
`identity.json` 快照。注册表无 workspace、无版本、无 generation、无审计。

**plan review gate 与身份无关**：`required_before_execution` 只在四处置 True
（`plan_review.py:54/:58/:78/:82`）；低置信身份今天直接 `ready` 执行（`orchestrator.py:53102-53116`）。
W11 链 gate 恒 required（`acquisition_command_owner.py:1143-1152,1203-1223`）；**review session id
在 `plan_review.request` 命令执行时才生成**（`:1342-1383`，v4 依此修正时序）。plan 终审批准会
**立即**计划 `acquisition.plan.commit`（`orchestrator.py:43083-43149`），commit owner 只查
approved/ready（`acquisition_command_owner.py:1855-1867`）——v4 的审批解耦（§2.1）与 commit 前
置检查（§2.2）针对此。`skip_plan_review` 可绕过 legacy gate（`:52975/:53061`）。

**产品定义**：agent 检索并给证据，人只在歧义时裁决；起点比现状更严——低置信身份从「无人拦截」
变为「必须有人或已验证结果放行」。

## 2. 形态

### 2.1 命令、活动与审批（v4 修正 round-3 critical #3 的死锁）

- 验证 owner：`company_identity_verification`。命令：`company.identity.verify.evidence`（Tier-1）、
  `company.identity.search.expand`（Tier-2）、`plan_review.identity_result.apply`（归 plan review
  owner）。EntityDelta kinds：`company_identity_verified` / `company_identity_verification_superseded`。
- **控制政策按具体 provider 能力派生**（v4 收紧）：远程 poll 型 search provider 的 attempt =
  `poll_cancel_late_result_quarantine`；**同步 HTTP/browser 型 search、document fetch、模型 judge
  = `fail_closed_until_terminal`**（或 provider 注册声明的本地中断政策）——政策来自 provider
  capability 注册，不按步骤类型一刀切。
- **Tier-2 审批与终审解耦**（v4 核心修正——终审批准即刻 commit，不能兼任搜索预算授权）：
  - review session 增加**类型化部分决定** `identity_search_budget_grant`：人在 review 卡上可
    **只授予搜索预算**（默认额度 = TD-5 的 3 次，可配置）而不终审通过 plan。**grant 是一等 PG
    记录**（v5 补 round4#6 生命周期）：键 =（workspace_id, review_session_id,
    verification_intent generation, policy_revision）+ **不可变签发身份**（v6，R5#3）：每次授予
    = 新 `grant_id` + scoped 单调 `issuance_generation` + 授予事件 id——**终态 grant 永不复活**，
    再授予 = 新行；命令/attempt/扣减/transport 绑定**精确签发身份**（旧授予事件驱动的 stale
    Tier-2 命令不能消费新 grant）。状态 `active/revoked/exhausted/superseded/reconciled`
    （sweep 修正：补 superseded 入枚举，与转移规则一致）。**retry 余额规则**
    （v6 消解与上层"不重置"的矛盾）：intent supersession 时由 grant owner 命令原子做
    「supersede-with-transfer」——旧 grant→superseded、新 grant 携带**恰好剩余额度**（不清零、
    不回满）绑新 intent generation。**revoke 走单写者路径**（v6，R5#4）：cancel/重编译/终审等
    非 plan-review-owner 的转移经 事件→reducer→plan-review owner 的 grant 命令收敛，不跨 owner
    直写；**pre-transport 授权扩展**（R5#4，D3b 精化）：review session 必须先创建；其 canonical physical
    lineage 是 `coordination_plan_review_id = plan_review_sessions.review_id`，物理类型与
    `plan_review_sessions.review_id` 一致为 positive `BIGINT`；brownfield 允许 `NULL`，strict row 必须 `> 0`，
    禁止 `TEXT`/empty-string 编码。baseline review row 没有 workspace/scope，因此 strict 路径必须先经 private
    scoped review-session repository 从 server runtime + authenticated workspace 写入 review 的 namespace/mode/workspace/
    issuer/digest；同时固定 `creation_source_workflow_command_id/creation_source_event_id/creation_plan_id/
    creation_plan_revision/creation_plan_bundle_digest/creation_idempotency_key`。private
    `create_or_exact_replay_scoped_plan_review_session(...)` 以 `(scope_digest, creation_idempotency_key)` unique，
    只接受 closed `ScopedSessionCreateContext|ScopedSessionCommittedReplayContext`。create mode 在
    `scoped-session-bootstrap-lock-v1` 下 exact-validate `ScopedReviewSessionBootstrapAuthority` +
    `ScopedReviewSessionBootstrapReceipt` 对应的 source review-request OperationRun/command、immutable event、typed plan
    pins 与 authenticated workspace；它不要求 bootstrap 前已存在 ActivityAttempt，不使用 normal
    `ClaimAuthority`/`ClaimReceipt`、business fence 或 review id。specialized bootstrap Stage B 才在一个 PG UoW 创建
    claim-bound ActivityRun/ActivityAttempt、session、session-created event 与 source command terminal pair；
    stale/cancelled/cross-tenant 零写。collision 逐字段不等即零写，commit 后 crash/retry 由 credential-free replay
    返回同一 review id。OperationRun 只能锁该 session 后 exact-copy scope 并固定 pin review id，禁止从
    request/plan/gate JSON 或 ambient operation context 补齐。该值由 OperationRun 固定 pin、`workflow_commands` exact-copy，并由
    `ClaimAuthority`/`ClaimReceipt` 绑定；FK、lock key 与 canonical numeric encoding 必须全链一致，strict row 的
    missing/`NULL`/`0` 一律 fail closed。除 session+intent+grant 三活跃外，同谓词 fence 当前 OperationRun/WorkflowCommand
    非终态 + registry/scheduler 在 exact-command selection 后由 private factory 单次铸造的 one-use
    `ClaimAuthority`（selection generation/expiry/atomic consumed authority id）+ current `ClaimIdentity`
    （generation/token capability/control epoch）+ attempt + 精确 grant 签发；wrapper 启动时不得铸造或持有
    standing capability。transport 与 plan-review/human/recompile/cancel owner 先按 `d3-dispatch-v2` 的
    immutable scope tuple + `coordination_plan_review_id` 取得同一 monotonic-budget transaction advisory
    coordination lock（root intent 不参与 key），再依唯一全局顺序锁
    operation root → optional plan/review/gate → **all participating `workflow_commands`**（按
    `(scope_digest, operation_id, command_id)` 确定序，显式包含 source/record/resume/supersession/current owner/
    idempotency target）→ intent/current predecessor → ActivityRun/Attempt → optional grant/cost exposure；进入 intent
    段后禁止回头 insert/lock command，未触及的可选 aggregate 可跳过但不得逆序。`d3_business_fence_v1`
    evaluator 只接受 closed typed context union：claimed-command phases
    `stage_b|terminal|record|dispatch|resume_after_grant` 必须带 current `ClaimAuthority` + `ClaimReceipt`；aggregate
    `control` phase 必须带 registered control authority + expected revisions，且不得伪造 claim token。六 phase 均
    mandatory，wrong context fail closed。dispatch、Stage B、terminal、record、resume-after-grant 与 control 共用由 typed
    `d3_business_fence_v1`（base plan/review/gate/intent pins）和 immutable
    `workflow_commands.d3_business_fence_digest` 驱动的 centralized phase-parameterized business predicate，
    exact-compare 当前 review/gate revision、`human_transition_pending`、decision source event、intent phase
    与 phase-specific manifest/grant/exposure。JSON、model text 或 ambient state 不得补齐 business pins。
    transport owner 在同一短 PG UoW 完成 cost reservation 与 `dispatching` 线性化，提交后才允许网络
    send。control-first 或 centralized business predicate mismatch 都是 domain/attempt/intent/event/command/
    source/result 零写，并只返回
    `not_applied(reason=stale_claim|business_precondition_conflict)`；该返回不是 event/state。dispatch 先赢则该调用属于已授权 in-flight
    （成本可发生），但新 epoch/revision 使其 result/domain apply 永远零写，response 只可进入
    transport-owner durable quarantine + cost reconciliation。post-network receipt get-or-create/quarantine/cost
    reconciliation 仅承接已授权 exposure，走独立固定 `exposure → receipt → quarantine/cost-axis` order，绝不回到
    operation/command/intent/domain，也不授权 send/apply；该 post-network path 与 heartbeat/read-only terminal replay
    的 command-row-only 窄例外分开。禁止持 PG transaction 跨网络，也禁止
    command owner 绕过 transport owner 直接调用 provider；generic/owner cancel 与所有会使 dispatch 授权失效的
    command control 也必须先取同一 coordination lock；D3 OperationRun
    terminal/cancel/retry/requeue/resume/reset/rebuild/recovery
    同样在 inventory 内。只有 heartbeat occurrence 与 read-only terminal exact replay 可走 command-row-only
    窄例外；lock-busy 是 typed 零写 outcome；
    grant 进 §4b owner 矩阵（owner = plan review owner，消费方 = Tier-2 命令）；
  - **plan 终审通过的前置** = 全部 blocking reasons 已清除或 human_confirmed（含身份 reason）；
    **commit owner 原子复查**：计划 provider 工作前在同一 UoW 内验证 blocking reasons 状态
    （修复 pinned 的 commit 只查 approved 状态的缺口——实施项，进 W11 commit owner 批）；
  - 死锁解除：验证需要搜索时，人先点 `identity_search_budget_grant`（不终审）→ Tier-2 跑完 →
    身份 reason 清除/转 human 决定 → 终审。信封耗尽后的追加 = 再次 grant（同一部分决定，新额度）。
  - ActionSpec 政策恒静态：入口 action `verify_company_identity`（not_required + Tier-1 小信封）；
    Tier-2 命令的审批证据 = grant 事件引用，缺引用 fail-closed。

### 2.2 W11 接入与 durable join（v4 采纳 round-3 critical #2 的修正链）

**事件驱动链（reducer 只计划命令、永不直写域状态，对齐
`DURABLE_EXECUTION_RUNTIME_CONTRACT.md:290-315`）**：

```text
acquisition.plan.build 结果事件（含 plan 期廉价解析的置信度）
  → reducer 计划 acquisition.plan_review.request
  → request owner 先创建 review session（canonical review_id 此刻才存在），发 session-created 结果事件
  → reducer（置信 < high 时）才计划 company.identity.verify.evidence；OperationRun pin
    coordination_plan_review_id=review_id，command exact-copy，authority/receipt bind
  → 验证 terminal 结果事件（判别化；只有 `final_adjudication` 是 record planner）：
      evidence_insufficient + active grant        → search.expand（中间事件，绝不到 record）
      evidence_insufficient + 暂无 grant 但允许补发 → 验证 owner 规范化为
                                                     final_adjudication(record_outcome=awaiting_budget)
      evidence_insufficient + hard envelope/policy 禁止再 grant
                                                   → final_adjudication(record_outcome=needs_human)
      non-retryable execution failure              → final_adjudication(record_outcome=failed)
      deadline/max_wall timeout                     → final_adjudication(record_outcome=timed_out)
      semantic ambiguity/protocol-invalid outcome  → final_adjudication(record_outcome=needs_human)
      authorizable final result           → final_adjudication(record_outcome=authorizable)
    reducer 仅从上述 `final_adjudication` 计划唯一 command type
    `company.identity.verification.record`，并把 discriminant 映成 typed record input
  → company.identity.verification.record（owner = 验证 owner；这是独立 record command，有自己的 current
    ClaimAuthority/ClaimIdentity 与 ActivityAttempt；幂等键 =
    `record:<runtime_namespace>:<workspace_id>:<intent_id>:<phase_generation>`——v7 修 R6#1 的
    命令类型命名空间化 + v8 补 namespace/phase；**三分支语义（v8 统一 R7#3/#4）**：
    授权分支 = 全条件 CAS 过 + manifest 完备 ⇒ shadow_would_verify；**非授权分支 = 同样过身份/
    围栏 CAS 但落 typed `awaiting_budget | needs_human | failed | timed_out` 域写**（record 是这些迁移的唯一 owner，非授权
    结果也是真实域状态；其中 `awaiting_budget` 精确写 intent 状态/owner event、保持 verification current-state
    行未授权且不伪造 `verification_state=needs_human`，其余三者写各自 declared verification/intent transition）；
    `not_applied` **只保留给** stale 身份/generation/epoch/hash 失配的 typed
    零持久写返回——不再与 needs_human 混用，也不写 no-op 域事件）
  → record owner 的 terminal-UoW specialization：一个 PG transaction 同时执行 `phase=record` 与
    `phase=terminal` 两套 mandatory predicate，§4c 全条件 CAS 同时验证 record command 自己的 current
    claim/attempt 与 intent，
    并通过 centralized phase-parameterized business predicate 复算 d3_business_fence_v1
    冻结的 source verify command generation/epoch/post-claim command attempt/terminal event+outcome digest；匹配时只写自己的
    聚合（verification 行 + intent 迁移 + applied/非授权证据），并在同一事务写
    `company_identity_verification_recorded` 域事件、workflow terminal event、ActivityAttempt + command terminal
    pair；任一身份/业务前置失配或 crash 整笔回滚。exact replay 复核整个 aggregate，禁止 applied intent + running
    record command 的部分提交窗口
  → reducer 对 record 的**授权与非授权分支结果**（不含 not_applied）计划
    plan_review.identity_result.apply（owner = plan review owner；幂等键 =
    `apply:<runtime_namespace>:<workspace_id>:<session_id>:<源域事件 id>`——v8 修 R7#2：
    namespace + provider_mode 绑入 apply 的事件/命令身份/幂等 scope/gate 行与 **CAS 每一条**
    【含 expiry/supersession/human 路径】，跨模式污染 preflight 覆盖 apply；v7：各次转移各有
    源事件、各自成键；not_applied 没有 durable source event，永不触发 gate 更新）
  → apply owner 单 UoW（v6 补 R5#1 的 gate 侧围栏）：CAS 于 {workspace + session 当前 revision +
    source 事件 id 匹配 + generation 规则}；**watermark 单调、每次成功 apply（含 blocking 方向）
    都推进**（sweep blocker 修正——阻塞若不推进 watermark，晚到的旧 clearing 可在
    blocking(gen N+1) 之后以 gen N+? 通过）：clearing 仅当 generation > watermark 生效；
    blocking 当 generation ≥ watermark 生效并推进 watermark——晚到旧 clearing 永被拒。对
    `record_outcome=awaiting_budget`，record UoW 只持久化 awaiting intent + `recorded_event_id`/domain event 并
    terminalize；随后 gate owner 取得 coordination lock，在进入 intent/grant 段前按全局顺序 reserve/lock
    resume-v2 command identity
    `resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>`，
    原子推进 exact gate watermark，并从 **post-gate-apply typed pins** 填完整
    `company.identity.verification.resume_after_grant` command/outbox。active grant 时置 `queued`，否则置
    `retry_wait`
  → gate-event owner 与 grant owner 共用 `maybe_plan_resume`：仅 gate watermark exact applied + intent
    awaiting + active grant 才 create-or-exact-replay/reawaken resume-v2；identity 包含 `recorded_event_id`，不含 grant
    id/瞬时 delivery。grant-first 由 durable grant 被 gate apply 看见；record-first 先落 post-apply `retry_wait`，later
    grant exact-lock/reawaken。`recorded_event_id` 只作 immutable read，不新增未排序 `FOR UPDATE` event lock。若
    watermark 已 applied 但 resume row 缺失，grant UoW 返回 typed `resume_convergence_missing` 且零写；既有
    recorded event/gate-apply command 是 durable replay source，不另 schedule 新 replay。禁止进入 intent/grant 后
    late insert，因此无 one-shot gap 或 old gate-digest collision
  → 发 apply 结果事件
```

**`final_adjudication.record_outcome` 的 exact owner transition（D3b round4）**：

| `record_outcome` | Exact source condition | `verification_state` after record | `intent_state` after record | Owner event | Gate / resume effect |
|---|---|---|---|---|---|
| `authorizable` | final adjudication satisfies the complete manifest and every authorization predicate | `shadow_would_verify` | `applied` | `company_identity_verification_recorded(record_outcome=authorizable)` | gate remains blocking for the explicit Phase-2 human/promotion rule; no automatic resume |
| `awaiting_budget` | evidence remains insufficient; no usable grant was observed by the adjudication, and policy permits a later grant | exactly `pending`; never `needs_human` | `awaiting_budget` | `company_identity_verification_recorded(record_outcome=awaiting_budget)` | gate remains blocking and exposes only the recoverable grant action; post-apply gate watermark plus durable grant converge through §6.2.3 |
| `needs_human` | hard no-more-grant envelope/policy exhaustion, semantic ambiguity, protocol-invalid output, model fallback, policy-invalid result, or incomplete/non-authorizable manifest | `needs_human` | `applied` | `company_identity_verification_recorded(record_outcome=needs_human)` | gate remains blocking with a non-resumable human reason; no false grant affordance |
| `failed` | non-retryable verification execution failure | `failed` | `applied` | `company_identity_verification_recorded(record_outcome=failed)` | gate remains blocking; a later retry requires a new owner intent/command, not grant resume |
| `timed_out` | the **recorded verification outcome** reached its declared deadline/`max_wall` while the record claim and business fence remained current | `timed_out` | `applied` | `company_identity_verification_recorded(record_outcome=timed_out)` | gate remains blocking; later re-verification requires a new owner intent/command |

`fallback`/policy-invalid 必须规范化为 `needs_human`，nonretryable execution failure 必须规范化为
`failed`。record outcome 的 `timed_out` 不等于 control timeout：control timeout 写 intent=`timed_out`、
verification=`needs_human`，并发独立的 typed control event，不冒充
`company_identity_verification_recorded(discriminant=timed_out)`。

**反向失效与 commit 复查（v6，R5#1）**：expire/supersede 命令改变验证聚合后，同样发域事件 →
reducer → `plan_review.identity_result.apply`（blocking 方向）**重开 gate**；expiry 定时事件携带
generation，触发时 CAS 于当前 generation（旧定时器不可失效新决定）。`acquisition.plan.commit`
owner 在计划任何 provider 工作的同一事务内**复查 canonical**：验证行 generation + `valid_until`
有效 + gate watermark 与之一致——不信任 gate 上复制的 reasons 快照。

**（v5，round4#1 单写者拆分）**两段各自单 owner 单聚合：验证状态/intent/generation 只由验证
owner 写，gate 只由 plan review owner 写，两者以域事件+reducer 连接；两步之间 gate 保持
fail-closed（身份 reason 未清即仍阻塞，中间态无放行窗口）。人工确认同构反向：review owner 在
自己 UoW 记录人工决定 + 发事件 → reducer 计划验证 owner 的 supersession 命令（写 human_confirmed
+ generation+1 + supersede intent）。**Phase-2 语义澄清（义务清单第 6 条的裁定）**：
`verified_accepted` 只清身份 reason，**永不计划 commit**——commit 仍只由终审批准链触发。

- plan 期廉价解析（只跑确定性分支，不 search 不调模型）在 plan.build owner 内完成，置信度随
  结果事件携带；
- legacy plan 前门：同一共享 helper 从 §4 读模型计算身份 reason（两前门一个计算逻辑）；legacy
  路径的验证 enqueue 走既有 operation 提交面，terminal 后同样经 apply 命令收口；
- gate reason `company_identity_unverified`：OR 组合、只清自己；`skip_plan_review` 绕过记录在案，
  hosted 激活前限 operator。

### 2.3 Loop 步骤与出口（自包含全文）

1. **Tier-1 取证**：候选 = plan 期 observed 候选 + §4 读模型历史；逐候选 documents.fetch 公司
   页面/official-domain 证据（fetch_key 去重）；
2. **裁决**：§6 schema 逐候选×证据比对（模型只见证据 id，见 §6）；
3. **Tier-2 续跑编舞（v7 修 R6#4——durable 化，不再隐含"命令内等待"）**：`verify.evidence` 的
   类型化 phase 结果 = `final_adjudication | evidence_insufficient`；最终结果的
   `final_adjudication.record_outcome = authorizable | awaiting_budget | needs_human | failed | timed_out`。
   `evidence_insufficient` 且 grant 可用 ⇒ 其**结果事件**经 reducer 计划 `search.expand`；
   expand 终态事件 ⇒ reducer 计划**后继 `verify.evidence` 子命令**（root intent + 单调
   `phase_generation`+1，每个子命令各绑自己的 claim/attempt——intent 结构升级为
   root-intent + per-phase 子绑定）；暂无 active grant 但 policy 允许补发时规范化为 `awaiting_budget`；hard
   envelope/policy 已禁止再 grant 或语义歧义/协议非法时为 `needs_human`；不可重试执行失败为 `failed`；
   deadline/`max_wall` 为 `timed_out`。只有 `awaiting_budget` 的 gate 可附可恢复的「授予搜索预算」动作；
   grant issuance 与 awaiting record 均 create-or-exact-replay
   `company.identity.verification.resume_after_grant` durable convergence command，关闭 grant-first/record-first
   lost wakeup；hard
   exhaustion 的 `needs_human` 不得伪装成可续跑。
   **只有显式 `final_adjudication` terminal event 才允许 reducer 计划唯一
   `company.identity.verification.record` command type**——中间 phase 结果结构上到不了 record；
4. **出口**：谓词（§7）+ 裁决全过 ⇒ Phase 1 记 `shadow_would_verify`（非授权态，gate 仍需人一键
   确认，同时记 shadow 统计）；Phase 2（§7 统计门 + owner GO + 逐行 revalidation + promotion 事件）
   才产 `verified_accepted`。其余出口按同一 typed map：可补 grant 的 step/cost/search-envelope exhaustion ⇒
   `awaiting_budget`；hard envelope/policy terminal exhaustion、谓词失败、uncertain/fallback、冲突或语义歧义 ⇒
   `needs_human`；不可重试执行失败 ⇒ `failed`；deadline/`max_wall` ⇒ `timed_out`。四者都保持 gate 阻塞，证据卡
   记录候选 URL、来源域、裁决、rationale、冲突与精确 terminal reason。

## 3. Provenance：服务端引用（v3 定型，v4 不变）

- 机器验证结果只存在服务端（§4 行 + result artifact）；消费方经 server-owned verification
  reference（workspace + fingerprint + generation + decision source 绑定）服务端 resolve；公共
  ingress 禁止携带该引用或镜像字段（守卫：出现即拒）。
- 优先级：manual override（前置 pass 现状不变）> `agent_self_verified`（服务端 resolve）> 其余
  既有分支；执行期守卫（`:985-990`）扩展为两通道任一在位即跳过付费分支。
- 人工 provenance 持久化：human_confirmed 行带 decision source=human + review session ref，跨 plan
  复用永不重标为机器验证。agent 永不写 `target_company_linkedin_url`；意图抽取白名单不扩展。

## 4. Current-state 读模型、注册表收编与 verification intent

### 4a. 全局文件注册表收编（v4 按 round-3 critical #1 扩全）

- **PG 表 canonical**。收编步骤：
  1. **全量 inventory（批 step 1，硬项）**：注册表与 `identity.json` 快照的**每个**读写点分类
     ——已知写入方 4 处（`acquisition.py:1017-1031`、`company_registry.py:492-539`【被
     asset_registration/cloud_asset_import/organization_assets 调用】、`:554-594` upsert、快照重扫
     `:641-681`），Scout 补全后逐点标注 authoritative/mirror/retire；
  2. backfill：文件注册表 → PG，`decision_source=legacy_registry_import`、
     **verification_state=needs_human**（导入行永不 verified/accepted 态）、无 workspace 的全局
     记录进 quarantine 映射表（显式映射到 workspace 或标 shared-origin，不静默落入任一 workspace）；
  3. 文件注册表 + 快照重扫路径全部降级 migration bridge：读先 PG 后 bridge（命中记指标）、
     **全部写入方**迁移或停写（asset/cloud-import/org-assets 调用方逐个改造为经 PG owner 写入；
     Scout 需补 round-4 点名的 `company_asset_supplement.py:1032-1042`、`asset_sync.py:1066-1077`
     等快照写方，并更正 `:641-681` 为读方）、deletion preflight 覆盖所有 resolver 路径含快照重扫；
     **bridge 消费语义（v5，round4#8）**：PG miss 且 bridge 命中而**无显式 workspace 映射**时，
     结果为 **diagnostic-only 的 needs_human**——不得授权身份、local-asset 可用性、gate 清除或
     付费分支抑制；存在 bridge 命中期间，normal-path 签收被 scope-local 阻断直至删除条件达成；
  4. precedence preflight：PG 行在位时文件注册表与快照不可能胜出；workspace 决定永不回写全局文件。
- 跨 workspace 共享语义显式推迟（内部单 org 阶段），推迟记录在案。

### 4b. 表契约（v4 恢复矩阵 + 补全迁移）

**PG-only**（migration 0002+ DDL + `store.repos.<domain>` repository owner；无 SQLite 路径）。

**运行时隔离（v7 修 R6#2，对齐 RUNTIME_ENVIRONMENT_ISOLATION 契约）**：operation/command/attempt/
信封/证据 bundle/本表行身份/幂等 scope/每条接受 CAS 全链携带不可变 `runtime_namespace` +
`provider_mode`；**非 live（simulate/scripted）证据 diagnostic-only**——结构上不可能产生
live 命名空间的 `shadow_would_verify`/`verified_accepted` 行（模式不匹配即 CAS 拒），Phase-2
promotion 只认 live 证据；跨模式污染 preflight 进 contract lane。

`company_identity_verifications`：`(runtime_namespace, workspace_id, company_fingerprint)` 唯一
（v7 隔离键前置）；`provider_mode` 不可变列；`fingerprint_version`；
`decision_generation` 单调；`verification_state`；`accepted_policy_version` + `valid_until`；
`canonical_url/slug` + 推导规则版本；`decision_source ∈ {machine, human, legacy_registry_import}`；
`result_artifact_ref`（schema 版本化；高基数内容全在 artifact）；物理因果列（source
workflow_command_id / activity_run_id / attempt_id / entity_delta_id）；预算 grant/信封引用；审计
时间戳。

**状态迁移表（全量，注册期校验；readers 永不直写）**：
| from | to | 触发（全部为类型化 owner 事件/命令） |
|---|---|---|
| pending | shadow_would_verify / needs_human / failed / timed_out | 验证 terminal（**record 命令**，v6 更正——apply 只写 gate） |
| shadow_would_verify | verified_accepted | **显式 promotion 命令**（Phase 2 + revalidation 过） |
| shadow_would_verify | needs_human / superseded | 过期/policy 升版/新 intent |
| verified_accepted | needs_human | **`company.identity.verification.expire` 命令**（v5 修正 round4#4——读者不得 enqueue：接受时**同 UoW 持久化 expiry not-before 定时事件**，定期域 owner 扫描发类型化事件 → reducer 计划 expire 命令；读路径只做 fail-closed 派生——过期行**按 needs_human 消费**但零写零 enqueue） |
| pending | needs_human / superseded | 控制路径（cancel/timeout/rebuild 的域命令）——v5 补 round4#9 缺口 |
| superseded | pending | 新 verification intent（重验；superseded 现态行被新 generation 决定覆盖）——v5 补 |
| verified_accepted | superseded | 新 generation 决定（人工或新验证） |
| failed / timed_out / needs_human | pending | 新 verification intent（重验） |
| 任意非 human_confirmed | human_confirmed | 人工确认 UoW |
| human_confirmed | superseded | 仅新 generation 人工决定 |

**历史与现态分离（v5，round4#9）**：append-only 的 decision/intent 历史（EntityDelta + intent 行
+ superseded 行审计链）与 canonical 现态（本表唯一行）分离——superseded 现态行由**新 generation
决定整行替换**（历史留审计链），不存在"superseded 行原地复活"。Plan §6#7 的 canonical claim-fence
shared-field matrix 已由 D3b §4 锁为 **10 列 × 26 data rows**、每格非空的 decision-complete 形状；terminal
provenance policy registry 与 transport terminal evidence receipt persistence 是两个不同 owner/lifecycle 行；本设计不
复制或另建第二份 claim-authority matrix。其 owner/migration/repository/runtime 物理实现仍是未来 D3 义务。

**company-identity domain-field owner addendum（6 data rows；沿用同一 10-column shape）**：

| Field/object | Single owner | Physical SOT | Allowed values | Derivation rule | Consumers | Forbidden consumers | Fallback/brownfield status | Migration status | Deletion condition |
|---|---|---|---|---|---|---|---|---|---|
| `verification_state` | company-identity verification owner | canonical `company_identity_verifications` row | registered state-transition enum in this section | record/promotion/expiry/human owner commands apply one registered transition | two front doors, gate, UI | intent extraction, public ingress, direct readers writing state | missing row means unverified | future D3 PG repository/state-machine implementation | retain as canonical current state; delete only superseded compatibility sources after bridge gate |
| resolver=`agent_self_verified` | connectors identity resolver owner | current canonical verification generation | fixed resolver value only for an accepted current row | resolver derives it from the current eligible verification row, never request preference | identity consumers | execution-preference ingress, model output | not emitted when the row is absent/ineligible | future D3 resolver cutover; D3b is decision-only | delete legacy resolver branch after the full bridge zero-hit window |
| evidence-card `candidates[]` | company-identity verification owner | versioned result artifact | schema-bounded candidate list | owner renders from the immutable evidence bundle and adjudication manifest | gate UI and authenticated audit view | model-output round-trip, public ingress, authorization CAS | absent list changes copy only and never authorizes | future D3 artifact schema/repository | purge payload only under artifact retention; retain digest provenance |
| `decision_generation` / intent | company-identity verification owner | canonical verification row plus `verification_intent` lineage | non-negative monotonic generation and the §4c intent enum | owner increments generation once per accepted new decision and creates the exact intent lineage | record/apply CAS paths | readers, public ingress, other aggregate owners | no intent/not issued | future D3 PG rows/CAS; D3b is decision-only | retain monotonic lineage and tombstoned history permanently |
| `canonical_url` / `slug` | company-identity verification owner | canonical verification row with derivation-rule revision | registered URL-shape predicate and canonical slug | versioned server resolver derives from accepted owner evidence | front doors and execution guard | caller/model text as source of truth | missing or invalid means `needs_human` | future D3 derivation/revalidation implementation | delete old derivation revision only after revalidation and golden-test parity |
| migration-bridge hit | migration bridge owner | report-visible bridge metrics table | non-negative count keyed by bridge/revision | increment once for every compatibility read/write hit | deletion preflight and operator report | authorization, gate clearing, product readiness | zero is the expected steady state | bridge metric lands with the future migration | delete bridge only after the registered full-denominator zero-hit window; retain tombstone metric |

### 4c. verification intent（v4 绑定物理执行身份，round-3 #4）

`verification_intent`：`(runtime_namespace, provider_mode, workspace_id, intent_id)` 身份（workspace 与
namespace/mode 等式进入索引、命令/事件引用、证据 bundle、repository 授权、幂等 scope 与下述 CAS
的每一条；这正是 OB-10.1 的实施落点）。intent 先携带 exact scope envelope =
**runtime_namespace + provider_mode + workspace_id + scope_digest + operation_run_id +
coordination_plan_review_id + review_session_id**，且 strict D3 invariant 是
**coordination_plan_review_id = review_session_id = plan_review_sessions.review_id**（同一 canonical positive
`BIGINT` id，不是两个 coordination owner/alias；brownfield `NULL`，strict `>0`，禁止 `TEXT`/empty）。其内 source verification identity 分成两个物理部分：不可变 source core tuple =
**source_verification_command_id + source_claim_generation + source_control_epoch + source_command_attempt + source_activity_run_id +
source_activity_attempt_id + source_claim_authority_spec_digest**；另有初始全 null、仅由 source result-terminal
UoW 在 expected-null CAS 下填一次的 append-once terminal tuple = **expected_source_terminal_status +
expected_source_terminal_event_id + expected_source_terminal_outcome_digest +
expected_source_terminal_transport_variant + expected_source_terminal_provenance_policy_digest +
expected_source_response_spec_digest + expected_source_transport_response_receipt_id +
expected_source_transport_attempt_failure_receipt_id +
expected_source_dispatch_exposure_id + expected_source_physical_call_index +
expected_source_provider_call_id_state + expected_source_provider_call_id +
expected_source_model_invocation_envelope_ref + expected_source_model_invocation_envelope_digest +
expected_source_terminal_reason +
expected_source_response_occurrence_id + expected_source_canonical_response_digest +
expected_source_canonical_result_digest + expected_source_result_artifact_ref +
expected_source_result_artifact_digest +
expected_source_failure_occurrence_id + expected_source_failure_code + expected_source_failure_spec_digest +
expected_source_canonical_failure_digest + expected_source_retry_policy_revision +
expected_source_retry_disposition + expected_source_failure_artifact_ref +
expected_source_failure_artifact_digest + expected_source_no_exposure_spec_digest**。response branch exact-copy
historical policy/`TransportResponseSpec` digest 与 valid envelope terminal reason；failure/no-exposure branch 按
contract 令不适用的 response fields 为 SQL NULL，但所有 branch 都 pin historical provenance policy。terminal tuple 一旦存在即
不可改写；registered source reopen 只可清 command row pair 并为 successor phase 建新 intent，不能改写历史
intent binding。raw claim capability/digest 永不复制进 intent。source 命令终态事件触发的
`company.identity.verification.record` 是独立 command，拥有自己的 record command id、current
ClaimAuthority/ClaimIdentity 与 ActivityAttempt；不得用 record claim 覆盖或替代前述 source binding。
**claim identity 的物理实现（D3b 决策锁，扩展 v5/round4#2）**：
pinned `workflow_commands` 只有会被 generic retry 重置的 `attempt` 计数——本设计**要求新增
operation-rooted immutable runtime namespace/provider mode/workspace scope、registry authority digest、永不
重置的单调 claim generation、private token verifier、独立 control epoch、heartbeat occurrence 与 terminal
outcome digest/event identity。`workflow_commands` 复用既有 `operation_id` 作为唯一 operation link，严格等于
`operation_runs.operation_run_id`，不新增/双写 `operation_run_id` alias。canonical registry/scheduler 只在
repository exact-command selection 后由 composition-installed private factory 单次铸造不可由 command id/owner
字符串或公共 payload 构造的 one-use `ClaimAuthority`；strict registry entry 把
`claim_fence_policy=d3_v1`、`allowed_stage_ids`/stage policy 以及从唯一 applicable
`TERMINAL_PROVENANCE_SPECS` manifest 推导的 `terminal_provenance_policy_digest` 纳入 authority spec digest；
command 与 exposure 创建时经既有 authority/business pins 冻结该历史 policy（不新增 command column），旧 spec/
manifest 保留到没有 active 或 retained command、exposure、response/failure receipt、terminal event、source-intent
tuple、quarantine/tombstone 或 cost/audit 引用。Migration C 的物理 population guards 由 registry 分别生成
hash-bound `scoped_session_bootstrap_command_types_v1` 与 `strict_d3_command_types_v1` literal manifests，并以
immutable `workflow_commands.command_type` 判别，不能用空/non-empty digest sentinel 代替。authority 的 `expected_stage_id` 只能来自 exact
selected row，不能由 caller 补齐。其同时绑定 `coordination_plan_review_id`、
    `d3_business_fence_digest`、selection generation、repository-time expiry、pre-claim `workflow_commands.attempt`
    与 expected control epoch；Stage A 必须在同一 claim CAS 把 attempt `+1`，receipt/ClaimIdentity 携 post-claim
    attempt。selection UoW 必须先证明旧 execution lease/selection reservation absent/expired，再以
existing lease fields 持久化 exact unexpired reservation；Stage A exact-match 该 current reservation、以
execution lease 覆盖它（绝不要求 absent/expired），并在 generation +1/token rotate 的同一 CAS 原子写 consumed
    authority id。`consumed_claim_authority_id` 只是 **current-selection one-use slot**，不是永久 audit：registered
    reselection 先将 `claim_selection_generation +1` 并清空该 slot，新 authority 才可写入；该 slot 不承担、
    不宣称 durable historical audit SOT。完整 29-wrapper inventory 仅作 observation/zero-unauthorized-increase
    denominator；scope-local deletion 只覆盖 bootstrap/strict-D3 manifests，任何 `legacy_unfenced` entry/caller 尚存
    时 generic bridge 明确保留。fenced wrapper
启动时不得铸造或持有 standing capability。operation → command → ActivityRun → ActivityAttempt 在各自创建 UoW
物理 copy+compare scope；ActivityAttempt 另 exact-copy post-claim `command_attempt`，intent source core 再持久化
相同 `source_command_attempt`（migration 项；retry 不清 generation），intent 在 **source claim 完成且 source
ActivityAttempt 创建之后**绑定；poll-mode strict scope 先由 private scoped review-session repository 通过独立
bootstrap contract 签发。source session-create command 的 registry policy 是
`claim_fence_policy=scoped_session_bootstrap_v1`；closed typed `ScopedSessionBootstrapContext`、private
`mint_scoped_review_session_bootstrap_authority(...)`、one-use `ScopedReviewSessionBootstrapAuthority`、Stage-A
pre-session current-claim `ScopedReviewSessionBootstrapReceipt`（不含 ActivityRun/ActivityAttempt/review/event/result）与
`verify_scoped_session_bootstrap(...)` 只授权 session create，禁止
transport/domain effect。它们 exact-bind source generation/epoch/post-claim command attempt/lease、authenticated workspace 与 typed plan
pins，且 review id/review/gate 明确 absent；专用 `scoped-session-bootstrap-lock-v1` key 只含 canonical scope tuple +
`creation_idempotency_key`，不依赖尚不存在的 review/gate。repository 在该 lock 下验证 bootstrap authority/
receipt、tenant/plan 与 durable source command/event；specialized bootstrap Stage B 在一个 PG UoW 创建 claim-bound
ActivityRun/ActivityAttempt + session/event/source terminal。Stage A 后但 UoW 前 crash 不留下这些 aggregate，须等
lease expiry/reselection；commit 后丢失 raw token 时，独立的 credential-free
`ScopedSessionCommittedReplayContext` 只能 read/exact-compare，不得重建 authority/receipt 或写入，并以
`(scope_digest, creation_idempotency_key)` 返回同一 `ScopedReviewSessionCreateResult`/positive review id。仅
session commit 后 normal `d3-dispatch-v2`/`d3_business_fence_v1` 才适用，再由 OperationRun exact-copy。legacy
unscoped/JSON-scan review creator 不能被采用；action-backed root 另受 Plan §6#6
`action-root durable-scope gate`（无 OB-ID）阻断，必须先在 action root 持久化 namespace/mode/workspace/issuer/
digest，scoped review-session repository 先 lock/exact-compare action pins 再签发 session scope，OperationRun
UoW 只 exact-copy；poll/action strict-D3 都不得由 OperationRun remint。这不是 OB-10.4。`workflow_commands` 新增 immutable
    `d3_business_fence_digest`，由 command creation owner 对 typed base plan/review/gate/intent pins canonicalize
    `d3_business_fence_v1` 后写入；central evaluator 使用 closed typed context union：claimed-command
    `stage_b|terminal|record|dispatch|resume_after_grant` context 必须带 current authority+receipt；aggregate
    `control` context 必须带 registered control authority/expected revisions 且无 claim token。六 phase 全部
    mandatory，wrong context fail closed。generic 控制面（cancel/retry/resume）
只动 runtime 现态，域侧 supersession 经 owner 控制事件 → reducer → 域命令完成（不假设控制面
直接原子改域行）。存储 plan bundle hash + fingerprint + `accepted_policy_version` +
route/schema revisions + **effective_route_snapshot digest（§6 共享契约）** + expected
decision_generation；`intent_state ∈ {pending, awaiting_budget, applied, cancelled, timed_out,
superseded}`（v8 补 `awaiting_budget` 入枚举与迁移：pending→awaiting_budget【record 非授权分支，
evidence_insufficient、暂无 active grant 且 policy 允许后续补发】；awaiting_budget→pending【durable
    `company.identity.verification.resume_after_grant` convergence command 只在 gate watermark exact applied + intent
    awaiting + active grant 的 post-gate-apply typed pins 下计划后继 verify.evidence，新 phase generation 绑定；
    gate-event owner 与 grant owner 共用 `maybe_plan_resume`】；awaiting_budget→cancelled/timed_out/superseded
【控制转移同 pending——永无 grant 时经 timeout 收敛，不悬挂】）。
- retry、resume、cancel、timeout、plan 重编译、人工决定——每种经上述事件→reducer→域命令路径
  **原子 supersede 旧 intent**；只有 invalidating control/input/requeue 接受态推进 command control epoch
  `+1`；`succeeded|failed_terminal` result-terminal 不推进 epoch，但必须清 token/lease 并写 terminal pair。
  generic requeue 只推进 command control epoch/清旧 token 并触发域侧
  supersession，绝不在无新 claim/ActivityAttempt 时铸 successor。下次 source Stage-B UoW 与 async
  supersession 都先取 common coordination lock，再按唯一全局序锁 operation root → optional plan/review/gate →
  all participating `workflow_commands`（确定序，包含 source/record/resume/supersession/current owner/idempotency
  target）→ intent/current predecessor → ActivityRun/Attempt → optional grant/cost；进入 intent 段后不得回头
  insert/lock command，
  未触及 aggregate 可跳过但不得逆序。随后原子将旧 active binding 标成
  superseded、推进 phase generation，并 create-or-exact-replay 唯一 successor；异步 supersession command
  physically pin `expected_predecessor_intent_id` + `expected_predecessor_phase_generation` +
  `expected_predecessor_source_control_epoch` + `expected_predecessor_decision_source_event_id`，并与 Stage B 调同一
  owner repository CAS。这四项是 `workflow_commands` 的 typed nullable physical columns（禁止 JSON/payload），
  local shape 只能 all-`NULL` 或 complete；all-`NULL` 具有 initial semantic role，必须锁内证明 canonical lineage 尚无
  current phase；complete tuple 才能做 successor expected-value CAS，任何 half-null 形状 fail closed。它们把 strict
  additive command columns 从 16 扩为 **20**。successor-producing async command
  只 schedule convergence、不在新 claim/attempt 前铸 successor，
  no-successor cancel/timeout/human variant 也 exact-CAS 同一 tuple。若 Stage B 先赢则 typed exact no-op，
  tuple 任一失配整笔零写，任意时刻最多一个 active phase；
- **record 命令 owner 的 terminal-UoW specialization**（v6 修正 R5#2 两向问题）：一个 PG transaction 同时执行
  `phase=record` + `phase=terminal` mandatory predicate；先取 common coordination lock，并按
  上述全局序锁 operation root、适用的 plan/review/gate、source+record command、intent/predecessor、
  ActivityRun/Attempt（record apply 不触及 grant/cost，故跳过尾部而不逆序）；再验证 record command 自己的
  private current ClaimAuthority/ClaimIdentity + record ActivityAttempt，再验证 `runtime_namespace/provider_mode/
  workspace_id 匹配 AND intent_id 匹配 AND intent_state='pending' AND source_claim_generation 匹配 AND
  source_control_epoch 匹配 AND source_activity_run_id 匹配 AND source_activity_attempt_id 匹配 AND
  source_claim_authority_spec_digest 匹配；同一事务按
  `(scope_digest, operation_id, command_id)` 固定顺序锁 source + record command，要求 centralized
  business predicate 先 exact-revalidate typed plan/review/gate/base-intent pins 与本 phase 所需
  manifest/grant/exposure，且 **source command 当前
  operation/scope + claim generation + control epoch + post-claim command attempt + terminal status + terminal event id + canonical outcome digest
  = intent 冻结预期值**，并经 composite event identity 复核（源命令重开/重试、epoch 前进、event/digest
  替换或缺失均零写）
  AND stored_fingerprint 匹配 AND decision_generation=<expected> AND policy/schema/route/snapshot
  pins 匹配 AND review_session 当前仍 pending-review AND verification_state NOT IN
  (human_confirmed) AND **（v7 修 R6#5）所属 OperationRun 当前非终态（terminal winner 检查）AND
  plan/review revision（bundle watermark）与 intent 存储值相等**——cancel/timeout/requeue/rebuild/
  重编译在各自 commit 的同一 UoW 内**同步推进**各自 owner 的对应围栏（epoch/revision），旧 record 在
  异步 supersession 落地前即已被挡；AND **（v7 修 R6#6）
  adjudication-set manifest 全终态聚合 hash 匹配**（见下）——全过则原子写 verification+intent state、
  `company_identity_verification_recorded` domain event、workflow terminal event、ActivityAttempt+command terminal pair；
  crash 全回滚，exact replay 复核整个 aggregate，绝无 applied intent + running command gap。任一失配 ⇒ 全不动 +
  typed `not_applied(reason=stale_claim|business_precondition_conflict)` 返回，零 durable event；
- **多候选完备性证明（v7，R6#6；v8 修 R7#4 的产生机制矛盾）**：owner 在裁决开始前**预创建**
  manifest 全部条目（键 = workspace/intent/phase generation；服务端枚举的 expected candidate
  ids）；每条目经裁决调用终态化，**未决条目由 owner 的超时/对账命令终态化**（标 unresolved）；
  完整 terminal bitmap 参与聚合 hash。**授权分支**要求 bitmap 全部 resolved-authorizable 且
  hash 匹配；存在 unresolved/非授权条目 ⇒ **走 record 非授权分支落 needs_human（真实域写）**
  ——不是 not_applied（后者只允许 closed reasons `stale_claim|business_precondition_conflict`）。"幸存者显得唯一有效"仍被结构性堵死，
  且 needs_human 有了明确的产生者；
- **retry ABA 窗口封堵（v6，R5#2）**：requeue 在同一控制 UoW 内先递增命令的 **durable control
  epoch**（与 claim generation 分立、requeue 即变），intent 记录 epoch——旧结果在"已 requeue、
  未重 claim"窗口内因 epoch 失配即拒；后继 intent 只在新 claim + 新 ActivityAttempt 创建事务内
  铸造；
- **跨 owner dispatch 围栏**：dispatch 与 plan-review/human/recompile/cancel UoW，以及 generic/owner cancel、
  retry/requeue/resume/timeout/rebuild/首次 `succeeded|failed_terminal` result-terminal 等所有 strict D3
  dispatch-invalidating command transition，以及 D3 OperationRun
  terminal/cancel/retry/requeue/resume/reset/rebuild/recovery，先按
  `d3-dispatch-v2` 的 immutable scope tuple + exact `coordination_plan_review_id` 取得同一有界 transaction
  advisory coordination lock，再按 operation root →
  optional plan/review/gate → all participating `workflow_commands`（确定序，source/record/resume/supersession/
  current owner/idempotency target）→ intent/current predecessor →
  ActivityRun/Attempt → optional grant/cost exposure 的唯一全局顺序锁行；未触及 aggregate 只可跳过、不可逆序；
  dispatch、Stage B、terminal、record、resume_after_grant 与 control 都通过 closed typed-context centralized
  phase-parameterized business predicate exact-compare review/gate revision、
  `human_transition_pending`、decision source event、intent phase、grant issuance 与 current claim。lock busy 是
  typed zero-write；只有 heartbeat occurrence 与 read-only terminal exact replay 是 command-row-only 窄例外；
  control 先提交则零 send，dispatch 先提交则只留下已授权 exposure，晚到 response 进入
  shared quarantine repository 的 insert-once immutable identity/digest + typed cost reconciliation，永不到达
  record/domain apply；
- **terminal identity 与 heartbeat occurrence**：strict-D3 source/record 首次 `succeeded|failed_terminal` result UoW 取同一 coordination lock，
  在同一事务写 command terminal status + nullable-as-pair canonical outcome digest/terminal event id、append exact
  event 并清 token/lease；event 建 scope+existing operation_id+command+event id+digest composite unique identity，
  command terminal pair 以 `MATCH SIMPLE DEFERRABLE` composite FK 引用；本地 `CHECK` 只校验
  null-pair/status/digest shape，不假装跨表检查：它强制 pair 同 null/同 non-null，故 pair 存在时其余 key 全非
  null，由 FK 精确验 event；`MATCH FULL` 会错误拒绝
  正常 null terminal pair。该单向 FK 不证明 event→terminal command 的反向存在性；terminal repository 单 PG
  UoW + injected-failure rollback 才保证不提交 orphan event。pair 在 result-terminal 期间不可变且不可覆盖，只有 registered reopen 可在 requeue 前
  清除；future real-PG DDL 必须证明 null pair 可写、half-null/missing event 被拒、exact event 接受且不可 orphan。
  transport-backed terminal 还必须 exact-bind 已提交的 dispatch exposure、physical-call row、provider-call id、
  `ModelInvocationEnvelope` identity/digest 与 durable `TransportResponseReceipt.response_occurrence_id` + canonical
  response/result digest + result artifact ref/digest；terminal typed outcome 的 exposure `result_ref` 唯一等于
  receipt result-artifact ref（缺席为 canonical empty），result digest 与 artifact pair 必须 exact-copy/exact-equal
  receipt；same-result/different-result-ref、same-result/different-artifact 或 half-pair 均 fail closed。provider delivery id 优先，缺失时由 durable inbound receipt get-or-create stable occurrence，禁止每次回调
  重铸。D0 不允许 `ModelInvocationEnvelopeV1` 伪装 pre-result transport/protocol failure；committed-exposure
  pre-response closed outcome 只能使用 transport repository 的 typed `TransportAttemptFailureReceipt` +
  `attempt_failure` terminal variant，绑定 registered failure code/spec/canonical digest、required failure artifact、
  claim/business pins 与 committed exposure/wire-call state，且 result/envelope/response receipt complete-none。只有
  terminal retry disposition 可写 `failed_terminal`；若之后再有 late response，只能进 quarantine。registered
  non-transport result 或 proven pre-call/no-send terminal 只能由 registry `NoExposureTerminalSpec` 授权 complete-none
  `no_exposure` variant；caller flag/null/猜测不可选路。checked-in `TERMINAL_PROVENANCE_SPECS` 是
  `TransportResponseSpec|TransportAttemptFailureSpec|NoExposureTerminalSpec` 的唯一 SOT，closed fields pins
  command/stage/status/event/outcome 与 variant digest；preflight 对
  `(command_type, stage_id, terminal_transport_variant, terminal_status, terminal_event_type, terminal_outcome,
  terminal_reason if exposure, failure_code if attempt_failure)` 强制恰好一个 applicable entry；failure
  spec pins failure-code→`retryable|terminal` + `retry_policy_revision`。response/failure receipt 与 exposure/
  attempt-failure terminal、retry transition 共锁同一 exposure；response 先赢拒 failure/retry，retry 先赢先推进
  epoch，后到 valid response 只进 response quarantine，failure terminal 先赢亦同。no-exposure branch 不锁一条
  必须不存在的 exposure；它持 common coordination/command locks，而 dispatch creation 也必须持这些 locks，故
  在锁内证明 exposure absent 后才 terminalize。valid D0 response envelope 即使
  `terminal_reason=length|content_filter` 仍生成 response receipt；只有 incomplete/truncated wire 或 protocol parse
  failure 才生成 attempt-failure receipt。stale already-authorized exposure 可留 response/failure receipt 作
  cost/audit，均不授权 apply；protocol failure 本身不进 response quarantine。exact
  status+digest+event/idempotency replay 零写，不同 digest/event 返回
  `terminal_replay_conflict` 零写。heartbeat
  使用 repository-issued occurrence id/sequence；同 occurrence replay 不续租，新 occurrence 才可推进 expiry；
- 竞态电池（批验收硬项，十二项）：人工确认 vs 机器到达、cancel vs 回调、retry 子 vs 父晚到、
  重编译 vs 旧结果、双机器并发 CAS、cancel 后晚到 apply、timeout 后晚到 apply、
  **generic retry 重排队后旧 attempt 晚到 apply**、**cancel 与 transport dispatch 线性点竞态**
  （cancel 先赢零 send；dispatch 先赢仅允许 in-flight，结果零应用）、**plan recompile vs dispatch**、
  **human decision vs dispatch**、**retryable attempt failure vs valid response**（response 先赢拒 retry；retry
  transition 先赢推进 epoch，response 后到只 receipt+quarantine）。

## 5. 「人永远赢」

机器路径走 §4c record 全条件 CAS + §2.2 apply 链。**人工确认（v7 修 R6#3 的窗口）**：
review owner 先取得 §4c 与 dispatch 共用的有界 coordination lock，再在人工决定 UoW **同时原子地**：
记录决定 + 发决定事件 + 推进 review/gate control
epoch + 在自己的聚合上装 fail-closed 的 `human_transition_pending` 态（该态本身 blocking——
窗口期内终审/commit 不可能凭旧 canonical 通过）→ reducer 计划验证 owner supersession 命令
（写 human_confirmed + generation+1 + supersede intent + superseded delta）→ 域事件 → reducer →
apply 命令清 pending 态并更新 gate。commit owner 的 canonical 复查（§2.2）同时比对最新人工决定
事件 id 与验证行 provenance 一致。R-019 边界不变。

## 6. 裁决 schema 与调用信封（v4 收紧证据 provenance，round-3 #5）

- **模型作者字段**（`company_identity_adjudication_v1`）：candidate_id、identity_relation 枚举、
  **evidence_ids[]（只允许引用 owner 提供的证据 bundle 内的 id）**、conflicts[]、confidence_label、
  confidence_basis。模型输出中出现 URL/domain/kind/provider/usage/fallback 字段一律忽略并记协议
  偏差。
- **owner 侧解析**：evidence_ids 逐一对照**不可变的 intent/attempt 证据 bundle**（bundle hash 绑
  入调用信封）resolve——URL/可注册域/kind 全部服务端派生；引用不存在的 id ⇒ needs_human。
  独立性计算（§7.3'）只用服务端记录。
- **服务端调用信封 = 单一物理 schema `ModelInvocationEnvelope`**（v6 收编 R5#8——不再散指
  "D0 §2.3"：那里定义的是请求执行上下文，本信封是**独立命名的不可变结果侧契约**，D0 与 D3
  同一物理定义一处落库，含 ref **与** digest 两者、tenant/permission/policy 身份、command/attempt
  因果、provider 响应身份、result artifact ref+digest、成本暴露行引用；经 action/command/attempt/
  结果槽/journal 与两侧接受 CAS 全链绑定）。**canonical 字段清单以 D0 §2.2 为唯一定义处**（sweep
  修正——两处各自列举即两个 schema），且必须含 `terminal_reason`：非可授权终态
  （{end_turn, tool_calls} 之外）在 D3 接受谓词同样 fail-closed 转 needs_human。这里的 valid envelope
  `terminal_reason=length|content_filter` 是已收到的 response（只是不允许 auto-confirm），不得误报成 transport
  failure；只有 incomplete/truncated wire 或 protocol parse failure 才没有 valid envelope 并走
  `TransportAttemptFailureReceipt`。本节只列裁决侧补充语义：
  provider、requested/response/effective model（精确匹配）、`model_identity_provenance`、
  provider call id、route/api_style + route revision + **effective_route_snapshot digest**、
  bounded usage（provider-neutral `ModelUsage`；`OpenAIModelUsage` 仅兼容别名）+ usage_status、fallback/circuit
  证据、evidence bundle hash、**canonical response/result digest + 不可变 result artifact ref**
  （v5 补 round4#11——信封与解析产物绑定，防有效信封配错输出）。接受 = 两半同过；
  §4c CAS 同时比对 result artifact / provider call / bundle hash / attempt 身份四方一致。
- 身份 ≠ 人群 scope：`confirmed_company_scope` 仍归 plan review。

## 7. 确定性接受谓词与统计门（v4 恢复全文）

auto-confirm（Phase 2）必须全部满足，任一失败 ⇒ needs_human（弃权优先）：
1. canonical URL 形态：重定向解析后 `https://www.linkedin.com/company/<slug>/`，slug 合法；
2. 候选身份稳定：全部采信证据解析到同一 canonical slug；
3'. **证据独立性（服务端计算）**：≥2 个不同可注册域来源，且必须包含 {公司 official domain 主动
   链接该 LinkedIn URL} 或 {两个彼此独立的非 LinkedIn 权威来源一致指向}；搜索摘要与其抓取目标 =
   同一来源；
4. 新鲜度窗口（默认 30 天）内抓取；
5. 零未决冲突 + 歧义弃权（≥2 候选过谓词 ⇒ needs_human；不复用 `normalize_company_key` 松归一化）；
6. parent/suborg：identity_relation 恰为 same_company；parent_of/subsidiary_of/related_brand ⇒
   永不 auto；
7. 模型半 + 信封半全过（same_company + high + fallback=none + 身份匹配 + bundle hash 匹配）。

**统计门**：零观察误确认且分母 ≥ 120（Wilson 95% 单侧上界 ≈3.0%）；shadow 期生产一致率同报；
Phase 2 = owner GO 后逐行 revalidation + promotion 命令，旧 shadow 行不随开关生效；先 K 个 plan
有界 canary + 强制回看。评测集：盲标注正例 + 对抗负例（同名异司/改名/被收购/多语言/SEO 污染/
重定向陷阱/母子公司）。

## 8. 项目级恢复与预算（v4 修正计费诚实性，round-3 #7）

- 稳定项目 id：query_key=`q_<hash>`（R-010 对齐）、candidate_key=canonical slug hash、
  fetch_key=重定向解析**前**规范化 URL hash（final URL 另记证据属性）、judge_call_key=
  (candidate_key, evidence_set_hash, prompt_version)。
- 语义：效果 at-most-once（幂等 apply + 去重键）、attempt at-least-once；
- **计费口径（v6 对齐 D0 成本台账，R5#6）**：worst-case 预留 + 每次物理调用一行暴露记录，状态
  `prepared → dispatching → sent → confirmed | uncertain | no_call`——**任何 wire 写之前先落
  `dispatching`（保守可能已发送态）**；只有可证明的 transport 前中止才转 `no_call`；crash 时
  停在 dispatching/sent 的一律按 `uncertain` 以 worst-case 预留计，直至 provider 对账或保守
  消耗。不存在"attempt 创建即记实际计费"的口径。可补 grant 的 step/cost/search envelope 耗尽 ⇒
  `awaiting_budget`；hard envelope/policy 已禁止再 grant ⇒ `needs_human`；不可重试执行失败 ⇒ `failed`；
  deadline/`max_wall` ⇒ `timed_out`。全部 fail-closed，永不静默超支或把 hard exhaustion 标成可恢复。
- cancel 前置停止；stale application 或 business mismatch 只能是 typed
  `not_applied(reason=stale_claim|business_precondition_conflict)` 且 domain/attempt/intent/
  event/command/source/result 零写；只有 dispatch authorization 已提交的 in-flight transport response 才可按
  §4c 进入 shared quarantine repository（绑定 exposure/call id/response digest/attempt/generation/epoch）。provider
  delivery id 存在时作为 stable delivery identity；否则 registered protocol 使用 replay-stable
  `body:<canonical_response_digest>:ordinal:<protocol_ordinal>`，durable inbound `TransportResponseReceipt`
  get-or-create。`response_occurrence_id = sha256(length_delimited("transport-response-occurrence-v1",
  scope_digest, dispatch_exposure_id, canonical_delivery_identity))`，redelivery 必须复用。late-response-v1
  idempotency identity 精确为
  `late-response-v1:<scope_digest>:<dispatch_exposure_id>:<canonical_delivery_identity>`（checked-in length-delimited
  component encoder）；canonical result digest 作为
  exact-replay equality；同 identity digest mismatch 是 collision。post-network owner 固定按
  exposure→receipt→quarantine/cost-axis lock/write，绝不反向进入 operation/command/intent/domain，也不形成 send/apply
  authorization，
  只供审计与成本对账，永不成为 command result/reducer/domain 输入。repository 是唯一 SQL owner，typed
  insert/CAS entrypoints 写集分离：immutable exposure/response identity + digests insert-once，`cost_state` 与
  `retention_state` 两轴各自 monotonic；不得称 whole row append-only，也不得让 result acceptance 直接修改
  两轴。其精确枚举为
  `cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain | reconciled_no_call` 与
  `retention_state: retained -> purged_tombstone` 是互不重置/互不阻断的正交状态机，禁止用单一 disposition
  混合表达；payload 到期删除不妨碍 digest-only 保守对账，成本对账也不得延长 retention。重试限额/退避/
  熔断显式。

## 9. 验收与激活边界

回放集 + §7 对抗集（scripted 转写）；指标：人工编辑率下降、误确认=0（带分母）、歧义 100% HITL、
invocation-count 单 owner、机器写人工字段 0 命中、ingress 伪造引用被拒、§4a 迁移桥 precedence
preflight、§4b 迁移表注册校验、§4c 十二项竞态电池、ClaimAuthority 非伪造/owner mismatch/one-use
selection consume、predecessor generation/control-source-event ABA、source/record 双身份+current source row
  锁验、所有 cancel 共用 dispatch coordination、terminal `MATCH SIMPLE DEFERRABLE` composite-FK native-PG
  null/half-null/exact/mismatch/non-orphan acceptance、transport-backed terminal exposure/physical-call/provider-call/
  ModelInvocationEnvelope/response-occurrence/result-ref binding、typed `TransportAttemptFailureReceipt` +
  `attempt_failure`、registered `no_exposure` 与 `TransportResponseSpec` 三 variant、unique applicable-policy pin/
  historical retention、no-exposure absence under common lock、retryable-failure/response 两序 race、valid
  `length|content_filter` response 与 truncated wire/protocol failure 分类，以及 heartbeat occurrence replay、
  stable late-result occurrence/idempotency collision、quarantine exact cost/retention enum 双轴与
**§8 计费对账 parity（预留-实际-对账三方一致）**；机械边界扫描零直写；lane green-modulo-ledger；
mypy 81 errors / 4 files 棘轮。
rollout 顺序：先 migrations/registries、bootstrap/strict-D3 双 manifests 与 dormant bootstrap factory+predicate，
再 scoped-session specialized Stage-B creator + credential-free committed replay/exact-copy，
最后按 Stage A→business evaluator→normal D3 paths 激活；consumer 不得先于 authority/receipt/registry dependency。
激活边界：C2+C3+TD-4+`skip_plan_review` 限 operator；live 等 owner 凭据授权。

## 10. 修订史与 findings 覆盖映射

- D3b 决策锁（2026-07-14，enclosing commit 待固定）：§4c 的抽象 claim generation/token 已收口为
  durable operation→existing command operation_id→ActivityRun→ActivityAttempt scope chain、exact-selection 后
  factory-minted one-use ClaimAuthority（selection generation/expiry/consumed id）、Stage-A current ClaimIdentity
  mint、physical predecessor tuple Stage-B/async shared CAS、`final_adjudication` 唯一 record planner、source current
  command + independent record claim 双锁验、包含 cancel 的跨 owner dispatch coordination、terminal composite FK
  digest/event replay、heartbeat occurrence 与 quarantine cost/retention 双轴；round5 fixed-forward 进一步锁定
  positive-BIGINT coordination lineage、四项 typed physical predecessor columns（strict additive 20）、closed typed
  six-phase business context、all-command deterministic lock segment、record+terminal 单事务、post-gate-apply resume-v2、
  current-selection consumed slot、transport/no-exposure terminal binding、closed not_applied reasons 与 stable response
  occurrence/idempotency；round6/7 fixed-forward 再锁 scoped-session causal/idempotency aggregate、唯一 issuer、
  response `result_ref` 唯一派生、typed `TransportAttemptFailureReceipt` + `attempt_failure` 与 registry
  `NoExposureTerminalSpec`，以及 strict-D3/non-D3 validation denominator 分离；round8 fixed-forward 再拆出
  cycle-free scoped-session bootstrap contract、response/failure/no-exposure 三 variant policy、creation-time policy
  pin、no-exposure absence lock 与 retry/response race；stale/business CAS 均零持久写。
  action-root durable-scope gate（Plan §6#6、无 OB-ID）与 R-019、OB-10.1/10.2/10.3/10.4 继续 open；served
  Agent tool population=`0`。round4 fresh local advisory=`NO-GO`（P0/P1/P2/P3=`0/3/2/0`），独立 semantic
  audit=`NO-GO`（`0/5/4/0`）；round5 semantic=`NO-GO`（`0/5/2/0`）与 fresh broad=`NO-GO`
  （`0/3/3/0`）、round6 semantic=`NO-GO`（`0/1/2/0`）与 broad=`NO-GO`（`0/4/0/0`）继续作为
  fixed-forward 输入；round7 semantic=`NO-GO`（`0/4/0/0`）与 broad=`NO-GO`（`0/4/2/0`）也已
  fixed-forward；round8 semantic=`NO-GO`（`0/1/0/0`）与 broad=`NO-GO`（`0/6/1/0`）亦作为
  fixed-forward 输入。均不是 formal verdict；两次 round9 local advisory 因 Codex operator usage limit 在 final
  response/artifact 前终止，故**没有 verdict**，其直接 partial findings 已 fixed-forward；fresh non-author local
  re-review 与 pinned formal review 仍 pending，且仅对 Live/signoff fail closed，不冻结后续 non-live batch。
  current author evidence=`32/49/58/81` + diff clean，不能写成 final GO。物理
  migration/产品码仍未落地，详见
  `TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md`。
- v1（`23a2b05`）→ 评审 24 findings（提取件 `20260713T112818Z_*.extracted-reference.md`）。
- v2（`ffdfa7c`）→ 有效 artifact `20260713T122908Z_*`（NO-GO：16 新 findings + 持久化事实更正）。
- v3（`656b368`）→ round-3 提取件 `20260713T125255Z_*.extracted-reference.md`（NO-GO：17 findings，
  其中 4 critical 类）。
- v4（`4745e9c`）→ round-4 **有效 artifact** `20260713T131447Z_*`（NO-GO，阻断集 = 新 #1、#3-#7；
  re-raise 明示不构成裁决依据）。
- v5（`88bc1c6`）→ round-5 **有效 artifact** `20260713T133324Z_*`（NO-GO 8 findings）。
- **v6（本版）round-5 覆盖**：#1→§2.2 watermark（sweep 后改为单调恒推进）+ 反向失效 + commit
  canonical 复查；#2→§4c terminal 事件谓词 + control epoch；#3→§2.1 不可变签发 +
  supersede-with-transfer；#4→§2.1 单写者 revoke + transport 前扩展 CAS；#5→（D0 §2.5 消费
  CAS）；#6→§8 dispatching 计费态；#7→本文/上层计划编舞逐字对齐 + §5 人工确认两 UoW 重述；
  #8→§6 信封 canonical 定义收归 D0 §2.2（含 terminal_reason）。另：九类不变量自查 8 blocker
  当场修复（watermark 单调、grant 枚举补 superseded、槽 CAS 补 epoch、信封单一定义、版本标签
  归一），52 obligation 见 `TRACK_D_INVARIANT_SWEEP_2026-07-13.md`。
- **v5（本版）round-4 阻断集覆盖**：#1→§2.2 record/apply 单写者拆分（验证 owner 写验证聚合、
  review owner 写 gate，域事件+reducer 连接）；#3→§4c workspace 键恢复进身份与全条件 CAS；
  #4→§4b expire 走定时事件+域 owner 扫描，读者零写零 enqueue；#5→（D0 §2.5 槽围栏）；
  #6→§2.1 grant 一等 PG 记录 + revoke 生命周期 + 三活跃 CAS；#7→§6 共享模型调用契约（快照
  digest 绑入 intent/attempt/信封/artifact/CAS）。re-raise 处置：#2→§4c claim generation 物理
  实现诚实化（新列，migration 义务）；#8→§4a bridge diagnostic-only；#9→§4b 迁移表补两行 +
  历史/现态分离；#11→§6 result digest 绑定；#13→（D0 §2.3 成本台账分账）。
- **v4（本版）round-3 覆盖**：#1→§4a（写入方全列 + quarantine + 快照路径入 preflight）；
  #2→§2.2（事件驱动链、apply owner 写、session-created 事件后才计划验证）；#3→§2.1
  （identity_search_budget_grant 部分决定 + commit owner 原子复查）；#4→§4c（物理执行身份绑定 +
  单 UoW 全条件）；#5→§6（evidence_ids only + owner 解析 + bundle hash）；#6→§4b（迁移表全量 +
  expire 命令 + 矩阵恢复）；#7→§8（每物理调用一笔 + worst-case 预留 + 对账）；#8→本文自包含恢复 +
  上层计划同步（其 §2 D3）；#16→§2.1（provider 能力派生控制政策）；
  非阻塞更正三则→§1（:554-594 为 writer）/§6（ModelUsage）/上层与 D0 术语统一。
- v1/v2 轮已结项映射见 git 历史中的 v2/v3 版本 §10（内容已并入本版正文，不再另表）。
