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
    直写；**pre-transport 授权 CAS 扩展**（R5#4）：除 session+intent+grant 三活跃外，同谓词
    fence 当前 OperationRun/WorkflowCommand 非终态 + claim generation/control epoch + attempt +
    精确 grant 签发——cancel 后的异步收敛窗口内 provider 调用被 epoch 失配挡住；
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
  → request owner 创建 review session（id 此刻才存在），发 session-created 结果事件
  → reducer（置信 < high 时）计划 company.identity.verify.evidence（携带 session id + fingerprint + intent）
  → 验证 terminal 结果事件（判别化，v8 修 R7#3——reducer 按结果变体路由，消除"全部事件到
    record"与"只有 final_adjudication 到 record"的矛盾）：
      final_adjudication        → record（授权分支）
      evidence_insufficient     → grant 可用 ⇒ search.expand；不可用 ⇒ record（非授权分支，
                                  intent 迁入 durable `awaiting_budget` 态；此后 grant 授予事件 →
                                  reducer 计划后继 verify.evidence 子命令【phase gen+1，同 root intent】）
      失败/超时/needs_human 类  → record（非授权分支）
  → company.identity.verification.record（owner = 验证 owner；幂等键 =
    `record:<runtime_namespace>:<workspace_id>:<intent_id>:<phase_generation>`——v7 修 R6#1 的
    命令类型命名空间化 + v8 补 namespace/phase；**三分支语义（v8 统一 R7#3/#4）**：
    授权分支 = 全条件 CAS 过 + manifest 完备 ⇒ shadow_would_verify；**非授权分支 = 同样过身份/
    围栏 CAS 但落 needs_human/failed/timed_out 域写**（record 是这些迁移的唯一 owner，非授权
    结果也是真实域状态）；`not_applied` **只保留给** stale 身份/generation/epoch/hash 失配——
    不再与 needs_human 混用）
  → record owner 单 UoW：§4c 全条件 CAS，只写自己的聚合（verification 行 + intent 迁移 +
    not_applied/applied 证据），发 company_identity_verification_recorded 域事件
  → reducer 对 record 的**授权与非授权分支结果**（不含 not_applied）计划
    plan_review.identity_result.apply（owner = plan review owner；幂等键 =
    `apply:<runtime_namespace>:<workspace_id>:<session_id>:<源域事件 id>`——v8 修 R7#2：
    namespace + provider_mode 绑入 apply 的事件/命令身份/幂等 scope/gate 行与 **CAS 每一条**
    【含 expiry/supersession/human 路径】，跨模式污染 preflight 覆盖 apply；v7：各次转移各有
    源事件、各自成键；not_applied 永不触发 gate 更新）
  → apply owner 单 UoW（v6 补 R5#1 的 gate 侧围栏）：CAS 于 {workspace + session 当前 revision +
    source 事件 id 匹配 + generation 规则}；**watermark 单调、每次成功 apply（含 blocking 方向）
    都推进**（sweep blocker 修正——阻塞若不推进 watermark，晚到的旧 clearing 可在
    blocking(gen N+1) 之后以 gen N+? 通过）：clearing 仅当 generation > watermark 生效；
    blocking 当 generation ≥ watermark 生效并推进 watermark——晚到旧 clearing 永被拒
  → 发 apply 结果事件
```

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
   类型化 phase 结果 = `final_adjudication | evidence_insufficient | needs_human_budget`；
   `evidence_insufficient` 且 grant 可用 ⇒ 其**结果事件**经 reducer 计划 `search.expand`；
   expand 终态事件 ⇒ reducer 计划**后继 `verify.evidence` 子命令**（root intent + 单调
   `phase_generation`+1，每个子命令各绑自己的 claim/attempt——intent 结构升级为
   root-intent + per-phase 子绑定）；`needs_human_budget` ⇒ terminal needs_human（gate 附
   「可授予搜索预算」提示）。**只有显式 `final_adjudication` 结果才允许计划
   `verification.record`**——中间 phase 结果结构上到不了 record；
4. **出口**：谓词（§7）+ 裁决全过 ⇒ Phase 1 记 `shadow_would_verify`（非授权态，gate 仍需人一键
   确认，同时记 shadow 统计）；Phase 2（§7 统计门 + owner GO + 逐行 revalidation + promotion 事件）
   才产 `verified_accepted`；其余（谓词失败/uncertain/fallback/冲突/预算步数耗尽/超时）⇒
   `needs_human`（gate 阻塞 + 证据卡：候选 URL、来源域、裁决、rationale、冲突）。

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
决定整行替换**（历史留审计链），不存在"superseded 行原地复活"。矩阵扩展到全部共享字段（含
validity/policy version/decision source/grant/物理因果列，补 derivation 与 migration-status 两列）
为实施批义务（见上层计划义务清单）。

**owner 矩阵（v4 恢复并补列）**：
| 字段 | owner | source of truth | 允许值 | 消费方 | 禁止消费方 | fallback | preflight |
|---|---|---|---|---|---|---|---|
| verification_state | 验证 owner | 本表 | 迁移表枚举 | 两前门/gate/UI | 意图抽取、公共 ingress | 缺行=未验证 | 状态迁移注册校验 |
| resolver=`agent_self_verified` | connectors 解析链 | 本表 generation 行 | 固定值 | identity 消费方 | 执行偏好 ingress | 不适用 | ingress 拒收守卫 |
| 证据卡 candidates[] | 验证 owner | result artifact | 有界列表 | gate UI | 模型输入回流 | 缺失=仅文案 | artifact schema 校验 |
| decision_generation / intent | 验证 owner | 本表 | 单调 int / §4c 枚举 | CAS 写路径 | 一切读方 | 不适用 | 竞态电池 |
| canonical_url/slug | 验证 owner | 本表（推导规则版本化） | URL 形态谓词 | 前门/执行守卫 | — | 缺=needs_human | 推导规则金测试 |
| 迁移桥命中 | bridge | 指标表 | 计数 | deletion preflight | — | — | 零命中窗口断言 |

### 4c. verification intent（v4 绑定物理执行身份，round-3 #4）

`verification_intent`：`(workspace_id, intent_id)` 身份（**v5 恢复租户键**——round4#3 指出 v4
重写时丢失；workspace 等式进入索引、命令/事件引用、证据 bundle、repository 授权、幂等 scope 与
下述 CAS 的每一条）；绑定 **operation_run_id + review_session_id + workflow_command_id +
claim_generation + activity_attempt_id**。**claim_generation 的物理实现（v5 诚实化，round4#2）**：
pinned `workflow_commands` 只有会被 generic retry 重置的 `attempt` 计数——本设计**要求新增
永不重置的单调 claim generation/token 列**（migration 项，实施批义务；claim 时 +1、retry 不清零），
intent 在 **claim 完成且 ActivityAttempt 创建之后**绑定；generic 控制面（cancel/retry/resume）
只动 runtime 现态，域侧 supersession 经 owner 控制事件 → reducer → 域命令完成（不假设控制面
直接原子改域行）。存储 plan bundle hash + fingerprint + `accepted_policy_version` +
route/schema revisions + **effective_route_snapshot digest（§6 共享契约）** + expected
decision_generation；`intent_state ∈ {pending, awaiting_budget, applied, cancelled, timed_out,
superseded}`（v8 补 `awaiting_budget` 入枚举与迁移：pending→awaiting_budget【record 非授权分支，
evidence_insufficient 且 grant 不可用】；awaiting_budget→pending【grant 授予事件 → reducer 计划
后继 verify.evidence，新 phase generation 绑定】；awaiting_budget→cancelled/timed_out/superseded
【控制转移同 pending——永无 grant 时经 timeout 收敛，不悬挂】）。
- retry、resume、cancel、timeout、plan 重编译、人工决定——每种经上述事件→reducer→域命令路径
  **原子 supersede 旧 intent**（含 generic retry：requeue 触发的域命令在同 UoW 铸新 intent）；
- **record 命令 owner 的单 UoW 全条件 CAS**（v6 修正 R5#2 两向问题）：`workspace_id 匹配 AND
  intent_id 匹配 AND intent_state='pending' AND claim_generation 匹配 AND activity_attempt_id
  匹配 AND **源命令的 terminal-success 结果事件身份 = intent 记录的预期事件**（v6：源命令正常
  完成后 record 才被计划，故谓词认"预期终态事件"而非"命令非终态"——后者会拒绝一切正常完成）
  AND stored_fingerprint 匹配 AND decision_generation=<expected> AND policy/schema/route/snapshot
  pins 匹配 AND review_session 当前仍 pending-review AND verification_state NOT IN
  (human_confirmed) AND **（v7 修 R6#5）所属 OperationRun 当前非终态（terminal winner 检查）AND
  plan/review revision（bundle watermark）与 intent 存储值相等 AND 存储的 control epoch 相等**
  ——cancel/timeout/requeue/rebuild/重编译在各自 commit 的同一 UoW 内**同步推进**对应围栏
  （epoch/revision），旧 record 在异步 supersession 落地前即已被挡；AND **（v7 修 R6#6）
  adjudication-set manifest 全终态聚合 hash 匹配**（见下）——全过则原子：intent→applied +
  verification 行迁移 + applied 证据事件；任一失配 ⇒ 全不动 + `not_applied` no-op 证据事件；
- **多候选完备性证明（v7，R6#6；v8 修 R7#4 的产生机制矛盾）**：owner 在裁决开始前**预创建**
  manifest 全部条目（键 = workspace/intent/phase generation；服务端枚举的 expected candidate
  ids）；每条目经裁决调用终态化，**未决条目由 owner 的超时/对账命令终态化**（标 unresolved）；
  完整 terminal bitmap 参与聚合 hash。**授权分支**要求 bitmap 全部 resolved-authorizable 且
  hash 匹配；存在 unresolved/非授权条目 ⇒ **走 record 非授权分支落 needs_human（真实域写）**
  ——不是 not_applied（后者只留给 stale 围栏失配）。"幸存者显得唯一有效"仍被结构性堵死，
  且 needs_human 有了明确的产生者；
- **retry ABA 窗口封堵（v6，R5#2）**：requeue 在同一控制 UoW 内先递增命令的 **durable control
  epoch**（与 claim generation 分立、requeue 即变），intent 记录 epoch——旧结果在"已 requeue、
  未重 claim"窗口内因 epoch 失配即拒；后继 intent 只在新 claim + 新 ActivityAttempt 创建事务内
  铸造；
- 竞态电池（批验收硬项，八项）：人工确认 vs 机器到达、cancel vs 回调、retry 子 vs 父晚到、
  重编译 vs 旧结果、双机器并发 CAS、cancel 后晚到 apply、timeout 后晚到 apply、
  **generic retry 重排队后旧 attempt 晚到 apply**。

## 5. 「人永远赢」

机器路径走 §4c record 全条件 CAS + §2.2 apply 链。**人工确认（v7 修 R6#3 的窗口）**：
review owner 的人工决定 UoW **同时原子地**：记录决定 + 发决定事件 + 推进 review/gate control
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
  （{end_turn, tool_calls} 之外）在 D3 接受谓词同样 fail-closed 转 needs_human（截断/过滤的
  judge 输出不可参与 auto-confirm）。本节只列裁决侧补充语义：
  provider、requested/response/effective model（精确匹配）、`model_identity_provenance`、
  provider call id、route/api_style + route revision + **effective_route_snapshot digest**、
  bounded usage（`OpenAIModelUsage`，`model_provider.py:36-55`）+ usage_status、fallback/circuit
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
  消耗。不存在"attempt 创建即记实际计费"的口径。信封耗尽 ⇒ terminal needs_human，永不静默超支。
- cancel 前置停止；晚到结果按 §4c 隔离；重试限额/退避/熔断显式。

## 9. 验收与激活边界

回放集 + §7 对抗集（scripted 转写）；指标：人工编辑率下降、误确认=0（带分母）、歧义 100% HITL、
invocation-count 单 owner、机器写人工字段 0 命中、ingress 伪造引用被拒、§4a 迁移桥 precedence
preflight、§4b 迁移表注册校验、§4c 八项竞态电池、**§8 计费对账 parity（预留-实际-对账三方一致）**；
机械边界扫描零直写；lane green-modulo-ledger；mypy 87 棘轮。
激活边界：C2+C3+TD-4+`skip_plan_review` 限 operator；live 等 owner 凭据授权。

## 10. 修订史与 findings 覆盖映射

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
  非阻塞更正三则→§1（:554-594 为 writer）/§6（OpenAIModelUsage）/上层与 D0 术语统一。
- v1/v2 轮已结项映射见 git 历史中的 v2/v3 版本 §10（内容已并入本版正文，不再另表）。
