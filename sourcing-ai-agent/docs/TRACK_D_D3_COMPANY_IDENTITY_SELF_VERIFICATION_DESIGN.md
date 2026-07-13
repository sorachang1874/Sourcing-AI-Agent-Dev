# Track D — D3 批级设计：公司身份自验证 loop（第一垂直切片）

> Status: Cross-model design input for owner review（v3 2026-07-13，作者 = Claude Fable 5；只设计、不改码）。
> **v3 修订**：按有效 runner artifact `runtime/reviews/20260713T122908Z_*`（NO-GO：16 条新 findings +
> prior-24 复核）修订——含一处 v2 事实错误的更正（身份**已有**文件级持久化）。覆盖映射见 §10。
> 上层计划：`TRACK_D_AGENT_RUNTIME_PLAN.md` §2 D3。TD-2/5/6/7 裁决见上层 §5。
> **不依赖 model_native_search、不依赖 D0 tool-calling、不依赖 D2。**
> 实施前按 handbook 纪律重新 Scout（基准 HEAD `ffdfa7c`）。

## 1. 现状精确描述（v3 修正持久化与优先级两处事实）

**身份解析优先级**（v3 修正：manual override 是**前置 pass**，不是 `resolve_company_identity` 内的
分支）：manual override 前置 pass → runtime/builtin（**读全局注册表**）→ legacy slug（`:219`）→
observed exact-match（`resolver="observed_candidates_exact_match"`，medium，`:354-391`；
`normalize_company_key` 剥掉全部非字母数字，`X.AI/X-AI/xai` 碰撞、首个命中即胜）→ observed 模型
判定（只收 same_company + high/medium）→ heuristic slug（low）。

**执行期付费路径 + 既有文件持久化**（v3 更正 v2 的"不持久化"错误）：`acquisition.py:970-1051`
`_resolve_company` 在「非 manual override + 低置信 + 无 local asset」时（守卫 `:985-990`）做
search（`:4078-4100`，异常静默吞、fail-open 回 heuristic）+ 模型判定；成功后写 `identity.json`
并更新**全局文件注册表 `company_identity_registry.json`**（`:1017-1031`），后续解析经
`company_registry.py:210-234,554-594,641-681` 回读。该注册表：文件制、跨 workspace 全局共享、
无 schema 版本、无租户、无 generation、无审计——**是本设计必须收编的既有持久 owner**（§4a）。

**plan review gate 与身份无关（结构性洞）**：`required_before_execution` 只在四处置 True
（`plan_review.py:54/:58/:78/:82`），身份置信度结构上进不了 gate；低置信身份今天直接 `ready`
执行（`orchestrator.py:53102-53116`）。W11 链的 gate 则**恒为 required**（
`acquisition_command_owner.py:1143-1152,1203-1223`，materialize 一次）。`skip_plan_review` 旗标
可整体绕过 legacy gate（`:52975/:53061`）。

**产品定义**：把「人去检索并确认 URL」变成「agent 检索并给证据，人只在歧义时裁决」；v3 起点
仍比现状更严：低置信身份从「无人拦截」变为「必须有人或已验证结果放行」。

## 2. 形态：W11 图内的类型化命令 + plan 期解析前移

### 2.1 新注册项

- 验证 owner 模块：`company_identity_verification`。
- 命令与政策（v3 修正 fetch 控制政策与两档审批的表达）：
  - `company.identity.verify.evidence`（Tier-1）：fetch 取证 + 模型裁决。**活动类型拆分**，
    控制政策按活动注册而非按命令一刀切：search 步 = `poll_cancel_late_result_quarantine`；
    **document fetch 步与模型 judge 步 = `fail_closed_until_terminal`**（对齐
    `DURABLE_EXECUTION_RUNTIME_CONTRACT.md:154` 与 `AGENT_OPERATION_CONTRACT.md:221` 的既有裁决）。
  - `company.identity.search.expand`（Tier-2）：付费检索。**审批表达**（v3 修正——两命令 + 单静态
    ActionSpec 仍表达不了两档）：Tier-2 的审批证据 = **plan review 人工批准事件铸造的预授权信封**
    （TD-5：N=3 可配置）——即人已经批过；信封耗尽后的追加检索是**独立注册的 approval_required
    action**（`expand_company_identity_search`，静态政策成立），批准后重铸信封。两条路径均满足
    「付费前有人工批准证据」，且每条 ActionSpec 政策恒静态。
- ActionRegistry：入口 action `verify_company_identity`（approval not_required + Tier-1 信封）+
  追加 action `expand_company_identity_search`（approval required）。
- EntityDelta kinds：`company_identity_verified` / `company_identity_verification_superseded`。

### 2.2 W11 接入与 durable join（v3 修正 critical #2）

- **时序**：`acquisition.plan.build` 产 plan 时做 plan 期廉价解析（只跑确定性分支：override 前置
  pass / §4 PG 读模型 / builtin / legacy / observed exact-match；不 search 不调模型）→ 先计划
  `acquisition.plan_review.request`（**review session 先于验证存在**，其 id 可被携带）→ 置信 <
  high 时计划 `company.identity.verify.evidence`（同 OperationRun、parent-command 因果、携带
  plan_review_session_id 与 fingerprint）。
- **terminal join**（不再"读模型顺便被看见"）：验证 terminal 时，owner 计划幂等命令
  **`plan_review.identity_result.apply`**（owner = plan review 所属 owner；幂等键 =
  verification_intent_id）——该命令在 reducer 内把结果重投影进对应 review session 的 gate payload
  （证据卡 + 身份 reason 状态迁移），source-event/parent-command/readiness effect/drain binding
  随 CommandTypeSpec 注册全量声明。W11 gate 恒 required 不变——apply 只更新 gate 内容与身份
  reason 状态；legacy 前门的条件 gate 由**同一共享 helper**从同一读模型计算身份 reason（两前门
  一个计算 owner，v3 修正 prior#2 的双 owner 不一致）。
- **gate reason**：`company_identity_unverified` 以 OR 组合进 `required_before_execution`；只清
  自己不清别人；`skip_plan_review` 绕过记录在案，hosted 激活前限 operator。

### 2.3 Loop 步骤与出口

同 v2（Tier-1 取证 → 裁决 → 不足且信封可用则 Tier-2 → 置信阶梯出口），出口状态改用 §4 的
两态拆分：谓词+裁决全过 ⇒ Phase 1 记 `shadow_would_verify`（**非授权态**，gate 仍需人一键确认）；
Phase 2（§7 统计门 + owner GO + 逐行 revalidation）才产 `verified_accepted`；其余一律
`needs_human`（gate 阻塞 + 证据卡）。

## 3. Provenance：服务端引用，不走公共 ingress（v3 修正 #6）

- **v2 的"结构化执行偏好"方案废弃**——执行偏好 allowlist（`execution_preferences.py:9-63,180-183`）
  今天会丢弃未知字段，若加入 allowlist 则客户端输入可伪造 `agent_self_verified`。v3：机器验证
  结果**只存在于服务端**（§4 PG 行 + result artifact），消费方（两前门、`resolve_company_identity`
  新分支、执行期守卫）经 **server-owned verification reference**（workspace_id + fingerprint +
  generation + decision source 绑定）在服务端 resolve；公共 ingress（plan/review/job payload）
  **禁止**携带该引用或其镜像字段（守卫测试：ingress 出现该字段即拒）。
- 三通道与优先级不变：manual override（人工，前置 pass 现状不变）> `agent_self_verified`
  （服务端 resolve）> 其余既有分支。执行期守卫（`:985-990`）扩展为两通道任一在位即跳过付费分支。
- **人工 provenance 持久化**（v3 新增）：人工确认写入 §4 行（state=human_confirmed + decision
  source=human + review session ref），后续 plan 消费该行时保持 human provenance——permanently
  区分 human/machine resolver 值，任何跨 plan 复用不得把 human_confirmed 重标为机器验证。
- agent 永不写 `target_company_linkedin_url`（守卫不变）；意图抽取白名单不扩展。

## 4. Current-state 读模型与 verification intent（v3 大改：#1/#3/#4/#8）

### 4a. 与既有全局文件注册表的关系（v3 新增，critical #1）

- **PG 表为 canonical**。既有 `company_identity_registry.json` 全链路收编：
  1. Scout 期 inventory（注册表读/写点全枚举：`acquisition.py:1017-1031` 写、
     `company_registry.py:210-234,554-594,641-681` 读族）；
  2. 一次性 backfill（文件注册表 → PG 表，`decision_source=legacy_registry_import`、
     confidence 原样、不授予 verified_accepted 态）；
  3. 文件注册表降级为 **report-visible migration bridge**：读路径先 PG 后 bridge（命中 bridge 记
     指标），写路径双写窗口后停写；deletion condition = bridge 命中率归零 + 一个发布周期，
     台账登记 bridge 行；
  4. **precedence preflight**：断言 PG 行存在时文件注册表不可能胜出；workspace 决定**永不**回写
     全局文件注册表（防 workspace→global 泄漏）。
- 共享 vs workspace 语义：PG 表 workspace-scoped；跨 workspace 复用属 shared-canonical 优化，
  显式推迟（内部产品单 org 阶段收益有限），推迟决定记录于此。

### 4b. 表契约（v3 补全 #8）

**PG-only**（无 SQLite 路径；migration 0002+ DDL + repository owner `store.repos.<domain>` 形态）：

`company_identity_verifications`：
`(workspace_id, company_fingerprint)` 唯一；`fingerprint_version`；`decision_generation` 单调；
`verification_state ∈ {pending, shadow_would_verify, verified_accepted, needs_human,
human_confirmed, failed, timed_out, superseded}`（v3：拆 shadow/accepted 两态，#4）；
`accepted_policy_version`（谓词/schema 版本，Phase 2 升版即失效重验）+ `valid_until`
（新鲜度窗口到期自动降 needs_human）；`canonical_url/slug` 及其推导规则版本；
`decision_source ∈ {machine, human, legacy_registry_import}`；`result_artifact_ref`（结果 schema
版本化）；**物理因果列**：source workflow_command_id / activity_run_id / attempt_id /
entity_delta_id；预算态引用（envelope 余额行）；审计时间戳。
**状态迁移表**（全量声明，实施时进注册期校验）：pending→{shadow_would_verify, needs_human,
failed, timed_out}；shadow_would_verify→{verified_accepted(仅经显式 promotion 事件+revalidation),
needs_human, superseded}；任意非 human_confirmed→human_confirmed；human_confirmed 终态（仅新
generation 可再变）；晚到/重复 apply ⇒ 行不变 + attempt 记 `not_applied` 终态 no-op。
**fast preflight**：字段-owner 矩阵 parity + 迁移桥状态 + 谓词版本一致性，进 contract lane。

### 4c. verification intent（v3 修正 critical #3——CAS 的比较对象）

新行（或同表 intent 列族）`verification_intent`：`intent_id` PK、workspace、fingerprint（**存储**
plan bundle hash + target/scope fingerprint，不只携带）、source command id、
`intent_state ∈ {pending, applied, cancelled, timed_out, superseded}`、expected generation。
- cancel / retry（子 run 铸新 intent）/ timeout / plan 重编译（fingerprint 变）/ 人工决定，**每一种
  都原子推进 intent 态**（cancelled/timed_out/superseded）；
- **机器 apply 的 CAS 全条件**：`WHERE intent_id=? AND intent_state='pending' AND
  stored_fingerprint=? AND generation=<expected> AND verification_state NOT IN (human_confirmed)`
  ——stale cancelled/timed-out/retry-parent/rebuild 结果全部 CAS miss ⇒ `superseded_not_applied`；
- 竞态测试清单在 v2 五项基础上加三项：cancel 后晚到 apply、timeout 后晚到 apply、retry 子 intent
  与父 intent 并发 apply。

## 5. 「人永远赢」（随 §4c 收紧）

机器 apply 走 §4c 全条件 CAS；人工确认 = 同一 UoW：human_confirmed + generation+1 + supersede
活跃 intent + `company_identity_verification_superseded` delta + review session 决定记录——原子。
与 R-019 的边界不变（本表自带 intent/generation 围栏，全局 command fencing 仍由 R-019 追踪）。

## 6. 裁决 schema 与调用信封分离（v3 修正 #5）

- **模型作者字段**（`company_identity_adjudication_v1`，模型只产这些）：candidate_id、
  candidate_url_canonical、identity_relation 枚举、evidence[]（ref/origin_domain/kind）、
  conflicts[]、confidence_label、confidence_basis。严格校验，缺失/未知枚举/解析失败 ⇒ needs_human。
- **服务端调用信封**（transport 层生成，模型**不可自证**）：provider、requested/response/
  effective model（精确匹配校验）、`model_identity_provenance`、provider call id、route/api_style、
  bounded usage + usage_status、fallback/circuit 证据——复用 `OpenAIModelCallResult.metadata()`
  形态（`model_provider.py:73-86,1497-1525`）。**接受条件同时校验两半**：信封
  fallback=none + 身份匹配 + 模型半 schema 过——模型输出里出现 provider/usage/fallback 字段一律
  忽略并记协议偏差。
- 身份 ≠ 人群 scope 不变：`confirmed_company_scope` 仍归 plan review。

## 7. 确定性接受谓词与统计门（v3 收紧 #18）

谓词七条基础上收紧第 3 条：
3'. **证据独立性**：≥2 个不同可注册域来源，且**必须包含** {公司 official domain 主动链接该
LinkedIn URL} 或 {两个彼此独立的非 LinkedIn 权威来源（新闻/官方文档）一致指向}——「LinkedIn 页
+ 任意第二域」**不再充分**；搜索摘要与其抓取目标 = 同一来源。
其余六条不变（canonical URL 形态/身份稳定/新鲜度/零冲突+歧义弃权/parent-suborg 永不 auto/
模型半+信封半全过）。

**统计门（量化，v3 补 #18 阈值）**：零观察误确认 **且分母 ≥ 120**（Wilson 95% 单侧上界 ≈ 3.0%）
方可申请 Phase 2；shadow 期生产统计（shadow_would_verify vs 人工实际选择的一致率）同报；
Phase 2 = 显式 promotion：owner GO 后**逐行 revalidation**（谓词当前版本重跑 + valid_until 内）
才把 shadow 行升 accepted——旧 shadow 行不随开关自动生效（#4）。Phase 2 先 K 个 plan 有界
canary + 强制回看。

## 8. 项目级恢复语义与预算（v3 收紧 #19 的诚实表述）

- 稳定项目 id：query_key = `q_<hash>`（R-010 对齐）、candidate_key = canonical slug hash、
  fetch_key = **重定向解析前**规范化 URL hash（去重键；final URL 另记录为证据属性——
  post-redirect 身份漂移不破坏去重）、judge_call_key = (candidate_key, evidence_set_hash,
  prompt_version)。
- **语义承诺（诚实版）**：付费效果 = at-most-once **effect**（幂等 apply + 去重键），attempt =
  at-least-once；crash-before-persist 的单项**可能重跑**——重跑成本有界于信封（该项扣减先于
  provider 调用同事务持久化，重跑不重复扣减、只消耗既扣额度）；不承诺严格 exactly-once。
- 信封原子扣减、cancel 前置停止、晚到结果按 §4c 隔离、重试限额/退避/熔断——同 v2。

## 9. 验收与激活边界

同 v2（回放集 + 对抗集、误确认=0 带分母、歧义 100% HITL、invocation-count 单 owner、机器写人工
字段 0 命中、ingress 伪造引用被拒【v3 新增守卫】、lane green-modulo-ledger、mypy 87 棘轮），外加：
§4a 迁移桥 precedence preflight、§4b 状态迁移表注册校验、§4c 八项竞态测试。激活边界不变
（C2+C3+TD-4+skip_plan_review 限 operator；live 等 owner 凭据授权）。

## 10. 评审 findings 覆盖映射

**v1 synthesis（经 v2 → v3 两轮）**：#1→§2.2（v3 补 terminal join 命令）；#2→§1/§2.2（v3 补双前门
单 helper）；#3→§4c/§5（v3 补 intent 全条件 CAS）；#4→§3/§4（v3 改服务端引用+human provenance
持久化）；#15→§3/§4a（v3 补文件注册表收编）；#16→§2.1（v3 改信封=审批证据+独立追加 action）；
#17→§6（v3 拆模型半/信封半）；#18→§7（v3 收紧独立性+阈值）；#19→§8（v3 诚实语义）；#21→§9；
#24→§1。

**v2 re-review 新 findings**：new#1→§4a；new#2→§2.2 terminal join；new#3→§4c；new#4→§4b 两态拆分
+§7 promotion；new#5→§6；new#6→§3；new#7→§2.1 活动级控制政策；new#8→§4b。
（new#9-15 属 D0 文档，见其 §7；new#16 属上层计划 §2 D2。）
