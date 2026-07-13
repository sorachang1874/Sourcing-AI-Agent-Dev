# Track D — D3 批级设计：公司身份自验证 loop（第一垂直切片）

> Status: Cross-model design input for owner review（v2 2026-07-13，作者 = Claude Fable 5；只设计、不改码）。
> **v2 修订**：按 gpt-5.6-sol reference review（NO-GO）findings #1-4/#15-19/#21/#24（synthesis 编号）
> 及 D3 子评审 findings 重写；全部代码断言经独立核查（C1-C9、C20-C22、C28）。覆盖映射见 §10。
> 上层计划：`TRACK_D_AGENT_RUNTIME_PLAN.md` §2 D3。TD-2/5/6/7 裁决见上层 §5。
> **不依赖 model_native_search、不依赖 D0 tool-calling、不依赖 D2。**
> 实施前按 handbook 纪律重新 Scout（基准 HEAD `5f14ed8`）。

## 1. 现状精确描述（v2 全面修正——v1 有三处事实错误）

**身份解析链（五分支，v1 漏了 exact-match）**，`connectors.resolve_company_identity`（`:193-248`）：
manual override（见下）→ builtin map（high）→ legacy slug（medium，`:219`，**先于** observed）→
observed 候选：先确定性 exact-match 分支（`resolver="observed_candidates_exact_match"`，medium，
`:354-391`——注意其归一化 `normalize_company_key` 剥掉全部非字母数字，`X.AI/X-AI/xai` 相互碰撞，
首个命中即胜、无歧义处理）再模型判定（只收 same_company + high/medium）→ heuristic slug（low）。

**执行期已存在一条活的付费身份路径（v1 完全遗漏）**：`acquisition.py:970-1051`
`AcquisitionExecutor._resolve_company`——低置信且非 manual override 时（守卫 `:986-990`）调
`_discover_company_identity_candidates`（`:4078-4100`，配置的 search provider，搜索异常静默吞掉、
fail-open 回 heuristic）+ 模型判定升级；**结果不持久化，每 job 从零重解析重付费**；写
`identity.json` artifact + 刷新身份注册表。

**plan review gate 与身份无关（结构性洞）**：`required_before_execution` 只在四处置 True
（open questions `:54` / google-scope `:58` / former-employee `:78` / investor `:82`），身份置信度
**结构上进不了 gate**——身份解析发生在执行期、gate 之后。低置信 heuristic identity 今天直接
`ready` 执行（`orchestrator.py:53102-53116`），无人工检查点；仅当 gate 因**无关原因**触发时，人才
有机会经 `target_company_linkedin_url`（editable_fields 里一个裸字符串，`plan_review.py:33`）修身份。
另有 `skip_plan_review` 载荷旗标可整体绕过 gate（`:52975/:53061`）。google-scope 有一个
LLM 置信≥0.8 即抑制确认的先例（`:504-549`）——本设计的 gate reason 形态与其同构。

**人工 override 通道**：`resolve_manual_company_identity`（`connectors.py:124-191`）把任何合法
slug/URL 无条件标 `resolver="manual_review_override"`、confidence=high，并**关闭执行期付费分支**
（`:986-990` 守卫）——这是本设计「以消费代退役」的机制原型，但 agent 直接预填该字段会伪装成
人工确认（provenance 违规），故需平行通道（§3）。

**产品定义不变**：把「人去检索并确认 URL」变成「agent 检索并给证据，人只在歧义时裁决」——
且 v2 起点比现状**更严**：低置信身份从「无人拦截」变为「必须有人或已验证结果放行」。

## 2. 形态：W11 图内的类型化命令 + plan 期解析前移（v2 修正 critical #1）

### 2.1 新注册项

- 验证 owner 模块：`company_identity_verification`。
- **两个** CommandTypeSpec（v2 修正——静态 ActionSpec 表达不了两档政策，拆命令是正解）：
  - `company.identity.verify.evidence`（Tier-1）：对已有候选做 documents.fetch 取证 + 模型裁决。
    **非零成本**（模型 judge tokens + fetch），走显式小信封，审批 not_required；
  - `company.identity.search.expand`（Tier-2）：付费检索扩展候选。ActionSpec 级 approval required；
    TD-5 的 plan 级预授权信封（默认 3 次 search/plan，可配置、内部产品软默认）在位且未耗尽时，
    Tier-1 owner 直接计划 Tier-2 命令（信封原子扣减）；耗尽则落 `approval_required` action 暂停,
    人批后继续——「durable interrupt before escalation」。
- ActionRegistry 新 action：`verify_company_identity`（入口）；EntityDelta kinds：
  `company_identity_verified` / `company_identity_verification_superseded`。
- 控制政策：cancel 在 attempt 前安全；search/fetch 步 after-start =
  `poll_cancel_late_result_quarantine`；模型 judge 步 = `fail_closed_until_terminal`（对齐既有 v1 模式）。

### 2.2 W11 命令图接入（v2 修正——不再是第二条编排路径）

- **W11 链**：`acquisition.plan.build` owner 在产出 plan 时做**plan 期廉价身份解析**（只跑确定性
  分支：override/agent_self_verified 读模型/builtin/legacy/observed exact-match——**不在 plan 期同步
  search 或调模型**）；置信 < high ⇒ 计划子命令 `company.identity.verify.evidence`（同
  OperationRun、parent-command 因果链），再计划 `acquisition.plan_review.request`；
  `plan_review.request` 读验证 current-state（§4）计算身份 gate reason——验证 terminal 前该
  reason 处于 `pending/needs_human`（阻塞），超时（deadline 配置）保持 needs_human 阻塞、晚到
  结果按 §5 CAS 处理。readiness effect / expected_run_statuses / terminal 与 no-op 出口在
  CommandTypeSpec 注册时全量声明。
- **legacy plan 前门**（`plan_workflow`/`_resolve_workflow_plan`）：同样在 plan 期做廉价解析 +
  读同一 current-state 读模型；置信 < high ⇒ enqueue 同一验证 operation + gate 加身份 reason。
  两个前门消费**同一 owner、同一读模型**——单编排事实源。
- **gate reason 语义**（v2 修正 critical #2）：新 reason `company_identity_unverified` 以 **OR
  组合**并入 `required_before_execution`（reasons 列表加条目；只有本 reason 因验证/人工确认清除时
  才移除自己，**其他 reason 一概不动**——防止清 aggregate 误放行 open questions 等无关阻塞）。
  `skip_plan_review` 现状也绕过本 reason——记录在案；hosted 多用户激活前该旗标限 operator
  （上层计划 §3 激活依赖列）。

### 2.3 Loop 步骤（owner 内，每步 = ActivityAttempt + 步内检查点）

1. **Tier-1 取证**：候选来源 = plan 期 observed 候选 + current-state 历史记录；对每个候选
   documents.fetch 公司页面/official-domain 证据；
2. **裁决**：新版本化 schema（§6）逐候选×证据比对；
3. 证据不足且信封可用 ⇒ 计划 Tier-2 检索扩展（新候选回到 1）；
4. **出口（置信阶梯 + TD-7 分阶段）**：
   - 确定性谓词（§7）+ 模型裁决全过 ⇒ `verified_auto` 候选态：**Phase 1（shadow）不放行**，
     gate 呈现证据卡 + 一键人工确认（走人工通道，provenance 诚实），同时记 shadow 统计；
     **Phase 2（统计门 + owner GO 后）** `verified_auto` 直接清身份 reason；
   - 其余（谓词未过/模型 uncertain/fallback/冲突/预算或步数耗尽/超时）⇒ `needs_human`：gate 阻塞 +
     证据卡（候选 URL、来源域、裁决、rationale、冲突），人裁决即完成。

## 3. Provenance 三通道（v2 修正 critical/high #4、子评审 #6）

| 通道 | 载体 | resolver 值 | 写入者 |
|---|---|---|---|
| 人工确认 | `target_company_linkedin_url` 执行偏好（现状不变） | `manual_review_override` / high | 仅真实人工决定 |
| 机器验证 | **新** `verified_company_identity` 结构化偏好/读模型 {canonical_url, slug, generation, artifact_ref} | `agent_self_verified` / high | 仅验证 owner（Phase 2） |
| 机器提案 | gate payload 的证据卡（display-only） | 无身份效果 | 验证 owner |

- `resolve_company_identity` 新增 `agent_self_verified` 分支（存储的 resolver 字面值统一为
  `agent_self_verified`，行文中「机器验证通道」即指此）：优先级 **manual override >
  agent_self_verified > builtin > legacy > observed > heuristic**；
- 执行期付费分支守卫（`acquisition.py:986-990`）扩展为 manual override **或 agent_self_verified 在位**
  即跳过——**「以消费代退役」**：legacy search+judge 分支只在两通道皆空时兜底，标注 migration
  bridge + 删除条件（Phase 2 稳定后移除）；**invocation-count preflight** 断言同一身份一次 run 内
  付费检索/裁决只发生于一个 owner；
- agent **永不写** `target_company_linkedin_url`（守卫测试：机器路径写该字段即红）。模型意图抽取
  本就排除该字段（`request_normalization.py:81-97`），新通道同样不得进入意图抽取白名单。

## 4. Current-state 读模型（v2 修正 #8/子评审 #11；TD-6 具体化）

新表 `company_identity_verifications`（migration，owner 物化，读者只读）：
`(workspace_id, company_fingerprint)` 唯一键；`decision_generation`（单调递增）、
`verification_state ∈ {pending, verified_auto, needs_human, human_confirmed, failed, timed_out,
superseded}`、`canonical_url/slug`、`confidence_basis`、`result_artifact_ref`（有界结果 artifact：
候选集、证据 refs、逐候选裁决——**高基数内容全在 artifact，delta 与行内只存摘要+ref**，对齐
bounded-envelope 契约）、审计时间戳。

- **消费方**：两个 plan 前门（§2.2）、`resolve_company_identity` 的 agent_self_verified 分支、
  acquisition 执行期守卫、gate 证据卡渲染。
- **owner 矩阵**（新公共字段全量登记，W10 矩阵风格）：

| 字段 | owner | source of truth | 允许值 | 消费方 | fallback |
|---|---|---|---|---|---|
| verification_state | 验证 owner | 本表 | §4 枚举 | gate/前门/UI | 缺行 = 未验证（≠失败） |
| resolver=`agent_self_verified` | connectors 解析链 | 本表 generation 行 | 固定值 | identity 消费方 | 不适用 |
| 证据卡 candidates[] | 验证 owner | result artifact | 有界列表 | gate UI | 缺失 ⇒ 仅 needs_human 文案 |
| decision_generation | 验证 owner | 本表 | 单调 int | CAS 写路径 | 不适用 |

- 附带收益（核查 C9）：身份解析首次可持久化——后续 plan 复用已验证身份，消除每 job 重付费。

## 5. 「人永远赢」的机制化（v2 修正 critical #3）

- **generation CAS**：机器接受 = `UPDATE ... SET state='verified_auto', generation=generation+1
  WHERE workspace_id=? AND company_fingerprint=? AND generation=<expected> AND state NOT IN
  (human_confirmed)`——CAS miss ⇒ 结果记 attempt outcome `superseded_not_applied`，不改行、不改
  gate、不写 delta 之外的任何状态；
- **人工确认** = 同一 UoW 内：写 human_confirmed + generation+1 + 追加
  `company_identity_verification_superseded` delta（若覆盖机器结果）+ 人工通道偏好写入——原子；
- **全链因果携带**：workspace_id、plan_review_session_id、plan bundle hash、target/scope
  fingerprint、expected generation 从 action→command→attempt→delta 逐级携带；plan 重编译/目标
  变更 ⇒ fingerprint 变 ⇒ 旧结果天然 CAS miss；
- **竞态测试清单（批验收硬项）**：人工确认 vs 机器结果到达、cancel vs provider 回调、retry 子
  run vs 父 run 晚到结果、plan 重编译 vs 旧结果、双机器结果并发 CAS——每项断言终态唯一且
  provenance 正确；
- 与 R-019 的边界：本表自带 generation 围栏，不等全局 command-generation fencing；stale owner 对
  **本表**的写被 CAS 挡住，R-019 的 phantom child/attempt 全局边界不因本批而缩小（引用台账行，
  不重新诉讼）。

## 6. 裁决 schema v1（v2 修正 #5/#17、子评审 #7；核查 C20-C22）

新 ModelClient 方法 `adjudicate_company_identity`（**不改**现 `judge_company_equivalence` 的
4-key 契约——prompt 与映射白名单双重钉死，改造反而破坏既有消费方）：

```python
# company_identity_adjudication_v1(严格校验,任何缺失/未知枚举/解析失败 ⇒ needs_human):
{ "schema_version": "company_identity_adjudication_v1",
  "candidate_id": str, "candidate_url_canonical": str,
  "identity_relation": "same_company|parent_of|subsidiary_of|related_brand|different|uncertain",
  "evidence": [{"ref": str, "origin_domain": str, "kind": str}],
  "conflicts": [str], "confidence_label": "high|medium|low", "confidence_basis": str,
  "provider": str, "requested_model": str, "response_model": str,
  "usage": {...}, "fallback_status": "none|deterministic_fallback|error_fallback" }
```

- 走既有 metadata-rich 调用路径（`OpenAIModelCallResult.metadata()` 已有 provider/model/usage
  管道，`analyze_public_web_candidate_signals` 即用例，`:1480-1526`）——不再用 `_safe_text_prompt`
  裸文本路径；
- **fallback 可判别**（核查 C21 的危险点：确定性 fallback 在单候选时会回显其 label，形似阳性）：
  `fallback_status != none` ⇒ 强制 needs_human，auto-confirm 不看 label；不再靠 rationale
  字符串匹配识别 fallback；
- **身份 ≠ 人群 scope**（核查 C22：`scope_disambiguation` 属 request normalization）：
  `identity_relation` 只裁公司身份；`confirmed_company_scope` 仍归 plan review 所有，本设计
  **不派生、不修改**——Gemini 案例：canonical 雇主身份可为 Google 而请求 scope 为子组织，两者
  各走各的契约。

## 7. 确定性接受谓词（先于模型置信；v2 修正 #6/#18）

auto-confirm（Phase 2）必须**全部**满足；任何一条失败 ⇒ needs_human（弃权优先）：
1. canonical URL 形态：重定向解析后 `https://www.linkedin.com/company/<slug>/`，slug 合法形态；
2. 候选身份稳定：全部采信证据解析到同一 canonical slug；
3. **证据独立性键 = 可注册域**：≥2 个不同可注册域的来源，其中至少一个是
   {公司 official domain 指向该 LinkedIn URL 的链接, LinkedIn 公司页自身字段佐证}；
   搜索摘要与其抓取目标页 = **同一**来源；
4. 新鲜度窗口（配置，默认 30 天）内抓取的证据；
5. 零未决冲突：无第二候选 `identity_relation=same_company`；无相反 official-domain 链接；
   **歧义弃权**：≥2 个候选过谓词 ⇒ needs_human（不学 exact-match 的「首个命中即胜」——其
   `normalize_company_key` 全剥归一化会让 X.AI/X-AI/xai 碰撞，本谓词用完整 canonical 比较）；
6. parent/suborg：`identity_relation` 必须恰为 `same_company`；parent_of/subsidiary_of/
   related_brand ⇒ 永不 auto，needs_human；
7. 模型裁决：same_company + high + `fallback_status=none`（必要非充分）。

**统计门（TD-7 Phase 1→2 的量化条件）**：
- 评测集：盲标注正例 + 对抗负例（同名异司/改名/被收购/多语言名/SEO 污染/重定向陷阱/母子公司）;
- 报告 = 分子/分母/95% 上置信界（Wilson），**零观察误确认 + 分母 ≥ 50** 方可申请 Phase 2；
- shadow 期生产统计（would-have-confirmed vs 人工实际选择）同报；Phase 2 先 K 个 plan 的有界
  canary + 强制回看，再全量——Phase 2 开关是 owner-gated 配置。

## 8. 项目级恢复语义与预算（v2 修正 #19/#16；核查 R-010 对齐）

- **稳定项目 id**：query_key = `q_<hash>`（对齐 R-010 identity-key 契约）、candidate_key =
  canonical slug hash、**fetch_key = 重定向解析后规范化 URL 的 hash**（同 URL 的 fetch 跨
  retry/resume 恰好一次）、judge_call_key = (candidate_key, evidence_set_hash, prompt_version)；
- **步内检查点**：每次成功的 search/fetch/judge 先持久化 attempt + artifact 再推进；resume 按
  项目 id 跳过已完成付费项；retry 永不重跑已完成付费项；
- **信封原子扣减**：维度 {searches, fetches, model_calls/tokens, wall}；扣减与 attempt 创建同
  事务，跨 retry/resume 不重置；耗尽 ⇒ terminal needs_human（永不静默超支）；
- cancel：attempt 前即停；attempt 后晚到结果按 §5 CAS 隔离（quarantine）；重试限额 + 退避显式，
  熔断照常生效。

## 9. 验收（批记录写数字）与激活边界

- 回放集：历史上人工编辑过 URL 的 plan（Scout 时从 jobs/plan_review_sessions 拉真实清单）+
  §7 对抗集，scripted 转写驱动；指标 = 人工编辑率下降、**误确认 = 0（分母一并报）**、歧义 100%
  进 HITL、invocation-count 单 owner 断言、机器写人工字段 0 命中；
- 机械边界扫描：owner 零直写 CRM/projection/person asset/provider registry；lane
  green-modulo-ledger；mypy 87 棘轮；
- **激活边界**（v2 修正 #21）：实现/scripted/simulate 验证不依赖 Track C；hosted 多用户激活须
  C2 可信身份 + C3 worker 隔离 + TD-4 路由表 + `skip_plan_review` 限 operator；live provider
  验证等 owner 授权凭据。

## 10. v1 评审 findings 覆盖映射（synthesis 编号 + D3 子评审编号）

| finding | 处置 |
|---|---|
| syn#1（未接 W11，第二编排路径） | §2.2 plan.build 子命令 + 双前门同源读模型 |
| syn#2 / sub#5（gate 兜底不存在） | §1 事实修正 + §2.2 身份专属 reason（OR 组合）+ TD-7 分阶段 |
| syn#3 / R-019 关联（人赢无机制） | §5 generation CAS + 原子 supersession + 竞态测试清单 |
| syn#4 / sub#6（prefill 毁 provenance、无 current-state owner） | §3 三通道 + §4 读模型与 owner 矩阵 |
| syn#15 / sub#C4-C6（执行期路径未退役） | §3「以消费代退役」+ invocation-count preflight |
| syn#16 / sub#4（两档政策表达不了） | §2.1 拆两命令 + 信封 + durable interrupt |
| syn#17 / sub#7（judge 契约不够） | §6 新方法新 schema，旧方法不动 |
| syn#18（谓词/统计门欠定义） | §7 七条谓词 + 量化统计门 |
| syn#19（单命令无项目级恢复） | §8 项目 id/检查点/原子扣减 |
| syn#21 / sub#9（实现与激活混同） | §9 激活边界 |
| syn#24 / sub#12（漏 exact-match 分支） | §1 五分支修正 + §7.5 显式不复用其松归一化 |
| sub#11（EntityDelta 无消费方） | §4 读模型 + 消费方清单 |
