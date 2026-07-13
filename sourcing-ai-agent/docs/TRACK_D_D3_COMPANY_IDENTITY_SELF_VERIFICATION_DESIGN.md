# Track D — D3 批级设计：公司身份自验证 loop（第一垂直切片）

> Status: Cross-model design input for owner review（2026-07-13，作者 = Claude Fable 5；只设计、不改码）。
> 上层计划：`TRACK_D_AGENT_RUNTIME_PLAN.md` §2 D3。TD-2 已于 2026-07-13 由 owner 裁决采纳
> （durable operation 内嵌 plan 流水线，不等 D2 会话层）。**不依赖 model_native_search**。
> 实施前按 handbook 纪律重新 Scout 全部锚点。

## 1. 要替代的人工步骤（现状精确描述）

- 解析链：`resolve_company_identity`（`connectors.py:193-248`）四级——builtin map（high）→
  legacy slug（medium）→ observed-candidates 模型判定（`:251-325`，`judge_company_equivalence`
  只收 high/medium 的 `same_company`）→ heuristic slug（**low，人工介入主场景**）。
- 人工介入点：plan review gate 暴露可编辑 `target_company_linkedin_url`
  （`plan_review.py:26-115`；google-scope 歧义时 `required_before_execution=True`）；
  用户手查手填 URL → `resolve_manual_company_identity`（`connectors.py:124-191`）以
  `resolver="manual_review_override"`、confidence=high 覆盖。
- 切片的产品定义：**把"人去检索并确认 URL"变成"agent 检索并给出证据，人只在歧义时裁决"**。

## 2. 形态：一个 OperationRun + 类型化命令（复用 W11 模式，零新 runtime）

### 2.1 新注册项

- ActionRegistry 新 action：`verify_company_identity`（owner `company_identity_verification`）。
  两档预算（见 §4）；`display_contract` 完整；进 allowlist 前过 registry 注册门。
- 新 CommandTypeSpec：`company.identity.verify`（单命令即可，步骤用 ActivityRun/Attempt 表达，
  不为每步造命令类型——与 W11d "activity spine 表达步骤" 的既定形态一致）。
- EntityDelta kind：`company_identity_verified`（write-once 效果证据：candidates 考虑集、
  采信 URL、置信、模型 rationale 摘要、evidence source refs）。

### 2.2 Loop 步骤（owner 内实现，每步落 ActivityAttempt）

1. **Tier-1 证据收集（零 search 花费）**：对既有 `observed_companies` 候选
   （label/slug/`linkedin_company_url`/domain_hint，`connectors.py:328-351`）做 documents.fetch
   （既有 fetch 路径）取公司页面证据；无候选或证据不足 → 进 Tier-2。
2. **Tier-2 检索扩展（付费,需预算）**：既有 `search_provider`（DataForSEO/Serper）按
   company name + "linkedin" 模板检索,归一出新增候选。
3. **裁决**：`judge_company_equivalence`（`model_provider.py:638`）对候选×证据比对；
   parent-vs-suborg 场景强制输出 scope 归属（沿用 `scope_disambiguation` prompt 语义,
   `model_provider.py:427-434`）。
4. **出口（置信阶梯）**：
   - `high`（模型 same_company/high 且 ≥2 独立证据源一致）→ 自动确认：新
     `resolver="agent_self_verified"`,confidence=high,EntityDelta 落证据链;
     review gate 字段变为**预填+可改**（不再 required_before_execution）。
   - 其余（medium/low/冲突/预算耗尽/步数耗尽）→ **保持现 gate 行为不变**,但 gate payload
     附证据卡：candidates[]（URL/来源/模型裁决/rationale）,人裁决即完成——
     裁决仍走既有 `manual_review_override` 路径,**本切片不改人工路径一行**。

### 2.3 接入点与时序

- 触发：plan compile 产出 identity confidence < high 时,plan 流水线 enqueue 该 operation
  （复用 202/poll 信封）;plan review gate 读 operation 结果决定字段形态。
- **超时降级**：operation 未在 gate 打开前完成 → gate 完全走现状（可编辑必填）,验证结果
  迟到只作证据卡补充,不回写已人工确认的身份——**人工裁决永远赢**（latest-wins 是人）。

## 3. 硬边界（对抗审计项，逐条可机械验证）

1. owner 只写自己的表 + ActivityRun/Attempt/EntityDelta;**零直接写** CRM/projection/person
   asset/provider registry（静态扫描 + 测试断言）。
2. 自动确认只影响 `CompanyIdentity` 解析结果与 review gate 字段形态;**不跳过 review gate 本身**
   （gate 仍渲染,错误自动确认可被人工改回——这是切片的安全网）。
3. `judge_company_equivalence` 走既有 ModelClient 单发方法——**D3 不依赖 D0 的 tool-calling**,
   两批可完全并行;D2 落地后 loop 原语迁移是后续重构,非本批。
4. simulate 模式全链路可跑（scripted search/fetch/judge 转写）;live 按 fail-closed 纪律。

## 4. 审批与预算（按 AGENT_OPERATION_CONTRACT 政策表）

| 档 | 内容 | 政策 |
| --- | --- | --- |
| Tier-1 evidence-only | 既有候选页面 fetch,零 search 花费 | 显式零 provider 预算,免审批（对齐 "seed-url-only 可零预算但仍须显式"） |
| Tier-2 paid search | DataForSEO/Serper 检索扩展 | 显式预算 + 审批（对齐 "paid provider 需 approval+budget"）;plan 级可预授权一个小额度（**TD-5,owner 决策**：默认每 plan ≤N 次 search 免逐次审批,N 建议 3） |

模型调用（judge）预算随 TD-4 路由表；步数上限 max_steps 建议 6（2 fetch + 1 search + 3 judge 量级），
超限即转 HITL。

## 5. 验收（批记录须写数字）

- **回放集**：取历史 plan 中曾人工编辑过 `target_company_linkedin_url` 的 case（Scout 时从
  jobs/plan_review_sessions 拉真实清单）+ 构造的 parent-vs-suborg 集,scripted 转写驱动:
  - 自动确认率（目标:明确 case 的显著多数转自动）;
  - **误确认率 = 0 为硬门**（自动确认了错误 URL 的 case 数;有 1 例即 NO-GO 该批）;
  - 歧义 case 100% 进 HITL 且证据卡完整。
- 机械边界扫描（§3.1）零命中;lane green-modulo-ledger;mypy 87 棘轮。
- 批 settle 后 pinned scope 发 canonical runner 异步评审;live 验证等 owner 授权凭据后单独做。

## 6. 决策点（2026-07-13 owner 已裁决）

- **TD-2**（已裁决:接受建议）切片形态 = plan 流水线内 durable operation，本文按此展开。
- **TD-5**（已裁决:暂按建议落地,附 owner 定位）默认 3 次/plan、超出逐次审批,但该额度是
  **内部团队产品的审批人体工学默认,不是产品硬限制**——额度必须可配置且倾向宽松,不得演变成
  强限流。方向注记（owner 2026-07-13）:强 Agent 化后检索面将扩展到模型原生 Search
  （ChatGPT/Claude 类模型自行检索取证）,不强依赖 DataForSEO;该扩展仍须按
  `MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md` 的 owner 矩阵走转正批（上层计划 D4）,
  本切片不预置、但预算/审批结构设计时不得与"多检索后端"形态冲突。
- **TD-6**（已裁决:接受建议）自动确认证据落点先取 EntityDelta 最小方案;是否同步写
  `company_evidence` 留给实施 Scout 依据现有 company asset 写路径裁决,原则:若写,
  必须经该表既有 owner 路径,不得 D3 owner 直写。
