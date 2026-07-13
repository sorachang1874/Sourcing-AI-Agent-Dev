# Track D — D0+D1 批级设计：模型工具运行时 + 工具面 serve

> Status: Cross-model design input for owner review（v2 2026-07-13，作者 = Claude Fable 5；只设计、不改码）。
> **v2 修订**：按 gpt-5.6-sol reference review（NO-GO，提取件
> `runtime/reviews/20260713T112818Z_*.extracted-reference.md`）findings #5-14/#22-23（synthesis 编号）
> 及 D0 子评审 findings 1-16 重写；代码断言经独立核查（C10-C19、C23-C27）。覆盖映射见 §6。
> 上层计划：`TRACK_D_AGENT_RUNTIME_PLAN.md` §2 D0/D1。TD-1 已裁决（requests + SSE 行解析）。
> 实施前按 handbook 纪律重新 Scout 全部行号锚点（基准 HEAD `5f14ed8`）。

## 0. 范围与安全定位（v2 首要修正）

**D0 交付的是低层能力面，不是 normal path。** live tool-calling 在以下三者齐备前对一切调用方
fail-closed：(a) TD-4 产品路由表 owner 批准；(b) 一个 typed model-turn owner（D2 `agent.turn` owner
或 D3 类命令 owner）绑定 action/operation/turn/attempt 身份、审批/预算、终态与重试归属；
(c) `ModelTurnExecutionContext`（§2.3）完整提供。D0 批内可交付并验证的 = 全部类型/解析器/scripted
回放/守卫 + simulate 路径；live 冒烟只在 owner 显式授权凭据后单独做。

## 1. 硬不变量（characterize-first 钉死后逐字节保持）

1. `ModelClient` Protocol 的 14+3 个既有方法（`model_provider.py:617-650`）签名与行为不变；
   消费模块零改动（Scout 时钉死枚举清单存档——"24 个"按 `git grep '\bmodel_client\b'` 去除
   `serving_projection_migration.py` 的 `model_client=None` 非消费者后得出，实施时重数）。
2. **三种** payload 形状全部特征化（v2 修正——v1 漏 Qwen）：
   chat `{model,messages,max_tokens,temperature:0}`（`:1860-1865`）；
   responses `{model,input,max_output_tokens,temperature:0}`（`:1923-1928`）；
   **Qwen `{model,input}` + 条件性 `max_output_tokens`、无 temperature**（`:1256-1264`）。
   Qwen 的 transport 分歧一并钉死：urllib、无重试、无熔断、**无 response 模型身份检查**（`:1256-1283`）
   ——这也是 Qwen tool-calling 延后的记录在案理由。
3. 既有安全面复用而非旁路，且**两把锁分开表述、分开测试**（v2 修正 v1 的混同）：
   - 通用 requested-vs-response 模型身份检查（`_openai_model_identity_failure`，`:89-106`，
     用于 `:1793-1804`）——每个流式调用都必须做；
   - CRM 产品模型锁（`CRM_PUBLIC_WEB_PRODUCT_MODEL`，仅 `analyze_public_web_candidate_signals`
     路径，`:1480-1526`）——与 agent turn 无关，不得外溢。
   - per-provider 熔断（成功/失败记录语义见 §2.6）；live 双钥门（见 §2.7）。

## 2. 新能力面（新模块 `model_tool_runtime.py`）

### 2.1 提供方无关消息模型 v1（v2 新增——裸 dict 撑不起多轮工具循环）

```python
# ModelTurnMessage 判别联合(schema_version="model_turn_message_v1"):
#   system(text) | user(text) | assistant_text(text)
#   | assistant_tool_calls(calls: [ToolCallRecord])          # 模型上一轮的工具调用
#   | tool_result(tool_call_id, content, is_error)           # 工具执行结果回传
# 尺寸上限:单条 content 字节上限 + 总 messages 字节上限;超限内容走 artifact ref。
# OpenAI chat 序列化器只上送 {type:"function", function:{name,description,parameters}};
# ToolSpec 的 approval/budget/display 元数据是服务端契约,永不上 provider wire。
```

### 2.2 类型

```python
@dataclass(frozen=True)
class ToolSpec:
    name: str                 # = action_type(§3 registry 投影,永不手写)
    description: str
    input_schema: dict        # JSON Schema,事实源 = §3.1 的 ActionSpec.input_schema
    schema_version: str
    approval_policy: str      # 服务端元数据,不上 wire
    budget_required: bool

# AgentTurnEvent(仅为流式传输/进度展示;对执行永远 advisory,见 §2.5):
#   text_delta | tool_call_partial | usage | stop(reason) | error(message)

@dataclass(frozen=True)
class ToolTurnResult:         # 唯一可授权后续动作的 canonical 语义结果
    text: str
    tool_calls: tuple[ToolCallRecord, ...]   # 已验证、有序、args 过 schema
    usage: BoundedUsage                      # 复用既有五字段有界对象(input/output/total/cached/reasoning)
    usage_status: str                        # reported | unavailable | invalid(v2:不再有裸 "unknown")
    model_identity: ModelIdentity            # requested / response / effective + provenance
    terminal_reason: str                     # end_turn | tool_calls | max_tokens | content_filter
    provider_call_id: str | None
    route_id: str
```

### 2.3 执行上下文（v2 新增——live 的准入契约）

```python
@dataclass(frozen=True)
class ModelTurnExecutionContext:
    route_id: str                    # §4 路由表条目;不接受裸 model 字符串
    operation_run_id: str; turn_id: str; step_id: str; attempt: int
    idempotency_key: str
    budget: ModelTurnBudget          # max_input_tokens/max_output_tokens/monetary_ceiling/wall_deadline(单调钟)
    approval_ref: str | None         # 审批证据引用(路由的 budget_class 要求时必填)
```
live 路径缺 context 或 budget 任一字段 ⇒ 构造期 raise（不发网络请求、不计熔断——本地配置失败
不污染熔断，对齐 `DURABLE_EXECUTION_RUNTIME_CONTRACT.md:157` 的 pre-transport 失败 carve-out）。
`usage_status != reported` 的付费调用按 `monetary_ceiling` 的保守保留额记账，**缺失 usage 永不
静默视为零成本**。

### 2.4 会话协议与实现基类（v2 修正 v1 的 Protocol 自相矛盾）

```python
class ToolCallingSessionBase(abc.ABC):
    # live 子类实现流原语;buffered 由基类消费流+终态验证合成(单向,无递归):
    def run_tool_turn(self, ctx, messages, tools) -> ToolTurnResult: ...
    def stream_tool_turn(self, ctx, messages, tools) -> Iterator[AgentTurnEvent]: ...
    # scripted 子类反向:实现 canonical ToolTurnResult 回放,基类按 coalescing 规则 v1 合成事件流
    # (每 assistant 文本一个 text_delta、每完整工具调用一个 tool_call_partial、一个 usage、一个 stop)。
```
**等价测试改为语义等价**（v2 修正）：同一 wire 转写的多种合法 SSE 分块切法（逐字节流/整帧/跨帧
切分/args 分片重组）→ 独立手写的期望 `ToolTurnResult` 完全一致；不再断言事件逐帧相等（事件框架
随 provider 分块漂移，非稳定契约）。

### 2.5 「流式输出 advisory、终态结果 authorize」（v2 新增核心安全规则）

- 流事件（含看似完整的 `tool_call_partial`）一律**不可执行**；只有经 §2.6 状态机验证的 terminal
  `ToolTurnResult` 可作为提交 `AgentAction` 的依据。
- 断流后 owner 层重试 = 同 turn 下新 attempt；工具执行去重键 =
  `(turn_id, step_id, tool_call_id)` 稳定 idempotency key——重复 side-effect 结构性堵死。
  部分输出保留为 quarantined 证据（attempt 级 artifact），不进结果。

### 2.6 OpenAI-compatible chat 流式实现：fail-closed 状态机（v2 全面收紧）

- **能力矩阵（网络 I/O 前判定）**：`api_style=openai_chat_completions` → 支持；
  `openai_responses` / Qwen / deterministic / offline → raise `ToolCallingNotSupported`
  （沿用 `:1837-1842` 对未知 api_style 的 fail-closed 先例；**绝不静默改道 /chat/completions**）。
- 请求：`stream=True`、`tools=[...]`、`tool_choice="auto"`、`n` 不设且**收到多 choice 即协议失败**；
  `stream_options:{"include_usage":true}`。
- 解析（自研范围显式声明：只解 `data:` 帧 + `[DONE]`，忽略 SSE retry/id/event 字段，注释行跳过）：
  - tool_calls 累积按 `(choice_index, tool_index)` 归组且强制 `choice_index==0`；call id/name
    冲突或分片间漂移 ⇒ 协议失败；args 分片拼接后**过 served schema 校验**；
  - 有界缓冲：单帧字节上限 / 单 call args 字节上限 / calls 数上限 / 总 text 上限，超限 ⇒ 协议失败；
  - finish_reason 显式映射：`tool_calls`→继续等 usage 帧与 `[DONE]`；`stop`→terminal；
    `length`/`content_filter`→terminal（原因入 `terminal_reason`）；未知值 ⇒ 协议失败；
  - 缺 finish、缺 `[DONE]`、HTTP-200 载荷内 provider error 帧、JSON 解析失败 ⇒ 协议失败；
  - response 在 `finally` 关闭；消费方 `GeneratorExit`/放弃迭代 ⇒ 关闭连接 + 记 incomplete attempt。
- **模型身份**：累积 response `model` 字段，terminal 时 requested-vs-response 精确匹配
  （复用 `:89-106` 语义）；不匹配 ⇒ 失败 + 熔断记录；provenance 只在真实拿到 response 身份时标
  `provider_response`，fallback 场景永不伪造（对齐 contract `:157` 注记）。
- **熔断语义**：transport 前查熔断；**成功只在「验证过的 terminal 帧 + 身份匹配 + 工具调用完整」
  后记录**（HTTP 200 即记成功会祝福断流）；transport / provider error 帧 / 解析 / 截断 / 身份失败
  均记失败；本地 pre-transport 配置失败（不支持的 api_style、缺 context）不触熔断。
- **墙钟**：以 ctx.budget.wall_deadline（单调钟）跨帧强制，非仅 requests read timeout；超时 ⇒
  失败终止 + incomplete attempt。
- 流内不做 session 级重试（v1 规则保留；重试权归 owner 层，配合 §2.5 去重键）。

### 2.7 live 门（v2 修正——构造期门不充分）

流式 helper 在 `requests.post` 前**每次**调用
`assert_live_provider_access_allowed(..., payload=normalized_request)`，不论 session 如何构造
（独立核查 C27：现契约的低层门规则列举的是搜索/抓取类 provider、模型中继未显式在列，且现实现
只在 `build_model_client:2291` 构造期设门——本批把该规则**显式扩展到模型中继**，含
`RUNTIME_ENVIRONMENT_ISOLATION.md` 契约文档同步更新 + 模型 API key 纳入子进程 credential-blanking
清单核对）。

### 2.8 Scripted 回放与转写治理（v2 收紧）

- 请求指纹升级为**canonical request hash v1**，覆盖全部影响行为的字段：
  `{route_id, provider, model, api_style, max_tokens, tool_choice, stream_options,
  message_model_version, tools_schema_digest, prompt_policy_version, messages_digest}`；
  任一字段变化 ⇒ 回放 fail-closed（测试逐字段验证）。
- 转写治理：落 `runtime/model_turn_transcripts/` 命名空间（runtime 不入库）；每份带
  schema_version、大小上限、保留 TTL；**录制管线内置脱敏**——转写只存归一化事件（永不存 raw
  provider payload），敏感字段（候选人姓名/邮箱/电话/CRM 备注）按字段级脱敏规则替换为占位符+
  digest，超限内容替换为 artifact ref + 摘要；含真实候选人/CRM 数据的转写**禁止**作为
  source-controlled fixture——入库 fixture 必须合成数据。
- 录制器包装 live session，仅 owner 显式授权时运行（provider fail-closed 纪律原样适用）。

## 3. D1 工具面 serve（v2 重构——先补 schema 地基，serve 可执行子集）

### 3.1 输入 schema 事实源（v2 新增，本批最重要的地基项）

- `ActionSpec` 扩展 `input_schema: dict` + `input_schema_version: str`（单一事实源；独立核查
  C10/C12/C17：现无任何 schema 字段，`submit_action` 对 `input_payload` 零校验，各 dispatch
  adapter 私有手写校验）。三方消费同一份：
  (a) `POST /api/operations/actions` 提交路径按 schema 校验 `input_payload`（顺带修复零校验）；
  (b) planner `ToolSpec.input_schema`；(c) dispatch adapter 入参校验。
- 迁移策略 fail-closed：schema 按 action 渐进补齐；**无 schema 的 action 不进 agent 工具面**
  （不 serve、不给 planner），既有 API 提交对无 schema action 保持现状（宽松）并入 residual 跟踪。

### 3.2 serve 子集：`agent_tool_enabled` 谓词（v2 修正「全集等价」）

served ⊆ ActionRegistry，当且仅当：
1. `input_schema` 已定义；
2. **dispatch 就绪**：dispatch adapter 集合改为从 registry 元数据派生（替换
   `orchestrator.py:47091-47096` 的硬编码字面集合——独立核查 C15：该集合与
   `allowed_workflow_command_types` 元数据脱节，`plan_acquisition`/`promote_person_assertion`/
   `external_intake` 注册在案却 dispatch `unsupported`，12/15）；
3. activity-spine 校验通过（**语义断言**，v2 修正 v1 空断言——`legacy_internal_pending_activity_spine`
   是 policy 值不是命令类型：对每个 served command 断言
   `activity_spine_policy.requirement != legacy-internal` 且 `agent_callable`，复用注册期校验
   `operation_runtime.py:121-134`；注意该校验只遍历 `allowed_workflow_command_types`——空命令面
   action 由本谓词第 2 条兜住）;
4. **simulate dispatch preflight**：每个 served tool 在 simulate 模式实际 dispatch 成功一次
   （contract lane 级守卫，防注册元数据与 adapter 再度脱节）。

### 3.3 路由与序列化

- 新只读 `GET /api/agent/tool-registry`：action 级（含 input_schema）+ command 级字段
  **复用 `command_type_manifest()` 序列化**（独立核查 C16：现 command-registry 端点从同源 specs
  重组字段致 manifest 字段缺失——不再第三处重组）。`schema_version: "agent_tool_registry_v1"`。
- 守卫：served 集 ≡ `agent_tool_enabled` 子集（双向）；输出快照特征化测试。

## 4. TD-4 草案：`ModelRouteRegistry`（产品侧，owner 终审前 live 一律 fail-closed）

```python
# checked-in、版本化、product-owned;与 INDEPENDENT_REVIEW_GATE 的 reviewer 路由表零耦合。
@dataclass(frozen=True)
class ModelRouteSpec:
    route_id: str            # e.g. "agent.planner.loop" / "company.identity.adjudicate"
    use_case: str
    provider: str            # "openai_compatible" | "qwen" | ...
    model: str               # 显式钉死;调用方永远只传 route_id
    api_style: str
    capabilities: frozenset  # {"stream","tools","usage","identity_check"} — 缺必需能力即拒
    budget_class: str        # 映射默认 ModelTurnBudget 与审批政策
    simulate_mapping: str    # simulate/scripted 模式的替身实现
    fallback_policy: str     # "fail_closed" 唯一初始值:无静默改道(对齐"model_usage 永非路由输入"、
                             # Qwen 非产品 fallback、review 证据的 empty reroute chain 要求)
    circuit_key: str
    rollout_state: str       # draft | canary | active | retired
```
初始表（draft，owner 终审）：`agent.planner.loop` 与 `company.identity.adjudicate` 两条，
provider/model 由 owner 定（CRM 的 `gpt-5.6-sol` 锁不外溢到这里）。路由变更 = 配置提交 + 审计，
不是运行时行为。

## 5. 批协议（handbook §4 形态，v2 更新）

1. **Scout**：重列 Protocol/消费方清单（存档枚举）；确认 `test_model_provider.py` 覆盖面。
2. **特征化先行**：14+3 方法 AST 金快照 + **三种** payload 形状 + 两把身份锁分开 + 熔断语义 +
   Qwen transport 分歧，对照树同绿后动码。
3. **落地**：`model_tool_runtime.py` + ActionSpec schema 扩展 + registry 派生 dispatch 集 +
   tool-registry 路由 + 守卫；model_provider.py 只加不改。
4. **A/B 与变异自检**：多分块语义等价电池；变异必红清单——丢 (choice,tool) 归组键、跳过 args
   schema 校验、HTTP 200 即记熔断成功、去掉 terminal 身份匹配、去掉 pre-post live 门、
   请求指纹漏 route_id 字段，六处逐一破坏确认变红。
5. **lane + 套件**：`make ci-pre-agent-contract` + `test_model_provider` + 新
   `test_model_tool_runtime` + `test_agent_tool_registry` + simulate dispatch preflight + lint 门。
6. **文档**：本文批记录；`AGENT_OPERATION_CONTRACT.md`（registry serve 语义）、
   `RUNTIME_ENVIRONMENT_ISOLATION.md`（模型中继纳入低层门）、
   `DURABLE_EXECUTION_RUNTIME_CONTRACT.md`（ToolTurnResult 身份/usage 对齐）同步一段。
7. **异步评审**：settle 后 pinned scope 发 canonical runner（runner 协议代差修复前，评审内容
   人工提取仅作参考，不作 gate 证据）。

## 6. v1 评审 findings 覆盖映射（synthesis 编号 + D0 子评审编号）

| finding | 处置 |
|---|---|
| syn#5 / sub#1,11（input_schema 无事实源） | §3.1 ActionSpec 扩展 + 三方同源消费 + 无 schema 不 serve |
| syn#6 / sub#3（全集≠可 dispatch） | §3.2 谓词第 2 条 + registry 派生 dispatch 集 + preflight |
| syn#7 / sub#2（legacy 守卫空断言） | §3.2 第 3 条语义断言 |
| syn#8 / sub#1,2（无 owner/预算信封即付费） | §0 fail-closed 定位 + §2.3 ExecutionContext/ModelTurnBudget |
| syn#9 / sub#3（TD-4 错挂 reviewer 表） | §4 独立产品注册表；上层计划 §5 TD-4 修正 |
| syn#10 / sub#4,8,9（能力矩阵矛盾/Qwen 漏钉） | §2.6 能力矩阵 + §1.2 三 payload 特征化 |
| syn#11 / sub#5,13（身份/usage 证据缺失） | §2.2 ToolTurnResult(model_identity+usage_status) + §2.6 身份验证 |
| syn#12 / sub#6（流状态机不完备） | §2.6 全节 + 熔断成功/失败时点 |
| syn#13 / sub#10（部分工具调用逃逸/重试重复） | §2.5 advisory/authorize 分离 + 去重键 |
| syn#14 / sub#12（消息模型缺失） | §2.1 消息模型 v1 + wire 序列化边界 |
| syn#22 / sub#8（buffered/streamed 自相矛盾） | §2.4 基类 + 语义等价测试 |
| syn#23 / sub#14,15,16（指纹弱/生命周期/转写治理） | §2.8 canonical hash + 治理；§2.6 墙钟/关闭 |
| sub#7（live 门不在低层） | §2.7（含契约扩展说明，独立核查 C27 partial 已注记） |
| 锚点子评审#10（身份检查两把锁混同——注意与 D0 子评审#10 编号无关） | §1.3 分开表述与测试 |
