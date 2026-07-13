# Track D — D0+D1 批级设计：模型工具运行时 + 工具面 serve

> Status: Track D D0+D1 design record（v6 2026-07-13，作者 = Claude Fable 5）。2026-07-14 的 D0a/D0b/D0c
> 只落 additive、non-live 前置能力；它们不等于完整 D0/D1，也不授权 live/product 集成。
> 修订史：v1→R1 24 findings→v2（29 断言核查+24 路覆盖审计）→v3（R2 16 findings）→v4（R3 #9-15：
> terminal 事件、结果槽、路由快照、pin 生命周期、字段所有权、model-safe、预算台账）→v5（R4
> #10-15：快照创建点前移+digest 入指纹、pin 物理生命周期、归一化边界、三策略 revision、成本
> 分账）→**v6**（R5 #5/#6/#8：槽消费 CAS + accepted→superseded、dispatching 计费态、
> ModelInvocationEnvelope canonical 定义于 §2.2 含 terminal_reason；sweep blocker：槽 CAS 补
> control epoch + 槽写者单 owner 化）。覆盖映射见 §6-§7；sweep 报告
> `TRACK_D_INVARIANT_SWEEP_2026-07-13.md`。
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
# v4(round3#14) model-safe 边界补全:每个 served adapter 在 registry 注册类型化
# `model_safe_result_schema`(字段白名单 + 尺寸上限 + 分类标签 + 校验器 owner);出站校验覆盖
# **全部**消息角色(system/user/assistant 同样过出站策略,不只 tool_result);workspace/actor/
# permission/policy revision 绑入 ToolTurnResult、转写与 accepted-action journal 身份。
# artifact ref 永不自动解引用进 provider 输入——上送原文需 owner 显式白名单裁决。
# 脱敏发生在 adapter 出口(provider 暴露之前),转写录制脱敏(§2.8)是第二道。
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

# AgentTurnEvent(对执行永远 advisory,见 §2.5)——v4(#9)增加类型化终态变体,流不再有无类型旁道:
#   text_delta | tool_call_partial | usage | stop(reason) | error(message)
#   | terminal(result: ToolTurnResult)      # 流的最后一个事件,承载与 run_tool_turn 同一结果对象

@dataclass(frozen=True)
class ToolTurnResult:         # 唯一可授权后续动作的 canonical 语义结果
    text: str
    tool_calls: tuple[ToolCallRecord, ...]   # 已验证、有序、args 过 schema
    usage: ModelUsage                        # provider-neutral 单一五字段类型(model_usage.py)
    usage_status: str                        # reported | unavailable | invalid
    model_identity: ModelIdentity            # requested / response / effective + provenance
    terminal_reason: str                     # end_turn | tool_calls | length | content_filter
                                             # (v4 统一词汇:与解析器/授权集合同用 provider 语义 "length")
    provider_call_id: str | None
    route_id: str
    workspace_id: str; actor_id: str         # v4(#14):冗余镜像便于查询;信封为权威
    # 当前 D0a 内存结果没有 invocation_envelope_ref。未来 durable result-slot owner 持久化
    # ModelInvocationEnvelopeV1 后，在其 own wrapper/journal 中关联 owner-issued ref；不回填伪造值。

@dataclass(frozen=True, slots=True)
class ModelInvocationEnvelopeV1:             # 单一物理结果侧 schema；实现 owner = model_tool_runtime.py
    schema_version: str                      # 固定 model_invocation_envelope_v1
    route_id: str
    route_revision: str                      # route 完整内容 SHA-256
    provider: str
    api_style: str
    requested_model: str
    response_model: str
    effective_model: str
    model_identity_provenance: str           # 当前唯一 provider_response
    effective_route_snapshot_ref: str | None
    effective_route_snapshot_digest: str     # SHA-256；ref 缺席也不允许缺 digest
    circuit_identity: str
    runtime_namespace: str
    provider_mode: str                       # simulate | scripted | live；仅表达数据，不是执行许可
    workspace_id: str
    actor_id: str
    permission_scope: str
    prompt_policy_version: str
    permission_scope_revision: str
    outbound_policy_revision: str
    model_safe_schema_revision: str
    operation_run_id: str | None             # 以下六项必须全有或全无
    turn_id: str | None
    step_id: str | None
    workflow_command_id: str | None
    activity_run_id: str | None
    activity_attempt_id: str | None
    provider_call_id: str | None             # provider response 缺 call id 的 quarantine 可显式缺席
    terminal_reason: str                     # end_turn | tool_calls | length | content_filter
    usage: ModelUsage
    usage_status: str                        # reported | unavailable | invalid
    fallback_status: str                     # not_used | blocked | used
    circuit_state: str                       # not_checked | closed | open | half_open
    evidence_bundle_hash: str | None         # SHA-256；物理 bundle 不存在时为 None
    canonical_result_digest: str             # SHA-256
    result_artifact_ref: str | None          # ref/digest 必须成对出现或成对缺席
    result_artifact_digest: str | None        # SHA-256
    cost_exposure_ref: str | None
    canonical_request_digest: str            # SHA-256
    # envelope_digest 是 exact to_record() 的派生字段：对上述 canonical record 做
    # sorted-key compact JSON SHA-256，计算时排除 envelope_digest 自身，避免自循环。
```

`ModelInvocationEnvelopeV1` 的字段集即 D0 与 D3 §6 共用的 canonical 定义，不允许另建第二个物理
schema。严格反序列化要求顶层 keyset 完全相等；不接受 opaque dict、raw provider payload、credential、
authorization 或 API key。该 v1 是 **terminal-result-only**：它要求已观察到 provider response identity、
terminal reason 与 canonical result digest；pre-call、transport 与 protocol failure 尚不属于此 schema，未来必须
由闭集 attempt-outcome discriminator 或独立 attempt artifact 表达，禁止伪造 terminal/result。`None` 是当前
terminal result 中物理证据尚不存在的类型化缺席，尤其用于 simulate/scripted 与可审计的 quarantine outcome；
`provider_call_id=None` 只表示 response 未提供可用 call id，绝不证明“没有发生调用”。不得为满足非空检查伪造
snapshot/artifact/cost/evidence ref。真实 durable issuer 对 live
路径施加的更强 presence/CAS 规则仍由后续 owner 批决定。D0a 的 `simulate|scripted` 执行 predicate 与该
数据 schema 分离：schema 能无损表达未来 `live`，但绝不因此打开 live 路由。

`validate_tool_turn_result_envelope_mirror` 只 fail-closed 比对两个对象真正共享的权威字段（route、tenant/
actor、namespace/mode、request/result digest、模型身份、provider call、terminal、usage/status）。它是证据
一致性检查，不是 durable owner、result-slot accept/consume CAS、permission/budget/approval 或 effect 授权。

### 2.3 执行上下文（v2 新增——live 的准入契约）

```python
@dataclass(frozen=True)
class ModelTurnExecutionContext:
    route_id: str                    # §4 路由表条目;不接受裸 model 字符串
    route_revision: str              # v3(#13):路由条目内容 digest,随 action/command/attempt/请求
                                     # hash/result 全链持久化——部署改路由不影响已排队重试的语义
    effective_route_snapshot_ref: str  # v5(round4#10):快照在 **action/approval/command 创建点**固化
                                     # (claim 时固化太晚——部署后首次 claim 的命令会拿到新配置);
                                     # 内容 = 非密钥 endpoint 身份/base_url digest、timeout、api_style、
                                     # 定价类、circuit policy id;随 retry 全链传播;**其 digest 计入
                                     # §2.8 canonical request hash**。settings.py:22-29 base_url/
                                     # timeout 可变、熔断键含 base_url(:1294-1295),route_revision
                                     # 单独不足以钉住。该快照契约为**共享模型调用契约**(D3 §6 同用)。
    permission_scope_revision: str   # v5(round4#12):权限/出站策略/model-safe schema 三个 revision
    outbound_policy_revision: str    #   进 context、ToolTurnResult、结果槽身份与 §2.8 request hash,
    model_safe_schema_revision: str  #   转写回放与 accepted-action journal 身份同绑
    workspace_id: str; actor_id: str; permission_scope: str   # v3(#14):租户/主体绑定,owner 铸造
    operation_run_id: str; turn_id: str; step_id: str; attempt: int
    workflow_command_id: str; activity_run_id: str            # v3(prior#8):物理因果 id
    idempotency_key: str
    budget: ModelTurnBudget          # token/monetary 上限 + deadline_at(UTC 墙钟,持久化;
                                     # attempt claim 后派生 attempt-local 单调 deadline 执行——
                                     # v4 统一命名,不再另有 wall_deadline)
    budget_reservation_ref: str      # 成本台账(v5 修正 round4#13——预留与物理调用暴露分账):
                                     # 单一 PG cost-ledger owner;父行 = worst-case 预留(挂 operation);
                                     # 子行 = (reservation, activity_attempt, physical_call_index)
                                     # 暴露行,状态 prepared→dispatching→sent→confirmed|uncertain|no_call
                                     # ——v6(R5#6):任何 wire 写之前先落 dispatching(保守可能已发送态);
                                     # 只有可证明的 transport 前中止才转 no_call;停在 dispatching/sent
                                     # 的 crash 一律按 uncertain 以 worst-case 预留计,直至 provider
                                     # 对账或保守消耗;confirmed/uncertain 对账后经 CAS 释放未用预留。
                                     # live 付费路径落地前该台账必须在位。
    activity_attempt_id: str         # v4:物理 attempt 身份(替代裸 int attempt 计数)
    approval_ref: str | None         # 审批证据引用(路由的 budget_class 要求时必填)
```
live 路径缺 context 或 budget 任一字段 ⇒ 构造期 raise（不发网络请求、不计熔断——本地配置失败
不污染熔断，对齐 `DURABLE_EXECUTION_RUNTIME_CONTRACT.md:157` 的 pre-transport 失败 carve-out）。
`usage_status != reported` 的付费调用按 `monetary_ceiling` 的保守保留额记账，**缺失 usage 永不
静默视为零成本**。

### 2.4 会话协议与实现基类（v2 修正 v1 的 Protocol 自相矛盾）

```python
class ToolCallingSessionBase(abc.ABC):
    # v3(#9/#22):canonical 构造源 = 解析器状态,不是事件流。
    # 解析器产出类型化 outcome:ParsedTurn = {advisory_events: [...], terminal_result: ToolTurnResult}
    # ——terminal_result 由解析器内部状态(含服务端信封:response model/call id/usage/route)构造,
    # 事件流只是同一解析器状态的 advisory 投影,两者不存在"从事件重建结果"的方向。
    def run_tool_turn(self, ctx, messages, tools) -> ToolTurnResult:   # = parse(...).terminal_result
    def stream_tool_turn(self, ctx, messages, tools) -> Iterator[AgentTurnEvent]:
        # 逐 advisory 事件产出,最后一个事件 = terminal_result 的显式 terminal 事件
    # scripted 子类:实现 canonical ToolTurnResult 回放,基类按 coalescing 规则 v1 合成 advisory 流。
```
**等价测试改为语义等价**（v2 修正）：同一 wire 转写的多种合法 SSE 分块切法（逐字节流/整帧/跨帧
切分/args 分片重组）→ 独立手写的期望 `ToolTurnResult` 完全一致；不再断言事件逐帧相等。
v3 追加：流路径消费到的 terminal 事件所载结果与 `run_tool_turn` 返回值逐字段一致（同一解析器
状态、两种投影）。

### 2.5 「流式输出 advisory、终态结果 authorize」（v2 新增核心安全规则）

- 流事件（含看似完整的 `tool_call_partial`）一律**不可执行**；只有经 §2.6 状态机验证的 terminal
  `ToolTurnResult` **且 terminal_reason 属可授权集合**才能作为提交 `AgentAction` 的依据。
  **v3（#11）**：`length` / `content_filter` / 未知 reason 的 terminal 结果 =
  `authorizable=false` 的隔离终态（结果照记、证据照留，**不得授权任何 action**）——看似完整或
  schema-valid 的截断前缀不能逃逸；可授权集合初始只含 {`end_turn`, `tool_calls`}。
- **v4（round3#10）工具执行去重改为逻辑结果槽 + 首成功 CAS；v8 修 R7#1 的多 call 语义**：
  provider `tool_call_id` 仅证据；owner 为每个 (turn_id, step_id) 维护 workspace-scoped 逻辑
  结果槽——首个验证通过的 terminal 结果以 CAS 占槽（journal 落库得 `accepted_result_id` +
  **canonical terminal-outcome digest**：覆盖终态变体 + text + **有序 call 多重集**）；重试结果
  **只在 outcome digest 精确相等时 join**（子集/超集/重排一律 quarantine——不再按单 call
  比对产生歧义 join）；同一逻辑 call 出现多次 ⇒ 每次获得**稳定 occurrence 序号**入身份。
  action 身份 = `(result_slot_id, slot_generation, tool_name, canonical_args_digest,
  occurrence_ordinal)`；**有意重新生成**显式推进槽 generation。
- **v5（round4#5）槽的取消/失效围栏**：槽自带 `slot_state ∈ {open, accepted, consumed, closed,
  superseded}`；**接受 CAS 的全条件** = 槽 open + 所属 OperationRun/turn/WorkflowCommand 均非终态
  + claim/attempt 身份匹配 + **durable control epoch 匹配**（sweep 修正：requeue→重 claim 窗口的
  ABA 同样适用于槽，与 D3 §4c 同一 epoch 机制）+ schema/route/policy pins 匹配；cancel/
  supersession 原子关槽（open→closed，**accepted→superseded**——v6 补 R5#5）；槽状态转移的
  **写者 = turn owner 的域命令**（generic 控制面经 事件→reducer→turn owner 命令收敛，与 D3 §4c
  同构，不跨 owner 直写）。
- **消费 CAS（v6，R5#5；v7 修 R6#12/#7）**：消费 = **完整终局结果的原子消费**，不是"一个
  action 一次转移"：`end_turn` 结果的消费 UoW 持久化最终 assistant 输出 + turn 完成记录
  （零 action，槽照样 accepted→consumed，不再滞留）；`tool_calls` 结果的消费 UoW 持久化
  **全部确定性 action 集 + journal + 期望数量/集合 digest**（部分持久化 = 整 UoW 回滚）。
  谓词含槽 generation + **durable control epoch** + canonical operation/turn/command/claim/
  route/schema/policy pins；approve 与 dispatch 各自复查同一全集。cancel 落在「接受后、消费前」
  窗口时 `accepted→superseded` 抢先。晚到 terminal 结果一律 quarantine 证据。
- **终态与形状的判别联合（v7 修 R6#8）**：`ToolTurnResult` 语义上是判别联合——`end_turn` 当且
  仅当 provider reason=stop 且 call 数为 0；`tool_calls` 当且仅当 reason=tool_calls 且 ≥1 个
  完整验证过的 call；任何错配（stop 带 call、tool_calls 零 call）⇒ 协议失败 + quarantine，
  不入可授权集合。
  **运行时隔离（R6#2 同 D3）**：信封/槽/journal/action 携带不可变 runtime_namespace +
  provider_mode，scripted/simulate 结果不可流入 live 命名空间的任何授权路径。
- 部分输出保留为 quarantined 证据（attempt 级 artifact），不进结果。

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
- **墙钟**：以持久化 UTC `deadline_at` 在 attempt claim 后派生的 attempt-local 单调 deadline
  跨帧强制（v5 修正 :178 与 §2.3 的命名矛盾），非仅 requests read timeout；超时 ⇒
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
  `{route_id, route_revision, effective_route_snapshot_digest, provider, model, api_style,
  max_tokens, tool_choice, stream_options, message_model_version, tools_schema_digest,
  prompt_policy_version, permission_scope_revision, outbound_policy_revision,
  model_safe_schema_revision, messages_digest, **workspace_id, actor_id, permission_scope,
  transcript_digest**}`（v7 修 R6#10：回放身份含租户——同请求不同租户不得命中同一转写）；
  **live 录制的转写 tenant-bound、跨租户不可回放**；只有显式标注 `synthetic` 的合成 fixture
  可跨租户复用（接受时校验信封租户等式）。任一字段变化 ⇒ 回放 fail-closed（测试逐字段验证）。
- 转写治理：落 `runtime/model_turn_transcripts/` 命名空间（runtime 不入库）；每份带
  schema_version、大小上限、保留 TTL；**录制管线内置脱敏**——转写只存归一化事件（永不存 raw
  provider payload），敏感字段（候选人姓名/邮箱/电话/CRM 备注）按字段级脱敏规则替换为占位符+
  digest，超限内容替换为 artifact ref + 摘要；含真实候选人/CRM 数据的转写**禁止**作为
  source-controlled fixture——入库 fixture 必须合成数据。
- 录制器包装 live session，仅 owner 显式授权时运行（provider fail-closed 纪律原样适用）。

## 3. D1 工具面 serve（v2 重构——先补 schema 地基，serve 可执行子集）

### 3.1 输入 schema 事实源（v2 新增，本批最重要的地基项）

- `ActionSpec` 扩展为**`ActionRequestSpec`**（v3 #10：schema 必须同时覆盖 `input_payload` 与
  `target_ref_json`——pinned dispatch 从两处取行为驱动值，`orchestrator.py:47253-47309`，只校验
  input 会被 target 旁路）：`request_schema: dict`（含 input 与 target 两段）+
  `request_schema_version: str`。单一事实源，三方消费同一份：
  (a) `POST /api/operations/actions` 提交路径按 schema 校验（顺带修复零校验）；
  (b) planner `ToolSpec.input_schema`（serve input 段；**target 段服务端绑定**——模型提供的
  target 字段一律拒绝，target 由 owner 从会话上下文铸造）；(c) dispatch adapter 入参校验。
- **v4（round3#12）schema pin 的物理生命周期**：pin（`request_schema_version` + digest）为
  **AgentAction 行上 owner 写入的不可变列**（提交时写）；`OperationRun` 在其实际创建点复制并
  校验 pin——审批型 action 的 run 在 approve 时才建（`operation_runtime.py:813-827`）、retry 子
  run 在 `:1042-1061` 才建，两处都执行 copy+verify；幂等重放时 pin 不匹配 ⇒ fail-closed 拒绝
  （不静默沿用任一侧）；保留元数据键防碰撞。dispatch 兼容性校验同前（digest 不匹配 ⇒ 转人工）。
- 迁移策略 fail-closed：schema 按 action 渐进补齐；无 schema 的 action 不进 agent 工具面；
  宽松提交路径登记 residual 台账行 + 命中指标，**deletion condition 非同义反复**：追踪**全部
  API 可提交 action**（非 served 子集）的宽松路径命中，连续一个发布窗口零命中方可关闭。
- **v4（round3#13）字段所有权归一化**：per-action 声明字段 owner——`target_ref` 只含 **owner
  铸造的资源身份**（模型/客户端提供的 target 字段一律拒绝）、`input_payload` 只含请求选项；
  两处出现同名字段或 input 别名试图覆盖 owner target ⇒ 提交即拒；dispatch 只消费归一化后的
  request（消除 pinned 的 input-优先于-target 双源行为）。

### 3.2 serve 子集：`agent_tool_enabled` 谓词（v2 修正「全集等价」）

served ⊆ ActionRegistry，当且仅当：
1. `request_schema` 已定义；
2. **dispatch 就绪**：`ActionSpec` 新增 `dispatch_adapter: str` 显式绑定字段（v3 prior#6——
   注册即声明 adapter，谓词检查绑定存在且 adapter 已注册），dispatch adapter 集合从该字段派生（替换
   `orchestrator.py:47091-47096` 的硬编码字面集合——独立核查 C15：该集合与
   `allowed_workflow_command_types` 元数据脱节，`plan_acquisition`/`promote_person_assertion`/
   `external_intake` 注册在案却 dispatch `unsupported`，12/15）；
3. activity-spine 校验通过（**语义断言**，v2 修正 v1 空断言——`legacy_internal_pending_activity_spine`
   是 policy 值不是命令类型：对每个 served command 断言
   `activity_spine_policy.requirement != legacy-internal` 且 `agent_callable`，复用注册期校验
   `operation_runtime.py:121-134`；注意该校验只遍历 `allowed_workflow_command_types`——空命令面
   action 由本谓词第 2 条兜住）;
4. **（v7 修 R6#9）已注册的 revisioned `model_safe_result_schema` + 校验器 owner 在位**——缺
   出站白名单的 tool 不 serve（§2.1 强制项进谓词，敏感输出上 wire 的路径结构性关死）；
5. **simulate dispatch preflight**：每个 served tool 在 simulate 模式实际 dispatch 成功一次，
   **且 preflight 实际行使 model-safe 序列化器**（contract lane 级守卫，防注册元数据与 adapter
   再度脱节）。

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
**初始表（owner 2026-07-13 批准起草并按推荐落档；rollout_state 全部 = draft，live 启用前
逐条升 canary 仍需 owner 拨动）**：

| route_id | provider | model | api_style | capabilities | budget_class | simulate_mapping |
|---|---|---|---|---|---|---|
| `agent.planner.loop` | openai_compatible relay | `gpt-5.6-sol` | openai_chat_completions | stream,tools,usage,identity_check | agent_turn_standard | scripted_tool_turn |
| `company.identity.adjudicate` | openai_compatible relay | `gpt-5.6-sol` | openai_chat_completions | usage,identity_check | adjudication_small | scripted_adjudication |

理由：owner 现有最强 relay 模型即 gpt-5.6-sol，chat_completions 是 D0 唯一 live 目标；
CRM 的产品模型锁与本表**互不引用**（锁在 `analyze_public_web_candidate_signals` 路径，
本表在 agent/裁决路径——同模型是巧合不是耦合，两处各自独立变更）。`fallback_policy =
fail_closed` 唯一初始值。路由变更 = 配置提交 + 审计，不是运行时行为。

D0c 机械 preflight 对 route record、manifest 顶层与 manifest route record 使用显式 exact keyset；要求
`route_id` 与 `circuit_key` 各自全局唯一、所有 checked-in route 都是 `draft`、`live_enabled=false`，且
manifest 与 checked-in revision 逐字段完全一致。该 preflight 不包含 canary/live predicate，也不改变 D0a
只允许 `simulate|scripted` 的执行门。

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

## 7. v2 re-review（artifact `20260713T122908Z`）新 findings 覆盖映射

| finding | 处置 |
|---|---|
| new#9（事件流无法构造 canonical 结果） | §2.4 解析器状态为构造源 + ParsedTurn 类型化 outcome + terminal 事件 |
| new#10（target_ref 旁路 schema） | §3.1 ActionRequestSpec 覆盖 input+target；target 服务端绑定 |
| new#11（截断/过滤结果可授权） | §2.5 authorizable 终态集合 {end_turn, tool_calls}，length/content_filter 隔离 |
| new#12（去重键非重试稳定） | §2.5 owner 铸造 accepted_result_id + ordinal/name/args digest；provider id 仅证据 |
| new#13（路由/deadline 不可持久化） | §2.3 route_revision 全链持久化 + deadline_at 墙钟 + claim 后派生单调钟；§2.8 指纹含 revision |
| new#14（无 model-safe/租户契约） | §2.1 adapter 出口白名单 + artifact ref 不自动解引用；§2.3 workspace/actor/permission 绑定 |
| new#15（schema 版本未绑 durable 对象/桥无治理） | §3.1 版本+digest 落 AgentAction/OperationRun + dispatch 兼容校验 + 宽松路径登记迁移桥 |
| prior#5/#6/#8/#22/#23 partial 收口 | §3.1（target/版本/桥）/§3.2（dispatch_adapter 绑定字段）/§2.3（因果 id+预算台账）/§2.4/§2.3+§2.8 |

**round-3（提取件 `20260713T125255Z_*`）覆盖**：#9→§2.2 terminal 事件变体（类型化，无旁道）；
#10→§2.5 逻辑结果槽 + 首成功 CAS + join/quarantine/显式 generation；#11→§2.3/§2.2 有效路由快照
（endpoint/timeout/定价/circuit policy）+ UTC deadline_at 命名统一；#12→§3.1 pin 不可变列 +
approve/retry 创建点 copy+verify + 非同义反复的桥退役条件；#13→§3.1 字段所有权归一化；
#14→§2.1 model_safe_result_schema + 全角色出站校验 + 租户身份入结果/转写/journal；
#15→§2.3 预算保留台账契约（owner/身份/状态机/对账/orphan 回收）；命名更正→§2.2
（ModelUsage；OpenAIModelUsage 仅兼容别名、length）。
