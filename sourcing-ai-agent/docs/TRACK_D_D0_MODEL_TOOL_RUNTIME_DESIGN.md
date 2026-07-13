# Track D — D0+D1 批级设计：模型工具运行时 + 工具面 serve

> Status: Cross-model design input for owner review（2026-07-13，作者 = Claude Fable 5；只设计、不改码）。
> 上层计划：`TRACK_D_AGENT_RUNTIME_PLAN.md` §2 D0/D1。TD-1 已于 2026-07-13 由 owner 裁决采纳
> （requests + SSE 行解析，不引 async 栈），本文按此展开。
> 实施前按 handbook 纪律对全部行号锚点重新 Scout。

## 1. 批目标与不变量

**目标**：给 ModelClient 家族加一个 agentic 能力面（streaming + native tool-calling），并把工具
registry serve 出来——为 D2/D3 的 planner loop 提供模型 IO 与工具 schema 两个前置。

**硬不变量（characterize-first 钉死后逐字节保持）**：
1. `ModelClient` Protocol 的 14+3 个既有方法（`model_provider.py:617-650`）签名与行为不变；
   24 个消费模块零改动。AST 级守卫（复用 `tests/source_inspection.py` 形态）断言 Protocol 方法集
   与签名不漂移。
2. 既有单发调用的 payload 形状不变：chat `{model,messages,max_tokens,temperature:0}`
   （`:1860-1865`）、responses `{model,input,max_output_tokens,temperature:0}`（`:1923-1928`）。
3. 既有安全面不变且被新能力面**复用而非旁路**：per-provider 熔断
   （`_record_model_provider_success/_failure`，`:1881/:1892`）、live 双钥门
   （`assert_live_provider_access_allowed`，`:2291` 经 `build_model_client`）、产品模型锁
   （`_require_business_model_identity` / `CRM_PUBLIC_WEB_PRODUCT_MODEL`，`:33`）。

## 2. 新能力面（新模块 `model_tool_runtime.py`，不增肥 model_provider.py）

### 2.1 类型

```python
@dataclass(frozen=True)
class ToolSpec:
    name: str                 # = action_type（ActionRegistry 投影,永不手写）
    description: str          # = display_contract 描述
    input_schema: dict        # JSON Schema
    approval_policy: str      # 透传,loop 层执行前置检查用
    budget_required: bool

# AgentTurnEvent = 判别联合(dataclass + kind 字段):
#   text_delta(text) | tool_call(call_id,name,arguments_json) | usage(prompt,completion)
#   | stop(reason: "end_turn"|"tool_calls"|"max_tokens"|"error") | error(message)
```

### 2.2 会话协议

```python
class ToolCallingModelSession(Protocol):
    def run_tool_turn(self, messages, tools, *, max_tokens) -> list[AgentTurnEvent]: ...
    def stream_tool_turn(self, messages, tools, *, max_tokens) -> Iterator[AgentTurnEvent]: ...
```

**关键设计点：loop 正确性只依赖 `run_tool_turn`（buffered 事件列表），streaming 是传输层优化。**
`run_tool_turn` 的默认实现 = `list(stream_tool_turn(...))`；scripted/offline 实现只需给 buffered
版。这样 D2/D3 的 loop 逻辑全部离线可测，SSE 只在 OpenAI-compatible live 路径存在，且两条路径
产出**同一事件序列**（等价测试钉死）。

### 2.3 OpenAI-compatible 实现（D0 唯一 live 目标）

- 复用 `OpenAICompatibleChatModelClient` 的 settings/headers/circuit：新增
  `stream=True` + `tools=[...]` + `tool_choice="auto"` 的 chat/completions 调用，
  `requests.post(..., stream=True)` + `iter_lines()` 手解 SSE（`data: ` 帧、注释行跳过、
  `[DONE]` 终止）。
- **tool_calls delta 累积**是最易错点（OpenAI 把 `arguments` 按 delta 分片、以 `index` 归组）：
  累积器按 index 归组、拼接 arguments 分片、finish_reason=="tool_calls" 时 flush——单元测试
  必须覆盖分片/乱序缺 index/参数为空对象/多工具并发四种转写。
- `usage`：请求带 `stream_options:{"include_usage":true}`；不支持的后端缺 usage 帧时发
  `usage(unknown)` 而非猜测。
- 失败语义与单发一致：HTTP/请求异常进熔断记录并 raise；**流中断（半途断流）= error 事件 +
  stop(error)**，不静默截断——loop 层据此决定重试，session 层不自行重试流。
- Qwen/responses 与 deterministic：**D0 不实现 live tool-calling**。`DeterministicModelClient`
  与 Qwen 对 `run_tool_turn` 显式 raise `ToolCallingNotSupported`（fail-closed，禁止静默降级成
  无工具文本回答）；Qwen 支持随后续批按需补。

### 2.4 Scripted 回放（测试与 A/B 的基座，先于 live 存在）

- `ScriptedToolTurnSession`：从 JSONL 转写（每行一个 AgentTurnEvent + 请求指纹）回放；请求指纹
  =(messages digest, tools digest)，指纹不匹配 fail-closed（防转写错位假绿）。
- 录制器包装 live session 落转写——仅在 owner 显式授权 live 时运行（provider fail-closed 纪律
  原样适用；模型 provider 也在 `SOURCING_EXTERNAL_PROVIDER_MODE` 语义内）。
- 形态参照：`OfflineModelClient`（`:897`）/`ScriptedLivePlanningModelClient`（`:920`）。

## 3. D1 工具面 serve

- 新只读路由 `GET /api/agent/tool-registry`：
  `ActionRegistry.to_record()`（action 级：approval/budget/display_contract/
  allowed_workflow_command_types）为主体，per-action 附 `command_type_manifest()`
  （`durable_runtime.py:665-693`）中被 allowlist 允许的 command 条目（cancel/resume/control
  成熟度语义）。`ToolSpec` 生成器从这同一份投影派生——**registry 是 serve 与 ToolSpec 的单一来源**。
- **守卫（fast guard，机械断言）**：(a) 路由输出的 action 集合 ≡ ActionRegistry allowlist；
  (b) `legacy_internal_pending_activity_spine` 命令永不出现；(c) durable owner registry
  （`DEFAULT_COMMAND_TYPE_SPECS` 全集）不得直接成为输出——契约明文 durable registry 不是
  product allowlist。
- 不做：鉴权（C2 到位后该路由自然被覆盖）、写路由、schema 版本协商（第一版 `schema_version:1`）。

## 4. 批协议（handbook §4 形态的 D0 具体化）

1. **Scout**：重列 Protocol 方法与消费方（24 模块）清单；确认 `tests/test_model_provider.py`
   现有覆盖面，缺口补进特征化。
2. **特征化先行**：14 方法签名 AST 金快照 + 两种 payload 形状 + 熔断/识别锁行为测试；
   在对照树同跑绿后才动码。
3. **落地**：`model_tool_runtime.py` + api 路由 + 守卫测试；model_provider.py 只加不改
   （新 client 方法为 additive）。
4. **A/B**：scripted 转写驱动 `run_tool_turn` vs `stream_tool_turn` 事件序列逐字节等价；
   **变异自检：破坏 delta 累积器一处（如丢 index 归组）确认变红**。
5. **lane + 套件**：`make ci-pre-agent-contract` + `test_model_provider` + 新
   `test_model_tool_runtime` + `test_agent_tool_registry` + lint 门（mypy 87 棘轮）。
6. **文档**：本文追加批记录；`AGENT_OPERATION_CONTRACT.md` 若加 registry serve 语义需同步一段。
7. **异步评审**：settle 后按 gate 文档以 pinned scope 发 canonical runner review（非阻塞）。

## 5. 风险与开放点

- **SSE 解析自研的边界**：只解 `data:` 帧 + `[DONE]`，不实现完整 SSE 规范（retry/id/event 字段
  按需忽略）——写明于代码注释与测试，防止将来被当成通用 SSE 客户端复用。
- **产品模型锁的交互**：`gpt-5.6-sol` 锁（`:33`）目前只锁 `analyze_public_web_candidate_signals`
  路径；agentic loop 用哪个 model 由 TD-4 路由表裁决，D0 不预设——session 构造时显式传 model,
  不隐式继承 CRM 锁。
- **max_attempts 与流**：单发路径有 `_MODEL_PROVIDER_CALL_MAX_ATTEMPTS_ENV` 重试（`:1858`）；
  流式 turn 不做 session 内重试（半途重放会重复 side-effect 语义），重试权归 loop/owner 层。
