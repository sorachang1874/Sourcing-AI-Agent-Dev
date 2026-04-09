# Frontend API Contract

这份文档定义 Web 层当前应该如何消费后端 API，尤其是：

- `plan -> review -> workflow -> progress -> results` 的标准接口顺序
- `intent_brief` 和 `intent_rewrite` 的职责分工
- 哪些响应适合做“语义解释层”
- 哪些响应只适合做“状态刷新层”

目标不是穷举所有内部字段，而是定义一套前端可稳定依赖的 contract。

仓库内对应的共享类型资产：

- `contracts/frontend_api_contract.ts`
- `contracts/frontend_api_contract.schema.json`
- `contracts/frontend_api_adapter.ts`
- `contracts/frontend_react_hooks.example.tsx`

推荐用法：

- 前端 TypeScript 项目直接复用 `frontend_api_contract.ts`
- 若想直接获得 typed `fetch` 调用示例，可复用 `frontend_api_adapter.ts`
- 若前端是 React，可直接参考 `frontend_react_hooks.example.tsx`
  - 已示例 `useSourcingPlan / useReviewInstructionPreview / useStartWorkflow / useJobProgress / useJobResults / useWorkflowRun`
- 后端或集成测试若需要做 response 校验，可按 JSON Schema 中的 `$defs` 引用：
  - `#/$defs/PlanResponse`
  - `#/$defs/ReviewInstructionCompileResponse`
  - `#/$defs/ReviewPlanApplyResponse`
  - `#/$defs/WorkflowStartResponse`
  - `#/$defs/JobProgressResponse`
  - `#/$defs/JobResultsResponse`
  - `#/$defs/RetrievalJobResponse`
  - `#/$defs/RefinementCompileResponse`
  - `#/$defs/RefinementApplyResponse`

adapter 最小示例：

```ts
import { createSourcingAgentApiClient } from "../contracts/frontend_api_adapter";

const api = createSourcingAgentApiClient({
  baseUrl: "http://127.0.0.1:8765",
});

const plan = await api.plan({
  raw_user_request: "帮我找 Anthropic 的华人成员",
  target_company: "Anthropic",
  planning_mode: "model_assisted",
});

console.log(plan.intent_rewrite.request.summary);
```

React hook 最小示例：

```tsx
import { useSourcingPlan, useWorkflowRun } from "../contracts/frontend_react_hooks.example";

export function SourcingConsole() {
  const plan = useSourcingPlan({
    baseUrl: "http://127.0.0.1:8767",
  });
  const workflow = useWorkflowRun({
    baseUrl: "http://127.0.0.1:8767",
    pollIntervalMs: 5000,
  });

  async function handlePlan() {
    await plan.run({
      raw_user_request: "帮我找 Anthropic 的华人成员",
      target_company: "Anthropic",
      planning_mode: "model_assisted",
    });
  }

  async function handleStartWorkflow() {
    const reviewId = Number(plan.data?.plan_review_session?.review_id);
    if (!reviewId) {
      return;
    }
    await workflow.start({
      plan_review_id: reviewId,
    });
  }

  return (
    <section>
      <button onClick={handlePlan} disabled={plan.loading}>
        Run plan
      </button>
      <button onClick={handleStartWorkflow} disabled={!plan.data || workflow.startState.loading}>
        Start workflow
      </button>
      <pre>{plan.data?.intent_rewrite.request.summary}</pre>
      <pre>{workflow.progress.data?.current_message}</pre>
      <pre>{workflow.results.data?.results?.length ?? 0}</pre>
    </section>
  );
}
```

## 1. 设计原则

- 后端是 request normalization 的唯一真源
  - 前端不要自己复刻 `华人 -> Greater China experience` 这类 rewrite 规则
- `intent_brief` 和 `intent_rewrite` 都应被视为产品语义输出，而不是调试字段
- `progress` 接口负责阶段状态，不负责重复返回完整语义解释
- 前端应缓存最早拿到的 `intent_rewrite`，并在 progress / results 页继续复用

## 2. 核心对象

### 2.1 `intent_brief`

用途：

- 面向用户解释“系统识别到了什么请求、准备交付什么、默认怎么执行”

主要出现在：

- `POST /api/plan`
- `POST /api/workflows`
- `GET /api/jobs/{job_id}/results`

推荐渲染：

- 第一屏 plan 卡片
- result 页顶部的 execution recap

### 2.2 `intent_rewrite`

用途：

- 面向用户和 operator 显式解释“系统把原始自然语言改写成了什么结构化意图”

主要出现在：

- `POST /api/plan`
- `POST /api/plan/review/compile-instruction`
- `POST /api/results/refine/compile-instruction`
- `POST /api/results/refine`
- `POST /api/workflows`
- `POST /api/jobs`
- `GET /api/jobs/{job_id}`
- `GET /api/jobs/{job_id}/results`

不出现在：

- `GET /api/jobs/{job_id}/progress`

原因：

- progress 轮询会很频繁
- 它只需要阶段、计时、worker 状态
- 语义解释层应来自 `plan / compile / results`

## 3. `intent_rewrite` 稳定结构

顶层结构：

```json
{
  "intent_rewrite": {
    "request": {
      "matched": true,
      "summary": "自然语言简称改写：华人 / 泛华人简称 -> 中国大陆 / 港澳台 / 新加坡公开学习或工作经历 / 中文 / 双语 outreach 适配",
      "rewrite": {
        "rewrite_id": "greater_china_outreach",
        "summary_label": "华人 / 泛华人简称",
        "keywords": [
          "Greater China experience",
          "Chinese bilingual outreach"
        ],
        "targeting_terms": [
          "中国大陆 / 港澳台 / 新加坡公开学习或工作经历",
          "中文 / 双语 outreach 适配"
        ],
        "matched_terms": [
          "华人"
        ]
      }
    },
    "instruction": {
      "matched": false,
      "summary": "",
      "rewrite": {}
    }
  }
}
```

字段说明：

- `request`
  - 对应原始 `raw_user_request` 或 `query`
- `instruction`
  - 只在存在自然语言 operator instruction 的编译场景下出现
  - 例如 review-plan compile、refinement compile
- `matched`
  - 是否命中了后端 rewrite 规则
- `summary`
  - 前端默认展示用的一行人类可读文案
- `rewrite`
  - 结构化 rewrite payload，供详情面板、回放和审计使用

前端处理规则：

- `matched=false`
  - 视为 “No rewrite applied”
  - 不要当成错误
- `summary=""`
  - 直接不展示 summary 行即可
- `rewrite={}`
  - 说明没有命中 rewrite，不需要额外兜底逻辑

## 4. 标准页面流与接口顺序

### 4.1 Plan Page

接口：

- `POST /api/plan`

前端应消费：

- `request`
- `plan.intent_brief`
- `plan_review_gate`
- `plan_review_session`
- `intent_rewrite`

其中 `plan_review_gate.execution_mode_hints` 用来承载“怎么执行更经济”的结构化提示，典型字段包括：

- `segmented_company_employee_shard_strategy`
- `segmented_company_employee_shard_count`
- `segmented_company_employee_shards`
- `incremental_rerun_recommended`
- `recommended_decision_patch`
- `operator_instruction_examples`
- `local_reusable_roster_snapshot`

前端应缓存：

- `plan_review_session.review_id`
- `intent_rewrite`
- `request`
- `plan.intent_brief`

推荐 UI：

- 主卡片：`plan.intent_brief`
- 次卡片：`intent_rewrite`
  - `matched=true` 时显示 “System rewrite applied”
  - `matched=false` 时可折叠或隐藏
- operator 提示卡：`plan_review_gate.execution_mode_hints`
  - 大公司 fresh live run 成本高时，前端应把推荐的 `recommended_decision_patch` 和自然语言 `operator_instruction_examples` 显式展示出来

### 4.2 Review Preview Page

接口：

- `POST /api/plan/review/compile-instruction`

这个接口是 review 页的语义真源。

前端应消费：

- `review_payload`
- `instruction_compiler`
- `intent_rewrite`

推荐理解：

- `instruction_compiler`
  - 告诉前端“这条 instruction 被编译成了哪些实际 decision”
- `intent_rewrite.request`
  - 告诉前端“原始用户 query 有没有被 rewrite”
- `intent_rewrite.instruction`
  - 告诉前端“本次 operator instruction 有没有命中 rewrite”

注意：

- `POST /api/plan/review` 是 mutation endpoint
- Web 层不要把它当 review summary 的主读取接口
- review summary 应优先来自 compile-instruction 的返回

### 4.3 Workflow Start

接口：

- `POST /api/workflows`

前端应消费：

- `job_id`
- `status`
- `stage`
- `plan`
- `plan_review_session`
- `intent_rewrite`

推荐做法：

- workflow 创建成功后，把 `job_id` 和当前缓存的 `intent_rewrite` 绑定
- 若 `POST /api/workflows` 自身也返回了 `intent_rewrite`，以后者为准

### 4.4 Progress Page

接口：

- `GET /api/jobs/{job_id}/progress`

前端应消费：

- `status`
- `stage`
- `elapsed_seconds`
- `blocked_task`
- `current_message`
- `progress.milestones`
- `progress.worker_summary`
- `progress.counters`

前端不应期待：

- 完整 `intent_rewrite`

推荐做法：

- progress 页直接复用之前缓存的 `intent_rewrite`
- 若用户刷新页面且本地状态丢失，可再调用 `GET /api/jobs/{job_id}/results`

### 4.5 Results Page

接口：

- `GET /api/jobs/{job_id}/results`

前端应消费：

- `job`
- `results`
- `manual_review_items`
- `agent_runtime_session`
- `agent_workers`
- `intent_rewrite`

用途分工：

- `job.request`
  - 原始结构化 request
- `job.summary`
  - retrieval 层 summary
- `intent_rewrite`
  - request normalization 回放
- `results`
  - 候选人结果
- `manual_review_items`
  - 边界项

## 5. 推荐的前端状态模型

推荐按下面方式缓存：

```ts
type WorkflowUiState = {
  planReviewId?: number
  jobId?: string
  request?: Record<string, unknown>
  intentBrief?: {
    identified_request: string[]
    target_output: string[]
    default_execution_strategy: string[]
    review_focus: string[]
  }
  intentRewrite?: {
    request: {
      matched: boolean
      summary: string
      rewrite: Record<string, unknown>
    }
    instruction?: {
      matched: boolean
      summary: string
      rewrite: Record<string, unknown>
    }
  }
}
```

状态演进建议：

1. `POST /api/plan`
   - 初始化 `planReviewId / request / intentBrief / intentRewrite`
2. `POST /api/plan/review/compile-instruction`
   - 覆盖 `intentRewrite.instruction`
3. `POST /api/workflows`
   - 写入 `jobId`
4. `GET /api/jobs/{job_id}/progress`
   - 只刷新状态字段，不覆盖 `intentRewrite`
5. `GET /api/jobs/{job_id}/results`
   - 若需要，以结果页返回的 `intentRewrite` 做最终校正

## 6. 推荐的渲染方式

### 6.1 默认展示

- 主展示：`intent_rewrite.request.summary`
- 仅当 `matched=true` 时显示

示例：

- `自然语言简称改写：华人 / 泛华人简称 -> 中国大陆 / 港澳台 / 新加坡公开学习或工作经历 / 中文 / 双语 outreach 适配`

### 6.2 展开详情

建议在 “Why this query was rewritten” 折叠面板里展示：

- `rewrite.summary_label`
- `rewrite.matched_terms`
- `rewrite.keywords`
- `rewrite.targeting_terms`

### 6.3 Review 场景

如果当前页面是 review-plan preview 或 refine-results preview：

- 同时展示 `request` rewrite 和 `instruction` rewrite
- 若 `instruction.matched=false`，可以只显示 request rewrite

## 7. 当前稳定接口摘要

### `POST /api/plan`

最重要的稳定字段：

- `request`
- `plan.intent_brief`
- `plan_review_gate`
- `plan_review_session`
- `intent_rewrite`

### `POST /api/plan/review/compile-instruction`

最重要的稳定字段：

- `review_payload`
- `instruction_compiler`
- `intent_rewrite`

### `POST /api/workflows`

最重要的稳定字段：

- `job_id`
- `status`
- `stage`
- `plan`
- `intent_rewrite`

### `GET /api/jobs/{job_id}/progress`

最重要的稳定字段：

- `status`
- `stage`
- `elapsed_seconds`
- `blocked_task`
- `current_message`
- `progress`

### `GET /api/jobs/{job_id}/results`

最重要的稳定字段：

- `job`
- `results`
- `manual_review_items`
- `intent_rewrite`

## 8. 一个具体例子

用户 query：

- `帮我找 Anthropic 的华人成员`

预期前端行为：

1. `POST /api/plan`
   - 读到 `intent_rewrite.request.matched=true`
   - 展示 rewrite summary
2. operator 再输入：
   - `scope 大一些，要整家公司全量成员`
3. `POST /api/plan/review/compile-instruction`
   - 读到：
     - `intent_rewrite.request.matched=true`
     - `intent_rewrite.instruction.matched=false`
4. workflow 运行中：
   - 轮询 `GET /api/jobs/{job_id}/progress`
   - 页面仍保留第一步拿到的 rewrite summary
5. 结果页：
   - `GET /api/jobs/{job_id}/results`
   - 若本地状态已丢失，直接从结果页里的 `intent_rewrite` 重新恢复

## 9. 非目标

当前 contract 不承诺：

- `progress` 接口返回完整 rewrite 语义
- plan review mutation 接口自身重复返回 compile preview 的所有解释字段
- 前端只靠一次 API 调用就拿到所有页面所需的所有信息

当前推荐方式是：

- 语义解释来自 `plan / compile / results`
- 状态刷新来自 `progress`
