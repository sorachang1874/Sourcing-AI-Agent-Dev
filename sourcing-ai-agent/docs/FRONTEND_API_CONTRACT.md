# Frontend API Contract

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义 Web 层当前应该如何消费后端 API，尤其是：

- `plan -> review -> workflow -> progress -> results` 的标准接口顺序
- `intent_brief` 和 `intent_rewrite` 的职责分工
- 哪些响应适合做“语义解释层”
- 哪些响应只适合做“状态刷新层”

目标不是穷举所有内部字段，而是定义一套前端可稳定依赖的 contract。

补充约定：

- `request` 表示当前后端准备真正执行的 execution-aligned request，而不只是最初的归一化输入
- `request_preview` 应被视为 `request` 的可解释展示层，两者在 `keywords / organization_keywords / employment_statuses / intent_axes` 上应保持同一语义
- 如果 planning / acquisition strategy 扩展了执行关键词，例如把用户 query 补成 `Pre-train`、`Vision-language` 这类真实 shard / provider keyword，前端应直接信任返回的 `request` / `request_preview`，而不是自己从原始 query 再做一轮猜测
- 前端接 hosted 后端时，默认不应再把 `force_fresh_run=true` 作为通用默认值
  - 当组织级 baseline 已经 `effective_ready`，这会直接绕开本地资产复用，重新触发高成本 provider
- `POST /api/workflows/explain` 应作为前端的 dry-run / operator explain 入口
  - 它现在会返回 `generation_watermarks`、`asset_reuse_plan`、`cloud_asset_operations`
  - 适合在真正 `start workflow` 前展示“这次会复用哪个 baseline、是否只补 delta、最近有没有云端 import/GC”
- `GET /api/runtime/progress` 应作为运维面板的聚合入口
  - 它现在会返回 `cloud_asset_operations` 和按公司过滤的 `company_asset`
  - 不需要前端自己再拼 runtime 文件或 registry
- 本地前端直连 hosted backend 时，默认使用 `http://127.0.0.1:4173 -> http://127.0.0.1:8765`
  - 后端会为 `127.0.0.1:4173` / `localhost:4173` 返回 CORS 响应头
  - 若前端运行在其他域名或端口，运维应设置 `SOURCING_API_ALLOWED_ORIGINS`
- 前端的“人工审核状态”和“目标候选人 CRM”现在已经切到后端 API + Postgres-backed control plane
  - 不再以浏览器 `localStorage` 作为真相源
  - 同一个 `job_id` 的审核状态，应通过 `GET/POST /api/candidate-review-registry` 读写
  - 跨 workflow 的目标候选人池，应通过 `GET/POST /api/target-candidates` 读写
  - 将某个已完成 workflow 的候选人批量导入目标池，应通过 `POST /api/target-candidates/import-from-job`
  - 导出目标候选人 CSV/profile bundle，应通过 `POST /api/target-candidates/export`
  - 目标候选人 Public Web Search 应通过 `POST /api/target-candidates/public-web-search` 触发，通过 `GET /api/target-candidates/public-web-search` 查询 batch/run 状态
    - POST 只做幂等排队，不在请求线程里跑 DataForSEO/fetch/LLM
    - 返回的 per-candidate run 是事实来源；batch 只做多选操作的聚合状态
    - 前端应按 `record_id` 将最新 run 映射到候选人卡片，稳定消费 `status / phase / summary / query_manifest / search_checkpoint / analysis_checkpoint / updated_at`
    - `summary.primary_links`、`summary.entry_link_count`、`summary.fetched_document_count`、`summary.email_candidate_count` 和 `summary.promotion_recommended_email_count` 可作为卡片级紧凑展示；完整 grouped signals/detail 必须通过 record detail API 获取，不应从 raw artifacts 反推
    - 目标候选人 Public Web detail 应通过 `GET /api/target-candidates/{record_id}/public-web-search` 获取，响应包含 `latest_run`、`person_asset`、`signals`、`email_candidates`、`profile_links`、`grouped_signals` 和 `evidence_links`
    - detail 响应只返回 first-class `person_public_web_signals` 的 model-safe summaries/evidence links；不返回 raw HTML/PDF/search payload、`search_checkpoint` 或 raw document paths
    - detail 中的 profile/public link signal 会携带 `link_shape_warnings` 和 `clean_profile_link`
      - `x_link_not_profile`、`substack_link_not_profile_or_publication`、`github_repository_or_deep_link_not_profile`、`scholar_link_not_profile` 必须作为 UI warning 展示
      - 即使 identity label 是 `likely_same_person`，`clean_profile_link=false` 的链接也只能作为 evidence/review signal，不应渲染成 clean profile 或 primary link
    - Public Web email candidates 可以展示为候选联系方式，但不能直接写入 `target_candidates.primary_email`；提升为 primary email 必须通过 `POST /api/target-candidates/{record_id}/public-web-promotions` 先写 promotion record
    - Public Web link/email promotion 状态可通过 `GET /api/target-candidates/{record_id}/public-web-promotions` 或 detail 响应中的 `promotions` / `promotion_summary` 读取
    - Web Search 专用导出应调用 `POST /api/target-candidates/public-web-export`，默认 `mode=promoted_only`，只导出人工确认的 model-safe signals/evidence links/promotions/manifest，不包含 raw HTML/PDF/search payload
    - 目标候选人页的 Public Web export 数字表示当前选择/筛选范围内的 target-candidate 数量，不表示这些候选人都已有确认 Public Web 结果；`promoted_only` 包只会包含已人工确认的 Public Web signals
    - 如果本地前端提示 Public Web Search 接口不可用并且该 GET 返回 404，优先怀疑后端仍是旧进程；重启当前分支的 backend/worker 后再排查契约
  - 前端仍可保留本地事件广播，只用于刷新 UI，不用于持久化
- 前端历史搜索记录现在也应以后端为准
  - Sidebar 列表统一读 `GET /api/frontend-history?limit=24`
  - 单条恢复仍读 `GET /api/frontend-history/{history_id}`
  - 删除历史统一走 `DELETE /api/frontend-history/{history_id}`
  - 浏览器 `localStorage` 只保留为本地 cache / optimistic UI，不再作为跨设备真相源
- Stage 1 之后的 LinkedIn 信息补全不应默认自动执行
  - 缺工作经历 / 教育经历的候选人，可在前端标记为 `needs_profile_completion`
  - 用户手动触发时，前端调用 `POST /api/jobs/{job_id}/profile-completion`
  - 后端会基于该 job 对应的 snapshot，只对指定 `candidate_ids` 跑受控 profile completion，并重写 materialization
- `GET /api/jobs/{job_id}/results` 若同时返回 `results` 和 `asset_population`
  - 前端结果看板默认应优先消费 `asset_population`
  - `results` 仍保留为排序结果与审计参考，不再默认作为用户主列表
- 候选人结果对象现在可稳定携带 `avatar_url / photo_url / media_url / primary_email`
  - 这些字段可能来自候选表、materialized candidate artifact，或 LinkedIn profile raw path 的二次解析
  - 前端应直接消费这些字段，不再自己从原始文本中推断头像或 Email

仓库内对应的共享类型资产：

- `contracts/frontend_api_contract.ts`
- `contracts/frontend_api_contract.schema.json`
- `contracts/frontend_api_adapter.ts`
- `contracts/frontend_react_hooks.example.tsx`
- `contracts/frontend_runtime_dashboard.example.tsx`

推荐用法：

- 前端 TypeScript 项目直接复用 `frontend_api_contract.ts`
- 若想直接获得 typed `fetch` 调用示例，可复用 `frontend_api_adapter.ts`
- 若前端是 React，可直接参考 `frontend_react_hooks.example.tsx`
  - 已示例 `useSourcingPlan / useReviewInstructionPreview / useStartWorkflow / useJobProgress / useJobResults / useWorkflowRun`
  - 也可直接复用 `getOrderedWorkflowStageSummaries / getWorkflowStageDisplayLabel`
- 后端或集成测试若需要做 response 校验，可按 JSON Schema 中的 `$defs` 引用：
  - `#/$defs/PlanResponse`
  - `#/$defs/ReviewInstructionCompileResponse`
  - `#/$defs/ReviewPlanApplyResponse`
  - `#/$defs/WorkflowStartResponse`
  - `#/$defs/QueryDispatchListResponse`
  - `#/$defs/JobProgressResponse`
  - `#/$defs/JobResultsResponse`
  - `#/$defs/RetrievalJobResponse`
  - `#/$defs/TargetCandidatePublicWebSearchState`
  - `#/$defs/TargetCandidatePublicWebStartResponse`
  - `#/$defs/TargetCandidatePublicWebDetailResponse`
  - `#/$defs/TargetCandidatePublicWebPromotionResponse`
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
import {
  getOrderedWorkflowStageSummaries,
  getWorkflowStageDisplayLabel,
  useSourcingPlan,
  useWorkflowRun,
} from "../contracts/frontend_react_hooks.example";

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

  const stageSummaries = getOrderedWorkflowStageSummaries(
    workflow.results.data ?? workflow.progress.data,
  );

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
      <ul>
        {stageSummaries.map((item) => (
          <li key={item.stage}>
            {getWorkflowStageDisplayLabel(item.stage)}: {item.status ?? "unknown"}
          </li>
        ))}
      </ul>
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
- 前端只能消费 API contract，不直接读取 runtime 文件
  - 不要扫描 `runtime/company_assets/*`
  - 不要读取 `runtime/jobs/*`
  - 不要将 snapshot 文件路径当成前端主数据源
- demo / prototype UI 也不应再默认依赖 `public/tml/*` 这类静态 JSON 作为主链路
  - 静态 JSON 只适合视觉开发或离线演示
  - 一旦接真实后端，应切到 API contract，并把静态资产读取退回成 fallback/debug 能力

### 1.1 Runtime 边界（强约束）

前端阶段卡片、漏斗、结果摘要统一读取：

- `GET /api/jobs/{job_id}/progress`
- `GET /api/jobs/{job_id}/results`
- `workflow_stage_summaries`

不要依赖 snapshot 内部文件（例如 `workflow_stage_summaries/*.json`）做页面逻辑。
这些文件只用于后端审计/排障，不保证前端兼容性。

历史记录列表也属于同一边界：

- 读取 `GET /api/frontend-history`
- 恢复 `GET /api/frontend-history/{history_id}`
- 删除 `DELETE /api/frontend-history/{history_id}`

不要把浏览器本地 `localStorage` 当成跨设备共享的历史记录数据库。

### 1.2 Dry-Run / Explain 边界

需要在“开始 workflow 前”展示的内容，统一读取：

- `POST /api/workflows/explain`

推荐前端只使用这几个稳定层：

- `request_preview`
- `organization_execution_profile`
- `asset_reuse_plan`
- `dispatch_preview`
- `lane_preview`
- `generation_watermarks`
- `cloud_asset_operations`

不要再用前端自己的 query heuristic 去猜：

- 目标公司
- 组织规模
- 会不会复用 baseline
- current/former lane 的执行方式

这些都应该以后端 explain 为准。

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

在真正 `POST /api/workflows` 前，前端可以先调用 dry-run explain：

- `POST /api/workflows/explain`

前端应消费：

- `status`
- `request_preview`
- `ingress_normalization`
- `planning.plan`
- `planning.plan_review_gate`
- `organization_execution_profile`
- `asset_reuse_plan`
- `dispatch_matching_normalization`
- `dispatch_preview`
- `lane_preview`
- `timings_ms`

推荐做法：

- plan/review 页在用户点击执行前，先调用一次 explain，用它展示“这次请求会走 full roster、scoped search，还是 baseline reuse / delta from snapshot”
- 如果 `dispatch_preview.strategy == reuse_snapshot` 或 `delta_from_snapshot`，前端优先展示 `dispatch_preview.request_family_match_explanation`
- 如果 `status == needs_plan_review`，前端用 `planning.plan_review_gate` 渲染 review 阶段，而不是直接创建 workflow job

注意：

- explain 是 dry-run，不创建 job，不写 plan review session
- explain 返回的是“如果现在提交，会发生什么”，适合作为执行前的解释层与排障层
- 前端不要把 explain 当成真实执行结果缓存到 job 详情页；真实状态仍以 `POST /api/workflows` 和 `GET /api/jobs/{job_id}/progress` 为准

接口：

- `POST /api/workflows`

前端应消费：

- `job_id`
- `status`
- `stage`
- `plan`
- `plan_review_session`
- `intent_rewrite`
- `dispatch`
- `dispatch.request_family_match_explanation`

推荐做法：

- workflow 创建成功后，把 `job_id` 和当前缓存的 `intent_rewrite` 绑定
- 若 `POST /api/workflows` 自身也返回了 `intent_rewrite`，以后者为准
- 若 `status` 为 `joined_existing_job` 或 `reused_completed_job`，直接复用 `dispatch.matched_job_id` 对应结果，不要再提示“新任务已创建”
- 若 `dispatch.strategy == reuse_snapshot`，前端可直接展示 `dispatch.request_family_match_explanation`，告诉用户这是 exact/family 命中还是同公司 snapshot 复用

可选的请求控制字段（都在 `POST /api/workflows` payload 顶层）：

- `requester_id` / `tenant_id`
  - 用于限定复用作用域（同租户 / 同用户）
- `idempotency_key`
  - 强约束幂等键，优先于 request signature 去重
- `query_dispatch_scope`
  - `global | tenant | requester`（缺省自动推断）
- `allow_join_inflight`
  - 是否允许加入在途任务（默认 `true`）
- `allow_result_reuse`
  - 是否允许复用已完成结果（默认 `true`）

请求归一语义：

- `ingress_normalization`
  - 请求入口的 LLM/rules 混合归一，负责把原始 query 提炼成结构化 request。
- `dispatch_matching_normalization`
  - dispatch 阶段的 deterministic 归一，负责 request signature / family score / snapshot reuse。
  - 这一层不重新调用模型。

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
- `workflow_stage_summaries`

前端不应期待：

- 完整 `intent_rewrite`

推荐做法：

- progress 页直接复用之前缓存的 `intent_rewrite`
- progress 页直接读取 `workflow_stage_summaries` 渲染阶段卡片/漏斗
- 若用户刷新页面且本地状态丢失，可再调用 `GET /api/jobs/{job_id}/results`

当前阶段顺序固定为：

- `linkedin_stage_1`
- `stage_1_preview`
- `public_web_stage_2`
- `stage_2_final`

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
- `workflow_stage_summaries`

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
- `workflow_stage_summaries`
  - 阶段完成态、阶段摘要、阶段文件路径
  - 前端应该优先用这个字段，而不是自己去读 snapshot 文件

### 4.6 Query Dispatch Audit

接口：

- `GET /api/query-dispatches`
- `POST /api/query-dispatches/list`

用途：

- 查询最近的 query 分发决策（新建 / 加入在途 / 复用完成）
- 支持按 `target_company / requester_id / tenant_id / limit` 过滤

推荐做法：

- 前端调试页或运营后台使用 `GET /api/query-dispatches` 展示去重与复用命中情况
- 如果上游网关不方便拼 query string，可使用 `POST /api/query-dispatches/list` 传同名 JSON 字段

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
  workflowStageSummaries?: {
    directory?: string
    stage_order: string[]
    summaries: Record<string, Record<string, unknown>>
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
   - 刷新 `workflowStageSummaries`
5. `GET /api/jobs/{job_id}/results`
   - 若需要，以结果页返回的 `intentRewrite` 做最终校正
   - 用结果页返回的 `workflowStageSummaries` 做最终阶段摘要校正

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
- `dispatch`

### `GET /api/query-dispatches`

最重要的稳定字段：

- `query_dispatches[]`
- `query_dispatches[].strategy`
- `query_dispatches[].status`
- `query_dispatches[].source_job_id`
- `query_dispatches[].created_job_id`
- `query_dispatches[].request_family_match_explanation`

### `GET /api/jobs/{job_id}/progress`

最重要的稳定字段：

- `status`
- `stage`
- `elapsed_seconds`
- `blocked_task`
- `current_message`
- `progress`
- `workflow_stage_summaries`

### `GET /api/jobs/{job_id}/results`

最重要的稳定字段：

- `job`
- `results`
- `workflow_stage_summaries`
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
