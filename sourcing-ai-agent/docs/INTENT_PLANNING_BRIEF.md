# Intent Planning Brief

这份文档定义 `plan` 阶段第一段产品原生输出的目标形态。它不是 workflow 内部执行 plan 的替代品，而是给用户看的第一层解释卡片，用来回答：

- 系统识别到了什么请求
- 系统准备交付什么结果
- 系统默认会怎么执行
- 哪些地方还需要用户确认

## 目的

- 让意图识别结果可读、可比较、可回放
- 当 Claude / Qwen 的理解偏差时，有一套稳定字段可以修 prompt 或修规则
- 让后续 Web / chat 前端直接渲染这一层，而不是临时拼接自然语言

## 标准结构

`SourcingPlan.intent_brief` 当前固定输出以下四段：

- `identified_request`
  - 用户请求被系统识别成什么
- `target_output`
  - 本次 workflow 预期交付什么结果
- `default_execution_strategy`
  - 默认的执行策略、source 顺序和过滤方式
- `review_focus`
  - 执行前仍需用户确认的风险点或开放问题

与这张卡片配套的顶层响应字段是 `intent_rewrite`。

- 它不是 `intent_brief` 的替代品
- 它负责把“用户原话里命中的自然语言简称改写”显式结构化暴露出来
- 前端可以把它渲染成一张更底层的 request normalization / operator rewrite 卡片

当前 `intent_rewrite` 主要出现在：

- `plan` 响应
- `review-plan --preview` / `compile-instruction` 响应
- `show-job` / `GET /api/jobs/{job_id}/results`
- retrieval refinement compile/apply 响应

标准结构：

```json
{
  "intent_rewrite": {
    "request": {
      "matched": true,
      "summary": "自然语言简称改写：华人 / 泛华人简称 -> 中国大陆 / 港澳台 / 新加坡公开学习或工作经历 / 中文 / 双语 outreach 适配",
      "rewrite": {
        "rewrite_id": "greater_china_outreach",
        "keywords": [
          "Greater China experience",
          "Chinese bilingual outreach"
        ],
        "targeting_terms": [
          "中国大陆 / 港澳台 / 新加坡公开学习或工作经历",
          "中文 / 双语 outreach 适配"
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

解释：

- `request`
  - 对应原始 `raw_user_request` 或 `query`
- `instruction`
  - 只在 `review-plan` / `refine-results` 这类存在自然语言 operator instruction 的场景出现
- `matched`
  - 是否命中了当前 rewrite 规则
- `summary`
  - 给前端直接展示的人类可读短句
- `rewrite`
  - 给前端、日志、回放和后续编排使用的结构化 payload

## 输出规则

- 每段都是简短 bullet string 数组，方便 CLI / API / Web 直接渲染
- 内容优先具体、可执行，不写抽象口号
- 若模型输出不稳定，必须回退到 deterministic brief
- `intent_summary` 仍保留，但它只是短摘要；真正可校正的产品层结构以 `intent_brief` 为准
- `intent_rewrite` 应被视为 `intent_brief` 的配套结构化解释层：
  - `intent_brief` 负责“系统打算怎么做”
  - `intent_rewrite` 负责“系统把用户原话改写成了什么”

## Humans& 示例

目标请求：

`我想了解 Humans& 的 Coding 方向的 Researcher。`

理想输出示意：

### 已识别请求

- 目标组织：Humans&
- 目标人群：Researcher
- 方向约束：Coding
- 任务类型：新组织端到端 sourcing test

### 当前理解的目标输出

- 找到与 Humans& 相关、方向偏 coding / code generation / coding agents / developer tools / software engineering / program synthesis 的研究型成员
- 优先返回高置信当前成员
- 若存在边界不清的人，进入 manual review，而不是静默丢弃

### 默认执行策略

- 先做 company identity resolve，确认正式组织身份与 LinkedIn company URL
- 判断是否已有可复用资产
- 若没有，则进入新组织 acquisition
- acquisition 后做多层 retrieval：structured filters、facet / role bucket、lexical / alias matching、semantic recall
- 输出结果，并显式带上 manual review items

### Review 关注点

- 是否允许在低成本公开网页验证不足时升级到高成本 LinkedIn provider
- 是否需要额外纳入 OpenReview、官方 blog/docs 之外的 source family

### 配套的 `intent_rewrite`

- `request.matched=false`
- 因为这条 query 没有命中 `华人 / 泛华人 / Chinese members` 这类简称改写规则
- 前端可以不显示 rewrite 卡片，或者折叠显示成 “No rewrite applied”

## 当前实现约束

- 当前 brief 由 `planning.py` 生成，并写入 `SourcingPlan.intent_brief`
- 默认走 deterministic brief，保证 plan 首屏稳定、低延迟、可回放
- `planning_mode=heuristic` 时，request / `intent_summary` / `intent_brief` / `search_strategy` 都走 deterministic 规划
- `planning_mode=model_assisted` 时，模型优先做 request normalization：
  - 识别目标组织
  - 识别 team / sub-org
  - 识别雇佣状态、方向关键词、组织关键词
  - 后续 `intent_brief` 仍走 deterministic 生成，保证结构稳定
- `intent_rewrite` 当前也是 deterministic-first：
  - 先看后端 rewrite 规则是否命中
  - 若命中，则把 rewrite 结果显式写进顶层响应
  - 这让前端不需要重新解析自然语言，也不需要依赖 prompt 文本猜系统做了什么
- 已预留 `draft_intent_brief(...)` 的模型增强接口；只有显式开启更激进的 planning mode 时才走模型直写 brief / search planning
- 这一层先服务 CLI / API，后续前端可以直接复用
