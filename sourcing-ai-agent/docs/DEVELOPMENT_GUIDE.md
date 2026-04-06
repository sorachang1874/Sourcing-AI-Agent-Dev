# Development Guide

## Core Engineering Rules

1. Raw-first  
   外部 API / 网页 / profile / search 返回必须先落盘，再做归一化、分析和合并。

2. Auditability  
   每条 workflow、每份 snapshot、每次 manual review、每次 criteria evolution 都应该能追溯。

3. Low-cost-first  
   先做 relationship check / web exploration / known-url enrichment，再考虑高成本 people search。

4. Unresolved retention  
   不能确认的人保留为 unresolved lead 或 manual review item，不静默删除。

5. Model-agnostic interfaces  
   LLM、embedding、rerank、search provider 都应通过抽象层接入，避免业务逻辑绑死某个 vendor。

## 当前推荐开发顺序

1. 先补 plan / acquisition / enrichment / retrieval 的缺口
2. 再做新的 live connector 验证
3. 最后再考虑 demo 前端

## 修改代码时要优先维护的对象

- `candidate_documents.json`
- SQLite `candidates / evidence / jobs / criteria_* / confidence_*`
- `asset_registry.json`
- manual review assets
- result diff / trace / worker state

## 对新增 source family 的要求

新增 source family 之前，应先补以下信息：

- 目标是什么
- 覆盖范围是什么
- stop condition 是什么
- 成本容忍度如何
- 只做标题/摘要级 surface capture，还是做深挖

## 对新 connector 的要求

- 定义输入/输出 schema
- raw payload 落盘
- cache / retry / rate-limit 策略
- failure mode 和 fallback
- 对模型上下文的安全裁剪策略

## 对 live test 的要求

- 先明确预算和 provider
- 先做最小可验证调用
- 参数与结果要写入 retrospective / progress
- 不允许只在对话里记经验，不落到 repo 文档
