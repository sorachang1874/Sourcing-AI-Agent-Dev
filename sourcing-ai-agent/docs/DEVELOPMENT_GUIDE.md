# Development Guide

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档只覆盖工程实现原则。分支、PR、代码评审、AI 使用和协作流程统一以 monorepo 根目录 [../../CONTRIBUTING.md](../../CONTRIBUTING.md) 为准。

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

6. Git is not runtime storage
   GitHub 只同步代码、文档、示例配置和去敏后的方法论资产；runtime、secrets、live payload、profile raw assets 必须单独存储。

7. Root-cause first
   遇到 planner / frontend / results 层症状时，先追 upstream writer、shared contract、持久化字段与恢复路径；只有确认 writer contract 正确后，才在 reader/UI 层补展示逻辑。

8. No silent dual-track systems
   兼容路径必须有明确开关、banner、测试和退出计划。PG-only、runtime namespace isolation、provider cache isolation、default-off stage 这类 contract 不能因为“兼容旧逻辑”重新变成双默认。

9. Durable session memory
   长会话中的结论必须写进 `PROGRESS.md`、`docs/NEXT_TODO.md` 或 active session tracker。下一轮接手时先读 tracker，再执行命令。

## 当前推荐开发顺序

1. 先补 plan / acquisition / enrichment / retrieval 的缺口
2. 再做新的 live connector 验证
3. 最后再考虑 demo 前端

## 修改代码时要优先维护的对象

- Postgres control plane
  - `jobs / job_results / job_events / job_result_views`
  - `criteria_* / confidence_*`
  - `organization_* / acquisition_* / linkedin_profile_registry*`
- generation manifest / shard / page / backlog artifacts
- `asset_registry.json`
- manual review assets
- result diff / trace / worker state

## 本地 control-plane 默认约定

- 本地开发默认是 `Postgres-first`，不是磁盘 SQLite-first
- 如果仓库或其父目录存在 `.local-postgres/`，系统会自动发现并推导本地 DSN
- 默认 shadow db 是 `runtime/control_plane.shadow.db`
- `runtime/sourcing_agent.db` 不应再被视为 live authoritative store
- 判断当前会话到底解析到了哪套 control-plane 资源，统一运行：
  - `bash ./scripts/dev_doctor.sh`
  - 或 `PYTHONPATH=src "$(./scripts/dev_backend.sh --print-config 2>/dev/null | sed -n 's/^python_bin=//p' | head -n 1)" -m sourcing_agent.cli show-control-plane-runtime`
- 统一入口与排障说明见：
  - `docs/LOCAL_POSTGRES_CONTROL_PLANE.md`
- 在开始本地联调前，优先运行：
  - `make bootstrap-test-env`
  - `make dev-doctor`

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

## 对跨设备继续开发的要求

- 关键上下文必须沉淀到 `README / PROGRESS / retrospective / module docs`
- 不依赖聊天上下文保存关键决策
- 新机器接手时，优先恢复 secrets 和必要 runtime 子集，而不是试图让 Git 承担资产仓库角色
- 跨设备 durable asset storage 的设计与边界，统一参考 `docs/CROSS_DEVICE_SYNC.md`
