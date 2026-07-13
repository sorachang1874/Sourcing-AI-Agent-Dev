# Frontend API Contract

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义 Web 层当前应该如何消费后端 API，尤其是：

- `plan -> review -> workflow -> progress -> results` 的标准接口顺序
- `intent_brief` 和 `intent_rewrite` 的职责分工
- 哪些响应适合做“语义解释层”
- 哪些响应只适合做“状态刷新层”

目标不是穷举所有内部字段，而是定义一套前端可稳定依赖的 contract。

## Track C C1a/C1b async transport contracts (2026-07-14)

C1a 只收口现有异步 transport/client 语义，不改变 schema、provider/model 或服务端 Plan submit 的 HTTP
`200` 行为。共享字段的 owner/source/consumer 约束如下：

| contract | owner / source of truth | allowed values / derivation | consumers | fail-closed state | migration / temporary-source deletion |
| --- | --- | --- | --- | --- | --- |
| workflow run status | 后端 workflow/progress owner；前端唯一 adapter 是 `frontend-demo/src/lib/workflowStatus.ts` | canonical `queued/running/blocked/completed/failed/cancelled`；`canceled/detached/superseded -> cancelled`；只有明确的 `completed + active background worker` 可投影为 effective `running` | `lib/api.ts`、`lib/sourcingBackend.ts`、`SearchPage`、Excel intake、timeline、dashboard cache | missing/unknown -> terminal `failed`；不得 fallback 为 `running` | C1a normal frontend path。别名仅在后端与 preflight 都证明只返回 canonical status 后删除；C1b 已同步 Python async-task authority 为 terminal-total |
| export `task_id` | export command owner 返回的 submit envelope | 非空、无 trim drift、无 `%`/separator/query/fragment/control/dot segment；客户端把值作为 opaque handle 保存，不从其他字段推导 | 两个 public export wrapper、status poll、artifact validator | 非法或缺失时 submit 后立即失败，不 poll/download | normal path；task retention/deletion 由 durable export owner 管理，客户端无 shadow ID |
| export `artifact.handle` | `GET /api/exports/{task_id}` 的 owner-supplied `artifact.handle` | 只接受 exact `/api/exports/${encodeURIComponent(task_id)}/artifact`；root-relative、同一 task、无 scheme/authority/query/fragment/dot/encoded separator | projection export 与 CRM Public Web export download | `succeeded` 但 handle 缺失或 near-miss 时不发 binary request；不拼 fallback URL，也不 fetch supplied alternate URL | C1a normal path；无兼容 source，artifact 过期/删除仍由 export owner 表达 terminal status |
| Plan submit bridge status | `plan_submit_contract.py` + `submit_plan_workflow`；当前 `POST /api/plan/submit` top-level response | server 固定 HTTP `200` + `pending`；history `plan_generation` 只走 `queued -> running -> completed|failed`；client 暂时接受 `pending|queued` | API route、`SearchPage` initial plan 与 revision flow、history recovery | submit owner 缺失时 HTTP `503` + `plan_submit_owner_unavailable` + `fallback_used=false`；绝不回退同步 `plan_workflow` | C1b characterized bridge；C1e durable 202 cutover、旧 client 窗口结束且 preflight 证明无 `pending` 后删除 |
| unlinked Plan history identity | API authentication state 是唯一 owner；`submit_plan_workflow` 只把 server provenance + exact `requester_id`/`tenant_id` 证明写入 history metadata | authenticated submit 覆盖 client identity/provenance；read/list/resubmit 必须同时匹配 `authenticated_request_state_v1`、requester 与 `user-<id>` tenant | Plan submit、frontend-history read/list/revision submit | authenticated mode 下 missing/partial/mismatch proof 一律 404 或从 list 排除；不得信任 body、姓名或 legacy metadata 推断 owner；旧 non-Plan unlinked row 可保持 read compatibility，但不能因此获得 Plan replacement authority；open mode 保持兼容 | C1b process-local bridge；C1c durable consumer identity 上线且 legacy unresolved inventory 清零后删除 metadata proof |

Lane ownership 仍在后端 `src/sourcing_agent/api.py::_request_priority_lane`：只有 `POST
/api/projections/export`、`POST /api/crm/records/public-web-export` 与 exact `GET /api/exports/{single-segment-id}`
是 light。binary `/artifact`、trailing/extra segment 和错误 method 必须保持 shared；前端不得从 URL prefix
重新推导 lane。唯一 transport-level 例外是既有 CORS `OPTIONS` preflight：所有 `OPTIONS` 都保持 light，
不表示对应 business method 被重新分类。

C1b additionally pins the legacy Plan execution boundary. `submit_plan_workflow` is the only serving owner allowed to
call `_queue_plan_hydration`; that queue owns the only `Thread(target=_run_plan_hydration)` creator, and the hydration
runner is the only serving caller of `plan_workflow`. `sourcing-agent plan` remains an explicitly classified CLI
one-shot helper, not a serving fallback. The source/AST preflight resolves callable aliases, `partial`/`getattr`,
thread targets, and executor targets; it fails if another queue, thread/executor hydration target, API compile caller,
or CLI callsite appears. Queue registration and owner retirement share one lock and owner token, so a late
same-signature consumer is either drained by the current owner or starts one successor. Same-history queued/running/
terminal publication is generation-guarded under that lock. This is deliberately bounded: the real compiler still
creates review and criteria rows before the post-compile generation fence. A deterministic test records those orphan
side effects, so D-C1-3/C1d compute/publish separation remains a blocker for durable cutover and milestone signoff.
The Python async-task adapter also rejects an empty artifact handle at construction and drops malformed succeeded
artifacts instead of publishing a blank handle.

补充约定：

- `request` 表示当前后端准备真正执行的 execution-aligned request，而不只是最初的归一化输入
- `request_preview` 应被视为 `request` 的可解释展示层，两者在 `keywords / organization_keywords / employment_statuses / intent_axes` 上应保持同一语义
- 如果 planning / acquisition strategy 扩展了执行关键词，例如把用户 query 补成 `Pre-train`、`Vision-language` 这类真实 shard / provider keyword，前端应直接信任返回的 `request` / `request_preview`，而不是自己从原始 query 再做一轮猜测
- `search_seed_queries` 不是通用的“真实 HarvestAPI search keyword”字段。它只在 scoped/directional profile-search、former keyword-only、large-org keyword probe、或显式 Stage 1 web seed fallback 中可以代表 provider-facing query。普通 `full_company_roster` 的 LinkedIn Stage 1 当前员工 lane 由 company-employees/company filters 驱动；former broad lane 由 `pastCompanies` + blank query 驱动。
- 计划页展示“检索关键词 / provider 参数”时，必须优先消费 `plan.acquisition_strategy.provider_execution_manifest.lanes[]`：
  - `provider_facing_query=true` 且 `query_texts` 非空，才可作为真实 provider keyword 展示
  - `provider=harvest_company_employees` 且 `query_texts=[]` 表示公司 roster API 无 search keyword
  - `provider=harvest_profile_search` 且 `past_companies` 非空、`query_texts=[]` 表示 broad former recall 由 past-company filter 驱动
  - 不得把历史 `Lovable Employee` / `Lovable LinkedIn Employee` 这类泛化 seed label 展示成真实调用参数
- `provider_execution_manifest` 是 Stage 1 实际 provider 参数的 canonical contract，不只属于计划页：
  - history recovery metadata、`/progress`、`/dashboard`、`/results`、`/candidates` 都应透传或投影同一个 manifest
  - 前端恢复历史、运行态调试、执行过程解释和结果页不得从 `search_seed_queries`、策略文案、历史缓存 label 重新推断 provider 参数
  - 如果只有 metadata 中存在 manifest，前端仍应消费它；如果 manifest 不存在，才允许展示“后端未返回实际 provider 参数”，而不是猜测
  - 普通用户计划页不展示完整 provider manifest；它只属于 Advanced Mode / developer diagnostic context，放在 Baseline snapshot 附近用于排障
  - 普通用户可见的策略解释应使用 `检索关键词`、`检索策略`、`目标公司`、`目标人群` 等产品语义字段，不应暴露 `current_companies/past_companies/searchQuery` 这类 provider 参数
- 前端接 hosted 后端时，默认不应再把 `force_fresh_run=true` 作为通用默认值
  - 当组织级 baseline 已经 `effective_ready`，这会直接绕开本地资产复用，重新触发高成本 provider
- `POST /api/workflows/explain` 应作为前端的 dry-run / operator explain 入口
  - 它现在会返回 `generation_watermarks`、`asset_reuse_plan`、`cloud_asset_operations`
  - 适合在真正 `start workflow` 前展示“这次会复用哪个 baseline、是否只补 delta、最近有没有云端 import/GC”
- `GET /api/runtime/progress` 应作为运维面板的聚合入口
  - 它现在会返回 `cloud_asset_operations` 和按公司过滤的 `company_asset`
  - 不需要前端自己再拼 runtime 文件或 registry
- Agent/Operation 调试面板必须消费后端的 typed operation/runtime contract，而不是从 command 名称猜测能力
  - `GET /api/operations/action-registry` 返回 `display_contract`、`allowed_workflow_command_contracts` / `default_workflow_command_contract`
  - `operation_action.display_contract` / `operation_run.display_contract` 由 `operation_runtime.ActionRegistry.display_contract_for` 生成，是 action/run 产品文案、类别和说明的唯一真源。Operation UI 可以展示 technical `action_type` / `operation_type` 作为 debug id，但标题/类别不得从 action type 字符串、operation type、owner module 或本地映射重新推导
  - `workflow_command_exposure_gate` / `workflow_command_exposure_status` 和 command-level `agent_exposure_gate` / `agent_exposure_status` 由 `operation_runtime.ActionRegistry.allowed_workflow_command_types` 生成，是 Agent/Operation UI 判断命令是否可由 action 规划的唯一 normal-path contract。Frontend 不得把 durable owner registry 或 `activity_spine_policy.agent_callable` 当作产品暴露列表
  - `workflow_command_control_summary` 由同一个 ActionRegistry 从 `allowed_workflow_command_contracts` 汇总，是 action-level running-control maturity/gap/category 的唯一摘要。Frontend/Agent UI 可以用它显示“可规划但运行中不可中断”的 gap，但不得重新遍历 command type 字符串或 owner 名称推断可取消性
  - `/operations` 对缺失的 display contract 必须 fail closed：显示 `Display contract missing` / `display_contract_missing`，并由 W10 preflight 捕获；不得 fallback 到 owner module、owner、operation type、action type 或 command type 来生成产品标题/类别
  - `GET /api/operations/actions`、`GET /api/operations/runs`、`GET /api/operations/runs/{operation_run_id}/provenance`、`POST /api/operations/actions/{action_id}/approve|reject`、`POST /api/operations/runs/{operation_run_id}/cancel|retry|resume|dispatch` 是 action queue / operation queue 的正常前端 API；前端不得直接写 `workflow_commands`、CRM、projection、Public Web、Excel 或 export 表来模拟状态变化
  - `/operations` 前端页面是任务审批与执行工作台。它可以列出待确认操作、确认/拒绝操作、列出执行任务、读取审计记录，并对 run 发起 dispatch/resume/retry/cancel；它不得调用 CRM、projection、Public Web、Excel、export 等 domain API 来补状态或模拟执行。默认产品文案应使用“待确认操作 / 执行队列 / 执行详情 / 执行证据”，不能把 `Operation Queue`、read model、owner module、command type 等内部合同词作为主页面说明。技术标识只能放在展开调试区。
  - `frontend-demo/src/lib/api.ts` 中供 `/operations` 使用的 Operation helper 也必须只调用 `/api/operations/...`。Activity/Attempt/Delta 详情只能通过只读 `/api/workflow/activities`、`/api/workflow/activity-attempts`、`/api/workflow/entity-deltas` runtime API 加载；这些 API 只做 provenance drill-down，不参与 run/command control，也不得调用 domain API
  - `GET /api/operations/runs/{operation_run_id}` 默认返回 `operation_run.status_summary`；`GET /api/operations/runs?include_status_summary=true` 可在列表中请求同一摘要。该摘要只从 `operation_runs`、`operation_events`、`workflow_commands` 汇总，命令详情继续通过 command `execution_summary` 引用 Activity spine，不从 domain 表补状态
  - Operation run 记录必须返回 `operation_run.control_state`，`POST /api/operations/runs/{operation_run_id}/cancel|retry|resume|dispatch` 响应顶层也必须返回同一 `control_state` 和 `display_contract`。`control_state` 由 `operation_runtime.operation_run_control_state` 生成，是 dispatch/resume/retry/cancel 按钮可用性的唯一真源。前端不得用本地 terminal status 集合或 status string heuristic 推导 OperationRun 控制能力；`retry` 返回 child OperationRun 时，UI 应选择返回的 child run 而不是继续停留在 terminal parent run
  - `GET /api/workflow/command-registry`、`GET /api/workflow/commands`、`GET /api/workflow/commands/{command_id}` 返回同一套 `display_contract`、`control_policy` 和 `activity_spine_policy`；command row/detail 还必须返回 status-specific `control_state`
  - `GET /api/workflow/command-registry` 是 durable owner/control registry，不是 Agent action allowlist。每个 command entry 以及 `GET /api/workflow/commands` / `GET /api/workflow/commands/{command_id}` 返回的每条 command row 都必须暴露 `agent_exposure_gate` / `agent_exposure_status`；`not_action_registry_allowlisted` 表示只能通过内部 owner/recovery 或显式 action adapter 使用，前端不得把它显示为可由 Agent 直接规划的 action command
  - `display_contract` 由 `durable_runtime.workflow_command_display_contract` 生成，是 command 产品文案、类别和说明的唯一真源。Operation UI 可以展示 technical `command_type` 作为 debug id，但标题/类别不得从 command type 字符串、owner 名、stage id 或本地映射重新推导
  - `GET /api/workflow/commands/{command_id}` 默认返回 `workflow_command.execution_summary`；`GET /api/workflow/commands?include_execution_summary=true` 可在列表中请求同一摘要。该摘要只从 `workflow_activity_runs`、`workflow_activity_attempts`、`workflow_entity_deltas` 读取，`fallback_status=fail_closed`，不修复、不执行、不读取 domain 表补语义
  - `/operations` 对每个 command 的状态文案必须使用 `workflow_command.execution_summary.activity_status_counts` / `attempt_status_counts` / `entity_delta_status_counts` / `entity_delta_kind_counts`、`latest_effect_status`、`latest_activity`、`latest_attempt`、`latest_entity_delta` 和 `sample_truncated`。页面不得从 command type 字符串、时间戳顺序、CRM/Public Web/projection/export/Excel domain rows 重新推导 command 是否成功、失败、无效果或可继续
  - `POST /api/workflow/commands/{command_id}/cancel|retry|resume` 是唯一正常命令控制入口；按钮可用性必须来自 command row / 响应顶层 `control_state.allowed_actions` / `can_cancel` / `can_retry` / `can_resume`，Activity evidence 要求必须来自响应顶层 `activity_spine_policy`；响应中的 `workflow_command` 也携带同一套 policy/state 供详情展示
  - `/operations` command-level `取消步骤` / `继续步骤` / `重试步骤` 按钮必须调用 `/api/workflow/commands/{command_id}/cancel|resume|retry`，并且只根据 `workflow_command.control_state.allowed_actions` 和 `disabled_reasons` 渲染可用性。前端不得用 command status string、本地 terminal set、owner 名称或 Activity rows 自己推断 command control 能力
  - `继续步骤` 不是通用修复按钮。当前 generic resume 只适用于 `retry_wait`；`claimed` / `running` resume 必须由后端 `control_policy.running_resume_*` 明确支持，否则前端必须展示 `running_command_requires_owner_specific_resume` / `running_resume_blocked_reason`，不得自行重排队或写入 owner 表
  - command control 响应若返回 `invalid` / `not_found` / 非预期 status，前端必须展示后端 `reason` / `command_status`，不得把失败当作成功刷新吞掉
  - `control_policy.running_cancel_upgrade_requirements` 是 running command 尚不能取消时的 owner 升级条件；前端不得把该字段当成可执行步骤，也不得在字段非空时显示 running cancel 为可用
  - `display_contract` 是命令展示文案的唯一真源；`control_policy` 是 cancel/retry/resume 能力规则的唯一真源，且必须暴露 `running_control_category` / `running_control_categories`、`running_control_maturity`、`running_control_gap_status` 和 `running_control_surface`，前端不得从 command type 或 blocked reason 反推 provider/domain/orchestration 分类或 running-control 成熟度；`control_state` 由 `durable_runtime.workflow_command_control_state` 生成，是当前 status 下按钮可用性的唯一真源；`activity_spine_policy` 是命令是否必须写 `workflow_activity_runs` / `workflow_activity_attempts` / `workflow_entity_deltas` 的唯一真源
  - 正常 command registry 不应出现 `activity_spine_policy.requirement="legacy_internal_pending_activity_spine"`；若迁移错误重新引入，前端不得展示或执行该 command 作为正常 Agent action
  - `GET /api/workflow/activities`、`/activity-attempts`、`/entity-deltas`、`/discovery-lanes` 是只读 provenance/debug surface；每条 activity、attempt、entity-delta、lane row 都必须暴露 `module_state_mutated=false` 和只读 `mutation_contract`，行内 `control_target` 指回所属 command，并携带 command-owned `display_contract`、`control_policy`、`control_state`、`activity_spine_policy` 和 `fallback_status=fail_closed`；前端所有 retry/cancel/resume 都必须打 command API，不得从 activity、attempt、delta、lane 字段推导展示文案或控制能力，也不得修改 activity、attempt、delta、lane、registry、projection、CRM 或 Public Web 表
- `GET /api/jobs/{job_id}/trace`、`GET /api/jobs/{job_id}/workers` 和 `GET /api/jobs/{job_id}/scheduler` 只作为诊断入口
  - 公共读路径必须以持久化 worker 记录为真源，不能因为 live runtime 连接失效就把页面打成 500
  - 如果 live runtime 不可用，接口仍应返回已持久化的 worker/scheduler 视图，而不是要求浏览器重试 worker DB
- 本地前端直连 hosted backend 时，默认使用 `http://127.0.0.1:4173 -> http://127.0.0.1:8765`
  - 后端会为 `127.0.0.1:4173` / `localhost:4173` 返回 CORS 响应头
  - 若前端运行在其他域名或端口，运维应设置 `SOURCING_API_ALLOWED_ORIGINS`
- 前端的“人工审核状态”和“目标候选人 CRM”现在已经切到后端 API + Postgres-backed control plane
  - 不再以浏览器 `localStorage` 作为真相源
  - 同一个 `job_id` 的审核状态，应通过 `GET/POST /api/candidate-review-registry` 读写
  - 跨 workflow 的目标候选人池，应通过 `GET/POST/PATCH /api/crm/records` 读写
  - 从本地资产公司页进入目标候选人时，前端必须把 `collection` 上下文作为 `source_collection_id` 传给 `GET /api/crm/records`；CRM API 是 collection-scoped 列表的 owner，前端不得拉取全局 CRM 列表后自己按公司推断。
  - 从 projection 添加目标候选人时，前端必须传 `projection_id + candidate_identity_key + expected_membership_revision` 到 `POST /api/crm/records`，由 `CRMWriter` 在同一 revision-fenced PG UoW 中复验 selection、去重并记录 provenance。projection id 或 displayed revision 缺失时按钮必须禁用，不能由服务端绑定“当前版”。
  - CRM follow-up task 只读列表应通过 `GET /api/crm/tasks` 或 `GET /api/crm/records/{crm_record_id}/tasks` 获取；前端不得从 `crm_events` payload 自行拼任务状态。响应必须标记 `read_contract.source=crm_tasks`、`read_contract.audit_source=crm_events`、`fallback_used=false`。
  - 将某个已完成 workflow 的候选人批量导入目标池仍属于 legacy/migration/import surface；正常 projection 页面应通过 CRM add-from-projection，而不是 job-bound target write
  - 导出普通候选人 CSV/profile bundle，应通过 `POST /api/projections/export`，并传入 `projection_id + expected_membership_revision`；目标候选人页面只有在每条记录都带完整 atomic `last_source_selection` tuple（projection/revision/source `N`/candidate/person）且所选记录属于同一 projection revision 时才能调用 canonical export。
  - Candidate page、dashboard 和 mutation control 必须共享 canonical `projection_id + row_publication_revision`。旧 revision 的分页响应即使晚到也必须丢弃；`forceRefresh` 必须创建新的 request generation，旧 in-flight promise 不得满足强制刷新或回填 cache。
  - `POST /api/target-candidates/export` 已退役为 migration/test-only 兼容入口，默认返回 `410`。前端正常路径不得调用它。
    - 目标候选人 Public Web Search 应通过 `POST /api/crm/records/public-web-search` 触发，通过 `POST /api/crm/records/public-web-search/poll` 查询 batch/run 状态；请求体使用 `crm_record_ids`
    - POST 只做幂等排队，不在请求线程里跑 DataForSEO/fetch/LLM
    - 返回的 per-candidate run 是事实来源；batch 只做多选操作的聚合状态
    - 前端应按 `record_id` 将最新 run 映射到候选人卡片，稳定消费 `status / phase / summary / query_manifest / search_checkpoint / analysis_checkpoint / created_at / updated_at`。`created_at` / owner planning time 是 current-run 选择语义，`updated_at` 只表示该 run 的进度刷新时间；旧 run 的晚到 progress update 不能让旧 run 重新成为当前 run。
    - CRM Public Web run 卡片的阶段命令行和运行控制必须由后端 owner 物化：`phase_command_display_line` 是阶段命令展示文案唯一真源，`run_control_state.allowed_actions` 是取消/重试按钮唯一真源，`run_display_contract` 是 run 产品展示合同。`phase_command_display_line` 必须使用产品化阶段名，不得透传 command registry label、command type、`CRM Public Web` 内部 owner 名、`materialize/signals/model_safe` 等实现术语。前端不得从 `phase_commands.current_command.command_type`、command `status`、run terminal status set、本地 command type label map 或本地 retry/cancel status set 推导产品文案或控制能力；字段缺失时必须 fail closed，不展示对应命令行或按钮。
    - 卡片上的 Public Web progress 文案应来自 `summary.phase_metrics` / batch `summary.phase_metrics`，用于区分 remote search pending、document fetch、AI adjudication、analysis、signal materialization；不要从卡片本地状态猜测阶段
    - `summary.primary_links`、`summary.entry_link_count` 和 `summary.fetched_document_count` 可作为卡片级紧凑展示；卡片上用户可见的邮箱候选计数必须来自 `summary.phase_metrics.email_signal_materialized_count`，其 owner 是 `crm.public_web.signals.materialize` 成功写入的 latest-run `person_public_web_signals` 行。历史 terminal run 若缺少这些 materialized metrics，后端 `poll` / `detail` 读路径必须从 `person_public_web_signals` 按 `latest_run.run_id + crm_record_id` 补齐 `signal_materialized_count` / `email_signal_materialized_count` / `profile_link_signal_materialized_count`，且不得从 raw summary 字段推导。`summary.email_candidate_count` / `summary.promotion_recommended_email_count` 只作为 raw/adjudication 诊断字段，不表示可审核、可导出、CRM-ready 邮箱，也不得作为卡片主指标或 detail 空状态的兜底。
    - `completed_with_errors` 不等同于 provider 执行失败。常见含义是 run 已完成并物化了公开信息 signals，但没有达到自动确认 `primary_links` 的质量阈值或需要人工复核；前端文案应偏向“已完成，需复核”，并提示同参数重试通常不会明显增量
    - 若 terminal run/export 没有可展示信号，前端应展示后端返回的 `export_record_status` / `export_skip_reason` 或 phase guardrail，而不是把空 zip/空字段当作成功结果
    - `record_id` 是 CRM record id；前端调用 path-style resource API 时必须使用 `encodeURIComponent(record_id)`，后端路由负责解码。`/profile`、`/public-web-search`、`/public-web-promotions` 必须对同一个 encoded `crm_record_id` 返回一致的候选人语义。
    - 目标候选人 Public Web detail 应通过 `GET /api/crm/records/{crm_record_id}/public-web-search` 获取，响应包含 `latest_run`、`person_asset`、`signals`、`email_candidates`、`profile_links`、`grouped_signals` 和 `evidence_links`。Public Web detail UI 不得通过 `/profile` 嵌套字段作为 detail 读源；`/profile` 只能作为 composed profile display owner。
    - Path-style CRM Public Web resource API 的 owner scope 由 `crm_record_id -> crm_records.workspace_id` 解析；前端不得假设所有 path reads/promotions 都属于 `workspace_id=default`，后端也不得用 default workspace filter 先过滤 path record。Body-style batch/action API（start/poll/cancel/retry/export）必须显式消费请求体 `workspace_id`，并在 mutation 前校验每个 explicit `run_id` 的 `run.workspace_id` 和所属 CRM record workspace 都与请求 workspace 一致；不一致必须 fail-closed，不能按 `run_id` 直接修改远端/provider-backed run。前端 `TargetCandidateRecord` / Public Web run adapter 必须保留后端 `workspace_id` 为 `workspaceId`，所有 body-style Public Web mutation/export/poll 都必须传该 workspace；跨 workspace 多选必须在前端 fail-closed，不能静默落到 `default`。
    - Public Web detail cache 必须区分长期人工确认资产和 latest-run 待审核信号。当 retry/force-refresh/cancel 切换 latest run 时，旧 run signal 只能作为审计历史，不能继续渲染为可审核信号；前端必须让本次待审核候选 fail-closed，且只允许确认/导出 `signal.run_id == latest_run.run_id` 的 signal。但前端不得因为 latest run mismatch、expected latest run 为空、latest run 非终态或 latest run 没有新 signal 而清空整个 detail cache，因为这会隐藏 durable manual promotion / PersonAssertion。`latest_run` 的唯一 owner 是 `crm_public_web_runs` 中匹配当前 `crm_record_id` + `workspace_id` 的 run；`person_public_web_asset.latest_run_id` 只能作为资产摘要/provenance，不能在 CRM detail/export 中 fallback 成 record-owned latest run。后端读取 signals 必须同时按 `run_id` 和 `record_id` 过滤。`POST /api/crm/records/{crm_record_id}/public-web-promotions` 必须在 CRM Public Web owner 内执行同一 latest-run guard；promotion owner 必须先确认 signal 所属 run 的 `crm_record_id` 和 `workspace_id` 都匹配当前 CRM record，且当前 record/workspace 存在非空 latest run 并等于 `signal.run_id`。若不满足，后端必须返回 `status=invalid` / `reason=public_web_signal_not_latest_run`，且不得写 promotion、PersonAssertion 或 exportable contact/link state。
    - detail 响应只返回 first-class `person_public_web_signals` 的 model-safe summaries/evidence links；不返回 raw HTML/PDF/search payload、`search_checkpoint` 或 raw document paths
    - 目标候选人 Public Web detail UI 必须把三类内容分开：`email_candidates` 是可审核联系方式候选，`profile_links` 是可审核公开主页候选，`evidence_links` 是只读证据/审计来源。`evidence_links` 不应渲染 promotion/export 状态控件；用户应在可审核信号区通过统一状态控件选择 `待复核`、`已确认并导出`、`已排除`。v1 不支持把已人工确认/排除的 signal 回退为待复核，因此 `待复核` 只能作为未人工判断信号的当前状态/可选状态；后续若新增 reset，必须通过 CRM Public Web owner 写审计事件、promotion void/reset 记录和 assertion/export 影响。
    - detail UI 不应重复卡片级运行控制或 provider progress。`entry_link_count`、`fetched_document_count`、provider retry/cancel、LinkedIn 打开入口属于卡片级紧凑运行视图；drawer 只负责审核/解释可导出信号。不要展示没有 owner/公式的“资料完整度”这类合成分数。
    - 前端触发 provider/model 成本或会切换 latest run 的操作必须有明确确认。首次多选 Public Web Search 与单人 retry 都应提示可能触发真实 provider/model 调用，并说明旧结果保留为审计记录。
    - detail/export 中的 run summary 和 person asset summary 会经过 model-safe sanitizer；`artifact_root`、`document_fetch_payload_path`、`adjudication_payload_path`、raw path/payload 等内部字段不属于前端合同
    - detail 中的 profile/public link signal 会携带 `link_shape_warnings` 和 `clean_profile_link`
      - `x_link_not_profile`、`substack_link_not_profile_or_publication`、`github_repository_or_deep_link_not_profile`、`scholar_link_not_profile` 必须作为 UI warning 展示
      - 即使 identity label 是 `likely_same_person`，`clean_profile_link=false` 的链接也只能作为 evidence/review signal，不应渲染成 clean profile 或 primary link
    - Public Web email candidates 可以展示为候选联系方式，但不能直接写入 CRM current-state contact 字段；提升为 primary email 必须通过 `POST /api/crm/records/{crm_record_id}/public-web-promotions` 写 promotion + `PersonAssertion` + CRM event
    - Public Web link/email promotion 状态可通过 `GET /api/crm/records/{crm_record_id}/public-web-promotions` 或 detail 响应中的 `promotions` / `promotion_summary` 读取
    - promotion API 默认只允许 `publishable=true` 且 clean URL-shape 的 signal；如果用户明确人工覆盖 non-publishable / dirty URL-shape signal，前端必须传 `allow_unpublishable=true` 和非空 `override_reason`
      - 无覆盖理由时后端返回 `override_reason_required` 和原始 `validation_reason`
      - hard validation 仍不可覆盖，例如无效 email candidate 不应写入 promotion
      - detail/promotions payload 会返回 `promotion_override_reason`、`promotion_override_validation_reason`、`promotion_requires_manual_override`，promotion row 会返回 `override_reason`、`override_validation_reason`、`requires_manual_override`
    - Web Search 专用导出应调用 `POST /api/crm/records/public-web-export`
	      - 默认 `mode=promoted_only`，只导出人工确认的 model-safe signals/evidence links/promotions/manifest，不包含 raw HTML/PDF/search payload
	      - 前端正常导出按钮必须默认 `promoted_only`；`mode=promoted_and_publishable` 只能通过显式导出范围控件选择，并且在导出前提示用户它会额外包含 AI 判定 `publishable=true` 但未人工确认的 signals，在 signal CSV 中标记 `ai_publishable_unpromoted`
	      - 目标候选人 composed profile 的 `export_readiness.default_public_web_export_mode` 也必须是 `promoted_only`。`export_readiness.ready` / `exportable_signal_count` 只描述默认导出模式下的人工确认 signals 或已确认 primary email；AI 判定可发布但未人工确认的候选只能进入 `ai_publishable_unconfirmed_signal_count`，并且只有用户显式选择 `promoted_and_publishable` 后才可导出。
	      - CRM Public Web export artifact reuse 必须绑定后端生成的 `export_input_watermark_hash`。水印 owner 是 `crm_public_web_exporter`，source of truth 是当前 export ZIP 会写入的 model-safe input snapshot：CRM record export fields、latest CRM Public Web run detail、run phase-command summary、person Public Web asset summary、export-mode-filtered `person_public_web_signals` / evidence links、`crm_public_web_promotions`、以及对应 `PersonAssertion` 状态；同一 record/mode 在用户 promotion/rejection、latest run、phase command、person asset 或 assertion 状态变化后必须生成新的 command/artifact，不能复用旧 ZIP。owner 在 replay 已成功 artifact 和 claim queued command 后执行 ZIP 副作用前都必须重算校验 watermark；过期 command 必须 fail-closed，不能发布 artifact。ZIP 写入前必须把每个 record 的 export input snapshot materialize 成有序集合；若任何 record 缺 snapshot，owner 必须 fail-closed，ZIP loop 不允许 fallback 重新读取 detail、asset、promotion、assertion 或 phase-command 源。
      - `POST /api/crm/records/public-web-export` 只有在 owner 返回 `status=ok` 且存在 ZIP body 时才可返回二进制下载；任何 `failed` / stale watermark / missing artifact / owner failure 都必须返回 JSON 非 2xx/409-style 错误，前端不得把空 ZIP 或 `download.bin` 当作成功导出。
    - 目标候选人页的 Public Web export 数字表示当前选择/筛选范围内的 target-candidate 数量，不表示这些候选人都已有确认 Public Web 结果；`promoted_only` 包只会包含已人工确认的 Public Web signals
    - 目标候选人卡片的简介行必须使用固定三行高度的可滚动容器，避免长 headline 使卡片网格错位。Public Web 状态区同样必须拆成固定高度的三行 progress 容器和固定高度的公开主页链接占位容器；长复核建议必须收进邻近的 `?` 帮助说明，不能作为卡片底部独立段落撑高单个候选人卡片。
    - 目标候选人卡片的跟进状态只属于候选人 CRM 元数据区；Public Web 操作区不应重复渲染 `待沟通` 这类 follow-up chip。卡片操作顺序应保持 `打开 LinkedIn`、取消/重试本次搜索、`查看公开信息详情`。
	    - `/api/target-candidates/public-web...` 已永久退役并返回 `410` + canonical CRM endpoint pointer；旧 `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS` env 只保留为历史上下文，不能重新启用。前端正常路径不得调用。`contracts/frontend_api_adapter.ts` 仍保留 target-candidate 命名的兼容方法，但这些方法必须映射到 `/api/crm/records/...` canonical CRM endpoints，并把 `recordIds` / `record_ids` 规范化为 `crm_record_ids`。CRM 响应必须标记 `public_web_storage_owner=crm_public_web_v1` 和 `public_web_execution_backend=crm_public_web_v1`；如果正常 CRM Public Web action 暴露 `target_candidate_public_web_v1` backend，应视为阻断性 Contract 回归。
	    - CRM Public Web batch/run 是 workspace-owned contract。`batch_id` / `run_id` 不是跨 workspace 全局读写入口；所有 body-style poll/cancel/retry/export/start payload 都必须携带 `workspace_id`，后端 storage/orchestrator/command owner 必须用同一 workspace scope 查询 batch-owned runs。若 batch/run 属于其他 workspace，API 必须返回 `public_web_batch_workspace_mismatch` 或 `public_web_run_workspace_mismatch`，不能返回空列表伪装成“没有候选”，也不能 fallback 到 `default` workspace。
		    - Public Web current-run / current-batch 选择由 planning/creation time owner，即 `crm_public_web_runs.created_at` / `crm_public_web_batches.created_at`，而不是 `updated_at`。`updated_at` 只表示 worker/progress freshness，不能让旧 run/batch 因补写 summary、promotion 或 artifact 而重新成为 current run/batch。前端缺失 `created_at` 时不得 fallback 到 `started_at` / `completed_at` / `updated_at`，只能 fail-closed 或保持后端返回顺序；当 action/poll 返回 `next` owner state 时，`next` 必须在 client merge 中排在旧缓存前，避免旧 valid-created row 击败当前但缺字段的 owner response。Latest-run reviewable candidates 和 durable confirmed promotions/assertions 是两层 UI 状态：本次重试没有候选时，已确认 Scholar/email/homepage 等资产仍必须可见、可导出、可审计。
	    - 如果本地前端提示 Public Web Search 接口不可用并且 CRM route 返回 404，优先确认传入的是 `crm_record_id` 而不是旧 target-candidate-only id；之后再检查后端是否为旧进程
  - 前端仍可保留本地事件广播，只用于刷新 UI，不用于持久化
- 前端历史搜索记录现在也应以后端为准
  - Sidebar 列表统一读 `GET /api/frontend-history?limit=24`
  - 单条恢复仍读 `GET /api/frontend-history/{history_id}`
  - 删除历史统一走 `DELETE /api/frontend-history/{history_id}`
  - 浏览器 `localStorage` 只保留为本地 cache / optimistic UI，不再作为跨设备真相源
- 执行过程页与候选人看板 tab 的自动切换必须是 workflow-scoped、一次性的交互
  - 当前 workflow 第一次出现 exact canonical visible membership 且候选行可分页时，可以自动打开一次候选人看板；`card_ready=0` 不是阻塞条件
  - 用户之后手动切回执行过程页，后续 running/results phase 变化、progress refresh、dashboard refresh 都不得再次强制跳转
  - 新 workflow / 新 job 才能重新 armed 这次自动打开
- Stage 1 之后的 LinkedIn 信息补全不应默认自动执行
  - 缺工作经历 / 教育经历的候选人，可在前端标记为 `needs_profile_completion`
  - 用户手动触发时，前端调用 `POST /api/jobs/{job_id}/profile-completion`
  - 后端会基于该 job 对应的 snapshot，只对指定 `candidate_ids` 跑受控 profile completion，并重写 materialization
- `GET /api/jobs/{job_id}/results` 若同时返回 `results` 和 `asset_population`
  - 前端结果看板默认应优先消费 `asset_population`
  - `results` 仍保留为排序结果与审计参考，不再默认作为用户主列表
- `boardRuntimeState.expectedCandidateCount` 是看板 canonical total
  - `publishedCandidateCount` / `rowHydrationTargetCount` 只表示行级水位或加载目标，不得反向抬高总量
  - 前端在 boardRuntimeState 存在时，不应再用 `publishedCandidateCount` 去修饰 `expectedCandidateCount`
  - `syncStatusText` 是主 `候选人同步 N/N` 的唯一真源；`N` 来自 exact canonical visible membership，不表达 profile/card richness
  - `displayReadyCandidateCount` 是 exact card-ready `C` 的兼容视图，只用于 `卡片详情 C/N` 与详情状态；它不决定主同步、分页总数、列表可渲染性或空状态
  - public `projection.membership_revision` 必须是 member-publication UoW 写入的非空 opaque equality token。summary/page/readiness 只可在 token 相等时合并；token 缺失/不等时丢弃混合快照、失效缓存并重新 resolve/read，不能按 token、`updated_at`、sequence 或 count 大小排序
  - `publishedCandidateCount` 是兼容旧 payload 的历史字段，不得再参与 `rowHydrationTargetCount`、分页、freshness 或其它主路径判断；若 canonical 字段缺失，视为后端 contract bug
- `boardRuntimeState.sync_note_lines` 是候选人同步文案的唯一真源
  - 前端不得把 intent match、stage summary、profile progress fallback 拼进同一行
  - 文案必须保持独立分母，例如 `候选人发现 597/597；新增 LinkedIn Profile 已取回 P/R；卡片详情已合入看板 C/597`；前端不得把 `P`、`C` 或 explicit capture 强制补成同一个值
  - `manual_review_count` 只来自 job-level canonical count 或 `progress.counters.manual_review_count`；Stage 1/Stage 2 summary 中的 `manual_review_queue_count` 不得作为运行页“需人工审核候选人”指标
- 候选人看板 filter/facet contract：
  - public facet 真值 owner 始终是 revision-fenced `projection_person_search_index`；初始 membership publication 必须暴露 facet `pending` / `unavailable`，不得从 members、当前 page 或本地 rows 同步重算
  - `facet_summary_scope` 和 `filter_contract.facet_count_scope` 是两个不同字段。前者表示 facet summary 覆盖范围，后者表示 filter count 的计数来源/readiness。前端不得用其中一个推导另一个，也不得在不同 endpoint 返回不一致时自行择优合并；这属于后端 Contract 漂移，应该触发 preflight/烟测失败。
  - `filter_contract.facet_count_scope="exact_projection"` 表示 filter count 来自 canonical projection/index 的完整投影计数；`unavailable` 表示计数不可展示；`index_partial` 表示索引未追上完整投影。前端只消费这些枚举，不从本地候选行窗口重算全局计数。
  - 前端只有在 facet/index product 的 membership revision 与当前 projection token 相等且 scope exact 时，才把 facet counts 作为全局计数展示；membership `N/N` 或 card `C/N` 完成不能替代 index readiness
  - 在候选行仍按 chunk hydration 装载时，前端可以提供筛选控件，但不能把当前 chunk 的局部 counts 当作全局业务真值
  - 用户一旦手动修改 filter，后续 hydration、facet option 扩展、running->results phase 切换都不能把选择重置为默认值；只能移除已经不存在的 option id
  - 全部具体选项都被选中时，摘要应显示 `全量` / fallback label，而不是显示第一个选项
  - 华人线索分层的 public `layer_0` 表示全量候选人，不是 outreach artifact 内部 `final_layer_distribution.layer_0` 的排他桶；Layer 1/2/3 是可筛选子桶，不能与 Layer 0 相加理解
  - 当 `filter_contract.backend_filtered_paging_supported=true` 时，前端当前页与筛选分页必须走 `/candidates` canonical backend paging；不得为了本地筛选把全量 7k/8k 行后台 hydrate 到浏览器
  - `GET /api/jobs/{job_id}/dashboard` 的 `asset_population.profile_fetch_progress` 与 `GET /api/jobs/{job_id}/candidates` 的 top-level `profile_fetch_progress`
  - 表示当前返回候选人集合的 LinkedIn URL hydration 状态，不代表全 job 所有候选人都已完成 profile detail
  - 字段包括 `total_url_count / fetched_url_count / queued_url_count / failed_retryable_url_count / unrecoverable_url_count / missing_registry_url_count / deferred_url_count / pending_url_count / status_counts`
  - 前端可以用它解释“候选人同步已完成但 LinkedIn profile materialization 仍在追尾”的状态
- `linkedin_stage_1_progress.profile_fetch_required_count` 是 Stage 1 profile 需求数的 canonical 字段
  - `profile_url_total_count` 是旧兼容字段，只能由后端/烟测历史报告消费，不得进入前端主路径或用户可见分母计算
- 候选人结果对象现在可稳定携带 `avatar_url / photo_url / media_url / primary_email`
  - 正常看板/详情路径的真源是 materialized serving page / candidate shard，而不是 request-time LinkedIn raw profile 二次解析
  - 前端应直接消费这些字段，不再自己从原始文本中推断头像或 Email
  - LinkedIn raw profile timeline resolver 只属于 legacy repair、diagnostic、显式 profile-completion/backfill、或 target-candidate export 打包 raw profile 文件的专用路径；不要把它作为候选人列表或完整 shard 详情的正常依赖
  - Raw `candidate_documents.json` 也不是 public-read serving fallback。正常 public reads 只能读取已物化 serving manifest/page/shard 或 job board-visible overlay；legacy candidate-doc materialization 必须由显式 repair/backfill/事件期 reconcile 先生成 normalized serving artifact，之后 public reads 才能服务
- 候选人结果对象现在也会携带 scoped-source provenance：
  - `matched_keywords` 是轻量筛选/display 字段，表示候选人命中过哪些 query/shard keyword
  - `source_matches` 是审计字段，允许同一个候选人同时属于多个 query/source shard；前端可把它映射为 `sourceMatches`
  - recall/filter 应优先消费 `source_matches` / `matched_keywords`；旧 artifact 缺这些字段时才 fallback 到文本搜索
- Target Candidate / CRM Public Web detail 必须把三类内容分开：
  - 长期人工确认资产：来自 `public_web_promotions` / `PersonAssertion`，包括已确认邮箱、Scholar、GitHub、X、homepage 等。它们是可导出资产，不属于某一次 latest run 的临时候选信号；重新搜索、provider 超时、AI fail-closed、latest run 没有新增 signal、latest run 仍在 queued/running/retry-wait，都不能让这些资产从 UI 或默认导出中消失。
  - 本次待审核候选：来自 latest run 且尚未人工确认/排除的 `person_public_web_signals`，必须绑定 `latest_run.run_id`，只允许对当前 latest run 的未决信号进行 promote/reject，避免旧 run 信号被误操作。若 latest-run signal 通过 `signal_id` 或 stable identity 匹配到已有 `manually_promoted` / `manually_rejected` promotion，它应归入已确认/已排除资产或历史状态，不再重复出现在“本次待审核候选”列表。
  - 只读证据来源：来自 evidence/source links，只解释模型判断与审计，不作为可直接导出的候选项，也不提供 promotion 操作。
  - Public Web detail cache 可以用旧 detail 显示长期人工确认资产；但本次待审核候选必须按当前 expected latest run id 过滤。expected latest run id 为空时，前端必须重新读取 owner detail 或只展示长期人工确认资产，不得让旧缓存自动通过。latest run id 变化不得删除整个 detail cache，因为这会隐藏 durable manual promotions；stale cache 只能保留 confirmed/rejected promotion 这类长期资产展示，不能重新暴露旧 run signals 为可操作候选。
  - 前端不得展示 `source projection provenance`、legacy table/backend、migration bridge 等 Contract/实现术语。缺少可打包资料时只能用用户可理解的业务文案。
  - 单人 `retry` / 重新搜索 API 是幂等用户意图，不是普通随机 force-refresh。前端可以不传 nonce；后端 owner 必须按 source run id、原因、workspace、操作者派生稳定 retry idempotency key 并 join 已存在 child run；PG normal path 也必须对 batch/run 非空 idempotency key 建唯一约束。重复点击、网络重试或丢失响应不得重复创建 provider/model run。
  - 重新搜索/start/retry 的 HTTP route 只能规划 `crm.public_web.queue_batch` root command；batch/run row 必须由 command owner 创建。前端不得把“已触发请求”理解为已有可见 run，必须以 owner 返回的 batch/run/detail 为准。若 owner disabled 或 command planning 失败，应展示业务错误，不得显示空的新搜索结果。
- CRM Public Web 阶段进度展示由后端 CRM Public Web owner 消费 `phase_commands.phase_order` 作为产品阶段分母，并物化为产品文案 `phase_command_display_line`。阶段名应类似“提交公开搜索 / 取回搜索结果 / 整理页面内容 / 判断候选信号 / 生成审核候选 / 保存公开信息结果”，不得把 `crm.public_web.*` command type 或 registry display label 直接展示给用户。`command_count` / `materialized_command_count` 是诊断字段，不能被前端用来显示 `1/2 -> 2/3 -> 3/4` 这类随命令物化抖动的阶段总数。DataForSEO remote-search 进度是 query-item 级别：一个 batch request 可以有 10 个 query task，其中 1 个 provider-pending/timeout 不代表整个 batch 失败或应整体重发。`provider_pending_deferred_count>0` 表示 provider 明确仍在处理单个 query，UI 应提示“可稍后刷新/后台等待”，不能把它展示成全批卡死、空结果或已确认信息消失。
- CRM Public Web 默认导出必须由后端 export owner 合并长期人工确认资产和终态 run 信号。前端不得因为 latest run 仍在执行或 latest run 没有 signal，就隐藏已确认资产或显示“没有可导出公开信息”。非终态 latest-run 信号不能作为导出事实，除非它已经通过 promotion owner 转成 durable manual promotion。

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
- `status=completed` 只表示主 job 可进入可浏览结果，不一定表示所有 background materialization tail 已完成；若 `progress.worker_summary.by_status` 仍有 `queued/running/waiting_remote_search/waiting_remote_harvest/blocked`，前端应保持 running/post-completion 工作态，并提示结果可浏览但 LinkedIn profile / candidate detail 仍在后台补全
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
- `linkedin_stage_1_progress`
- `board_runtime_state`
- `result_view_lifecycle`
- `execution_phase_contract`
- `workflow_stage_summaries`

`linkedin_stage_1_progress` 是执行过程页面的结构化采集指标来源，包含：

- `current_search_returned_count`
- `former_search_returned_count`
- `all_search_returned_count`
- `deduped_candidate_count`
- `deduped_profile_url_count`
- `profile_fetch_required_count`
- `profile_fetched_count`
- `profile_queued_count`
- `profile_failed_retryable_count`
- `profile_pending_count`

这些字段的详细来源和不变量以 [WORKFLOW_PROGRESS_CONTRACT.md](WORKFLOW_PROGRESS_CONTRACT.md) 为准。关键约束是：`profile_fetched_count <= profile_fetch_required_count`，且 `profile_fetch_required_count <= max(deduped_candidate_count, deduped_profile_url_count)`；后端必须合并 search-seed aggregate 与 current/former lane 文件，不能让 partial aggregate 造成 `新取回在职候选人0 / 新取回离职候选人74 / 需补取 LinkedIn Profile174` 这类不可解释状态。

`result_view_lifecycle` 是 Delta asset board streaming 的结果视图状态合同，前端不要从自然语言 timeline 推断这些状态。当前状态值包括：

- `baseline_serving`
- `delta_applying`
- `current_snapshot_materializing`
- `current_snapshot_serving`
- `post_result_layering`

`board_runtime_state` 是候选人看板的主业务状态合同，前端应该优先把它当作板面显示、同步进度、分层状态和全局 filter 的唯一业务来源。`result_view_lifecycle` 仍保留为结果视图/兼容性合同，用于生命周期与修复语义，但不应再和 `board_runtime_state` 竞争同一层的板面状态。

`execution_phase_contract` 是执行过程页面的阶段语义合同，前端不要只按旧 `stage_2_final` / timeline summary 推断标题。关键字段包括：

- `active_phase_id`
- `active_stage_id`
- `active_phase_label`
- `active_phase_detail`
- `public_web_stage_applicable`
- `profile_work_pending`
- `stage_title_overrides`
- `stage_detail_overrides`

当 `public_web_stage_applicable=false` 或 `active_phase_id` 是 `linkedin_acquisition` / `local_asset_materialization` / `post_result_layering` 时，前端不应展示默认 `Public Web Stage 2` 文案。对应可见标题应来自 `active_phase_label` 或 `stage_title_overrides`，例如 `LinkedIn Stage 1`、`本地资产物化`、`结果分层刷新`。

候选人看板同步卡应把 exact canonical visible membership 主同步 `N/N` 与独立卡片详情 `C/N` 分开展示；浏览器当前加载行数、patch 水位、profile readiness 和 card readiness 都不能互相代替：

- 主计数必须来自 exact `serving_projection_members` visible membership 投影的 `board_runtime_state.sync_status_text` 与 `expected_candidate_count=N`，并显示 `N/N`。`display_ready_candidate_count=C` 只表示 exact card-ready/detail；它不是主同步、分页、export/CRM source total 或 render 分子。
- `board_runtime_state.published_candidate_count` 是旧 row-publication 水位字段；新主路径的 hydration target 必须用 `board_runtime_state.row_hydration_target_count`。`published_candidate_count` 只能作为诊断/历史报告字段，不能进入分页、freshness、候选人同步或用户可见总量计算。
- 前端合并 `/progress`、`/dashboard`、`/candidates`、`/board-patches` 时先区分 canonical membership tier 与 legacy/partial tier；canonical summary/page/readiness 还必须携带相同的 `projection.membership_revision`。该 token 只比较相等/不等，不能排序；缺失或 mismatch 时 direct merge fail closed、清掉受影响 cache 并重新 resolve 一次 projection 后 pin 同一 token 重读。`updated_at`、发布时间、`row_publication_sequence` 和更大的 count 都不能替代 token；sequence 只排序同一 patch generation。
- 四个 endpoint 必须暴露同一 membership token 下可比较的 `board_runtime_state` 业务字段。`/candidates` 的分页 payload 只能贡献同 token 的 rows/filter window，不能根据当前 page/overlay 自行升级 membership、readiness 或 facet scope。若 endpoints 在 token、N、C、P、explicit capture、profile/card 文案、publication tier、layering 或 filter contract 上不一致，属于后端 contract 失败，fast preflight 与 `require_board_runtime_state_cross_endpoint_parity` 必须先拦住。
- 一旦存在 canonical projection link，新合同缺失、non-exact、token mismatch 或 read fallback 都必须 fail closed。前端不得兼容读取 `result_view_lifecycle.served_candidate_count / expected_candidate_count`、overlay、patch 或本地 rows 补成业务总数；这些字段只保留诊断用途。
- Frontend hydration 只能单独展示为“已加载候选行 loaded/expected”或“本地已缓存候选行”，不能把 `dashboard.candidates.length`、分页缓存长度、已加载片段筛选命中数称为最终同步进度。
- 全局 facet/filter 选项只能消费 `projection_person_search_index` owner 发布、且与当前 `projection.membership_revision` 相等的后端 `facet_summary`。`dashboard.candidates`、members、card-ready subset 和本地窗口不能推导全局地区、职能、在职状态或分层计数；后端 summary 缺失时保持 disabled/unavailable，不启用 fallback。
- `facet_summary_scope` 是后端 index product 声明字段。只有 `/dashboard` 或 `/candidates` 返回 revision-matched `exact_projection` / `global_full_population` 且 candidate count 等于 canonical `N` 时，前端才能展示全局 facet counts。初始 membership publication 的合法状态是 `pending` / `unavailable`。
- `current_served_partial` / `raw_profile_partial` 只能作为显式局部或未来 raw-profile 状态，不能显示为全局 counts；`unavailable` 隐藏/禁用相关计数和 active filter 请求。前端不得因为已有 summary 对象、exact membership 或 complete card detail 就把 facet scope 补成 global。
- 当用户选中了某个 facet 的全部 concrete options 时，右侧摘要应显示 fallback/all label，例如地区全部选中显示 `全量`，不能显示第一个 option（例如 `美国`）。
- 地区和职能这类 board-wide narrowing filters 的初始状态应是全量开放选择；除非用户明确切换，前端不得因为 hydration 期间 option/count 变化自动改成 `美国`、`未提供地区信息`、`Researcher` 等单选状态。
- LinkedIn profile 获取进度和卡片合入进度必须拆开展示。`profile_fetch_status_text` 只报告 owner-scoped `新增 LinkedIn Profile 已取回 P/R` 或 `本次 LinkedIn Profile 已取回 P/R`；`card_materialization_status_text` 报告同 membership token 的 exact `卡片详情已合入看板 C/N`。`profile_ready` 与 `card_ready` 各自独立，允许 `C>P` 或 `P>C`，前端不能从其中一个、published rows 或 hydration 推导另一个。
- `explicit_profile_capture_candidate_count` 只消费 explicit capture owner 的 evidence/scope。缺失时保持 unavailable；不得用 `profile_ready_count`、`profile_fetched_count`、`card_ready_count`、`display_ready_candidate_count` 或 visible membership 代填。
- `serving_projection_phase="current_snapshot_row_shell_overlay"` 表示后端已经发布可分页候选行，但 profile/card enrichment 仍在进行。前端可以渲染这些行和 row-sync 进度，但不能把它当作 `current_snapshot_serving`，也不能用全局 baseline profile 质量计数推导新增 delta 的 profile/card 完成数；profile/card 文案仍只消费 `board_runtime_state.profile_fetch_status_text` 和 `card_materialization_status_text`。
- row-shell 现在在 Stage 1 / candidate-source terminal 事件发布，而不是等待 Stage 2 final retrieval。前端看到该 phase 时应立即允许列表行浏览，同时继续展示 profile/card enrichment 进度；不要把“可分页候选行已发布”解释成 profile/card 已完成。
- 当 `board_runtime_state` 存在时，前端必须把 `profile_fetch_status_text` / `card_materialization_status_text` 当作用户可见 profile/card 进度的唯一来源。`linkedin_stage_1_progress` 仍可渲染执行过程页的 Stage 1 采集明细，但不能再二次格式化成另一套 profile/card 同步文案；`result_view_lifecycle` 只保留 diagnostic/explicit migration repair 语义，canonical link 存在时不能 fallback 补数。
- 候选人同步卡的说明行必须优先消费 `board_runtime_state.sync_note_lines`。前端可以保留 `note_text` 作为旧 payload 兼容，但不能把 `当前意图匹配 ...`、hydration 片段或本地 recall option counts 拼入同一 provider/card 业务进度行。
- 对于已有 canonical projection link 的 legacy 行，lifecycle/patch mirror 不再是用户可见 N/C/P 真源。前端只接受同一 `projection.membership_revision` 下的 exact membership/readiness；缺失或 mismatch 时 fail closed 并重读，不能通过 lifecycle helper 把 profile/materialized/board-visible 数规范化成 card readiness。
- 执行过程页的 `总候选人数量` 使用同 membership token 的 `board_runtime_state.expected_candidate_count=N`；row-publication / hydration 水位单独展示。只有尚无 canonical projection link 的显式历史迁移页可标记 migration fallback 读取 lifecycle；canonical link 存在时缺失/不 exact 直接 not-ready，不能显示 baseline 或 legacy expected 代替 `N`。
- 前端判断 results page 是否可渲染、是否仍在 candidate-page hydration、是否处于 bootstrapping，应统一使用 `dashboardHydration` 合同：同 token 的 exact `expected_candidate_count == served_candidate_count == N` 与可读 membership page 决定主看板；`N>0,C=0` 必须渲染 row-shell/detail-pending 卡片，exact `N=0` 才是空结果。Non-exact/mismatch/read fallback 是 not-ready，不能当空；`display_ready_candidate_count > 0` 不再是 render 门槛。
- `post_result_layering`、profile tail 和 card tail 都不是 candidate-row serving blocker。若 canonical exact membership `N/N` 已发布且 page token 匹配，frontend hydration banner 应关闭；独立 enrichment/index 状态继续显示自己的 pending 文案。
- Canonical projection state 只能在相同 membership token 下与其他 endpoint 直接合并。Token mismatch 时不能用 tier、时间或较大 count 猜赢家；应失效缓存、重新 resolve/pin。Revision-matched complete index facets 可压过同 token 的 stale layering mirror，但不能跨 token 拼接。
- 用户手动选择的 facet/filter 在 hydration 期间必须保留。选项 count 暂时为 0 只代表当前已加载片段不含该类别，不能自动重置到默认筛选。
- 候选人看板分页 API 的默认用户路径应请求 `lightweight=1`，这里的 lightweight 语义是读取同一 membership token 的 bounded serving row。`card_ready` 行应包含其 exact card fields；row-shell 可以显式缺少仍 pending 的详情并继续可见。任何行都不得靠 public-read raw timeline resolver 补字段，且 card-ready subset 不能替代完整 membership page。
- 大规模历史结果应采用两阶段加载：先用 exact canonical membership/page 与 revision-matched index summary 完成全局看板和筛选稳定，再通过 `/api/jobs/{job_id}/candidates/{candidate_id}` 或 `/candidates/batch` 读取 detail。详情接口消费 materialized shard；正常 public read 不为补字段解析 raw profile timeline。单候选详情只有显式诊断参数 `hydrate_legacy_timeline=1` 才允许进入 legacy resolver。
- `GET /api/jobs/{job_id}/results?include_candidates=1` 也只应返回 materialized asset-population rows，不应触发 raw profile timeline hydration；ranked results 在 public `/results` API 中同样是 materialized/summary-only。前端主看板应继续使用 `/dashboard` + `/candidates?lightweight=1`。
- `GET /api/jobs/{job_id}/board-patches` 是候选人看板的轻量增量观测接口。前端轮询必须传递并保存 `latest_sequence_index`，并附带 `latest_published_at` 作为兼容上下文，因为同一发布时间内可能存在多个 sequence patch。只要接口返回新的记录，就触发 authoritative projection/dashboard/page refresh；patch sequence 和 `display_ready` 都不是 membership revision、render 或 total 阈值。该接口不直接承载候选人行。
- 候选人看板 header/filter 不得从 `dashboard.candidates.length` 或本地缓存行数推断 canonical business count。部分 offset window 只能显示“已加载窗口筛选命中”；最终筛选命中只来自同 membership token 的 backend-filtered paging，不能因本地窗口恰好覆盖旧 hydration target 而升级。
- `GET /api/jobs/{job_id}/candidates` 是候选人看板筛选/分页的 canonical row endpoint。前端把 filters 传给后端；后端通过 revision-matched `projection_person_search_index` 对完整 exact visible membership 过滤，再分页并返回 `filtered_candidate_count`。Index pending/unavailable 时禁用 active global filters，不回退到 loaded window。
- Job-scoped baseline+delta overlay 只是 migration/repair artifact，不是 profile/card/member 真源。稀疏旧 row 必须由事件期 repair/backfill 先写入 canonical projection/readiness；public read 不从 current/baseline snapshot 临时补字段或拼 membership。
- Completed-workflow result-view repair 不能把 serving source repoint 到 raw `candidate_documents.json`。若旧 result view 只能从 candidate-doc 输入恢复，后端必须在事件期先生成 materialized serving artifact，再发布 manifest/materialized source；public read 不负责这一步。
- 前端可以有短暂 API/UI 采样延迟，但 canonical link 存在后必须收敛到同一 opaque membership token 下的 `N/N`。Profile/card/capture/index work pending 可独立追尾，不得把主同步退回 baseline，也不得为了消除尾部把 C/P/capture 抬到 N。既有延迟 gate 衡量 token/owner 状态收敛，不再要求 card readiness 与 membership 同时完成。

目标候选人 Public Web Search 的阶段展示应区分 remote provider wait 和 local processing：

- `searching` / `search_submitted` 可以跨 worker/daemon tick 等待外部搜索 provider ready。
- 一旦 remote search tasks 已取回，document fetch、AI adjudication、model-safe artifact finalization、signal materialization 都是本地连续阶段，不应靠 recovery tick 间隔逐步推进。
- `updated_at` 等运行时间字段在前端展示时必须通过统一 workflow time formatter 转为 Asia/Shanghai，而不是直接渲染 UTC/naive backend timestamp。

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
