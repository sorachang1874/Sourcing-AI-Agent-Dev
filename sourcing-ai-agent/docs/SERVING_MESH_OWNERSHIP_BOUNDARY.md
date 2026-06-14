# Serving Mesh Ownership Boundary — Phase 4 Step 4

> Status: Boundary freeze (2026-06-14). Phase 4 Step 4 deliverable — boundaries frozen now; code extraction lands with M3-M5 (decision #5). No code moved by this doc.

参见 `docs/PHASE4_ENTANGLED_CORE_DESIGN.md` §2(b) option B2 + §3 Step 4。所有 `file:line` 引用基于 `src/sourcing_agent/orchestrator.py`（73,807 行）@ `9db6943`，已对当前树逐条 re-verify（Audit-B 范围已随 orchestrator 变动而平移）。

## 1. 目的：为什么现在冻结边界而不搬代码

projection / candidate_source / asset_population 三者在 orchestrator 内构成一张纠缠 mesh（含一条 candidate_source ↔ projection reader 的双向环，见 §3）。若 M3-M5 服务化时才临时画线，会把"今天的纠缠"原样复制进服务边界，环会变成跨进程 RPC 环。decision #5：**现在冻结四块切分 + resolver 接口，M3-M5 按本边界 verbatim-move 搬代码**。本步只产出本文件，不动任何代码、不改任何测试。

## 2. 四块切分

### (a) ResultView / CandidateSource RESOLVER →（新 resolver 模块；M3-M5 首个抽取目标）

解双向环的关键块。

成员（核心解析）：`_resolve_job_candidate_source` 23638；`_build_job_results_context` 23858（在 23896 调 resolver）。其它调用点 1246 / 23773 / 31565 / 35492 / 36103。
成员（result_view stub/lifecycle）：`_candidate_source_result_view_stub` 3945；`_candidate_source_result_view_payload/_metadata/_summary` 4317/4323/4333；`_build_job_result_view_payload` 22161；`_persist_job_result_view` 22287；`_record_job_result_view_row_shell_publication` 22345；`_record_job_result_view_lifecycle_publication` 22432；`_apply_job_result_view_to_candidate_source` 22679；`_attach_persisted_lifecycle_to_candidate_source` 22763；recovery/repair `_should_recover…` 22832、`_recover_job_candidate_source_from_authoritative_registry` 22871、`_resolve_materialized_current_workflow_snapshot_result_view` 23056、`_recover_delta_only_result_view_to_baseline_delta_overlay` 23339。
成员（precedence 谓词，reader-touching）：`_candidate_source_claims_current_snapshot_serving` 4934；`_candidate_source_final_serving_artifact_outranks_overlay` 5043。

自然 owner：CandidateSourceResolver。
冻结接口（签名照搬当前 23638）：
```
resolve(*, job, request, job_summary=None, stage1_preview_summary=None,
        linkedin_stage_1_progress=None) -> tuple[candidate_source: dict, result_view: dict]
```
依赖方向：resolver **只依赖 ServingProjectionReader 接口**（`get_projection` 等），**禁止反向依赖 projection 命令层（Block 3）或读模型 paging（Block 2）**。precedence 谓词 5043 调 reader.`get_projection`（5028）保留——这是 resolver→reader 的单向边，合法。
关键约束：projection 翻页（Block 2 的 dashboard/page）**改吃 resolver 输出** `context["candidate_source"]`，不再自己重解（断 Edge B，见 §3）。

### (b) ServingReadModel 服务 →（读模型模块；M3-M5 落地）

public board 读模型 + asset_population 缓存"读"半边。

成员（读模型）：`_build_public_board_runtime_projection` 25774；`get_job_dashboard` 26671；`get_job_candidate_page` 30098；`_load_job_canonical_serving_projection_payload` 26175（reader.`get_projection` 26222）；`_apply_canonical_serving_projection_readiness` 26020；`_canonical_public_board_candidate_source` 6157；`_build_public_board_visible_patch_payload` 30652；`_build_job_asset_population_page_from_canonical_projection` ~30358（reader.`get_projection_candidates` 30437）。
成员（asset_population 缓存半边）：ctor attrs 1012–1015；`_asset_population_cache_key` 23950；overlay summary cache r/w 24098–24191；`_invalidate_asset_population_snapshot_cache` 24053；overlay payload cache 4270/4277/4288。
成员（facet summaries）：`_asset_population_facet_summary_cache_key` 25064；`_build_asset_population_facet_summary_*` 25241/25342/25374；`_candidate_source_stored_facet_summary[_scope]` 4507/4529；facet cache r/w 25391–25429。

自然 owner：ServingReadModel。
对外接口形状：`get_job_dashboard(...)`、`get_job_candidate_page(...)` 维持现签名；内部 paging 消费 resolver 输出而非重解 candidate_source。
依赖方向：依赖 resolver(a) 输出 + ServingProjectionReader + 9 方法读族(c)；**单向向下**，不被 (a)/(c)/(d) 反依赖。

### (c) ProjectionCommandOwner（按命令族）→ ProjectionCommandOwner

6 个 plan/enqueue/process/drain 命令带 + 9 方法投影读族。

finalize 带：`_plan_run_scope_projection_finalize_command` 7029、`_drain_run_scope_projection_finalize_commands` 7506。
person-search-index 带：plan 8248、enqueue 8136/8357、process 8395、drain 8949。
facet-layering 带：enqueue 19669、process 20482（`_from_serving_projection` 19980）、plan-for-item 8044。
admission 带：`_plan_operation_native_projection_admission_command` 16313、`_drain_operation_native_projection_admission_commands` 17613。（orch:979 注入给 ProfileFetchOwner 的 callable 即 admission——**写侧命令**，非读。）
export 带：`_plan_projection_export_generate_command` 27887。
adjacent（collection merge）：7724/7812/9160/9614。

**9-METHOD READ FAMILY（归本块，不归 profile_fetch）**：`get_serving_projection_api` 27380、`get_serving_projection_candidate_page` 27386、`search_projection_person_index_api` 27404、`get_serving_projection_person_detail_api` 27487、`get_person_summary_api` 27503、`list_company_assets_api` 50166、`list_company_evidence_api` 50201、`list_company_assertions_api` 50237、`get_media_asset_content_api` 50272。这 9 个 thin-wrap `serving_projection_reader.*`（27381/27394/27412/27494/27509）。
**纠正 prompt 前提**：ProfileFetchOwner **不**消费这 9 读族——orch:979 注入的是 admission（写侧），ProfileFetchOwner 其它绑定（16283/16298/16311/17088）全是命令 planner/executor，从不碰读族。9 读族 truth source = `ServingProjectionReader`，自然挂在读模型(b) 之上，由本块的 reader 提供，**非 profile_fetch**。

自然 owner：ProjectionCommandOwner。
对外接口形状：命令带对外是 plan/enqueue/process/drain 四元；9 读族对外是 `*_api(...)` 现签名（如 `get_serving_projection_api(projection_id) -> dict | None`）。
依赖方向：命令带写 `serving_projection_writer`（928/7687）；读族读 `serving_projection_reader`（929）。两者均向下依赖 store，不反依赖 (a)/(b)/(d)。

### (d) fast-path 拆两半（`_execute_asset_population_fast_path` 68704, 终 ~69037）

缓存半 → 读模型(b)；完成写入半 → completion owner。
- 完成-WRITE 半（→ completion owner）：`_write_job_asset_population_overlay` 68789；`_persist_job_result_view` 68947；`_record_job_result_view_row_shell_publication` 68959；deferred-completion blocker 68772–68796/68943–68981；`_mark_completed_workflow_*`。
- 缓存-READ 半（→ 读模型 (b)）：asset_population payload/summary cache 命中路径，复用 (b) 缓存半边的 key/invalidate。

依赖方向：write 半 → completion owner（向下到 store + result_view persist）；read 半 → (b)。fast-path 入口本身在 M3-M5 后退化为对两半的编排薄壳。

## 3. 双向环与解法

当前精确两条边：
- **Edge A（resolution → reader）**：`_candidate_source_final_serving_artifact_outranks_overlay`（5043）调 `serving_projection_reader.get_projection`（5028）；该谓词被 precedence/resolution 路径调用于 4256 / 5136 / 5187 / 24640 / 25432。即 candidate_source precedence resolution 伸进 projection reader。
- **Edge B（paging → candidate_source）**：`_build_job_results_context`（23858）→ `_resolve_job_candidate_source`（23896）把 `(candidate_source, result_view)` 写进 `context`；dashboard（`get_job_dashboard` 26671）与 page（`get_job_candidate_page` 30098）各自 `dict(context.get("candidate_source"))`，喂给 `_build_job_asset_population_page_from_canonical_projection`（~30358），后者经 `_load_job_canonical_serving_projection_payload`（26175）从 candidate_source 取 `projection_id` 再调 `get_projection_candidates`（30437）。

净效果：candidate_source resolution → reader（A），且 reader/paging → candidate_source（B），成环。

**B2 解法**：保留 Edge A（resolver→reader 单向，合法）；**消除 Edge B 的"重解"**——paging 不再独立解 candidate_source，而是消费 resolver(a) 的输出。环降为有向无环：`(b) paging → (a) resolver → reader`。

## 4. 已在外且不动的模块（stays）

- `ServingProjectionReader`（`serving_projection_reader.py`，ctor 929 注入，read-only over store）— 不动，作为 (a)/(b)/(c) 共同下游接口。
- `ServingProjectionWriter`（`serving_projection_writer.py`，ctor 928 注入）— 不动，命令带(c) 的写下游。
- `ProfileFetchOwner`、其余已抽 owner — 不动；仅纠正其与 9 读族无消费关系（§2c）。

## 5. 现有守卫与新增守卫需求

现状：9 读族的 forbidden-mutation 守卫散落于 `tests/test_pre_agent_contract_review.py`、`tests/test_serving_projection_writer.py`、`tests/test_person_asset_crm_projection_contracts.py`、`tests/test_regression_matrix.py`、`tests/test_operation_runtime.py`——读族经 `ServingProjectionReader`（只读 store），无直接写守卫尚未集中化。
边界冻结后应新增（仅描述，不实现）：
1. 结构守卫：resolver(a) 模块 import/调用图**不得**触达 projection 命令层(c) 的 plan/enqueue/process/drain 符号，也不得触达读模型(b) paging。
2. 结构守卫：读模型(b) paging **不得**再调 `_resolve_job_candidate_source`（强制消费 resolver 输出，钉死 Edge B 已断）。
3. 不变量守卫：9 读族保持 read-only——禁止经 `serving_projection_writer` 或 store 写路径（把现有零散 forbidden-mutation 断言集中到读族契约测试）。

## 6. 迁移顺序提示（供 M3-M5）

1. **先搬 resolver(a)**——它解环；先断 Edge B（paging 改吃 resolver 输出）再抽出模块，否则环会被复制成跨服务环。
2. 再搬读模型(b)（依赖 a 已稳定的输出）。
3. 再搬命令带(c)（含 9 读族，挂在 (b)/reader 之上）。
4. 最后拆 fast-path(d) 两半。
每块验证：**verbatim-move playbook**（按行平移、不改语义）+ **特征化测试（characterization）** 锁定搬前/搬后行为字节级等价；每块搬完跑 §5 对应结构守卫 + 既有回归矩阵。

## 7. 非目标

- Phase 4 **不搬任何 B 带代码**（本文件仅冻结边界）。
- **不改 projection 存储后端 / overlay 格式**——Track B PG-pure 另行推进，与本边界正交。
- 不调整 `ServingProjectionReader/Writer` 接口签名（stays，§4）。
