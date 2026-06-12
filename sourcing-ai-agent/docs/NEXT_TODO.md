# Next TODO

> Status: Living tracker, rewritten 2026-06-11 as a short rolling file (keep under ~150 lines; rotate completed/superseded content into `docs/archive/`). Full pre-restructure snapshot: `docs/archive/NEXT_TODO_2026-06-10_full.md`.

## Session Resume Rule

- Tests: `PYTHONPATH=src ./.venv-tests/bin/python -m pytest <targets>`（永远跑 targeted 目标，`tests/test_pipeline.py` 全量 457 个测试不要整体跑）。
- 启动任何运行时前先读 `docs/RUNTIME_PREFLIGHT.md`；网络诊断只读（`make agent-network-preflight`），不得改 proxy/VPN 状态。
- PG-only normal path；不要 `git add .`；不可重建资产的破坏性操作仍走 reviewed 流程。

## Track Structure (decided 2026-06-11)

详细依据见 `docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md` 的 "2026-06-11 Plan Revision" 一节。

### Track A — Orchestrator 分解
- [x] **Phase 0 测试前置修缮**（2026-06-11，PR #15）：`tests/source_inspection.py` AST 解析助手；两个守卫测试文件脱离源码切片；patch 接缝；regression_matrix 未映射守卫；边界扫描 rglob 化。
- [x] Phase 1：`CommandKernel` 提取（2026-06-11）：17 个 store-only 协议方法迁入 `command_kernel.py`，facade 全名保留、577 调用点零改动、独立 AST 级验证。
- [x] Phase 2：CommandTypeSpec registry（2026-06-11，= 重定义后的 M1 落地）：`DEFAULT_COMMAND_TYPE_SPECS` 41 类型单一事实源；4 张字典+policy set 族+metrics 表+orchestrator 3 张映射全部收敛；`command_type_manifest()` 导出（Agent tool spec 种子）；金快照特征化测试 + 字节码级独立比对零漂移；src/ 裸 command-type 字面量清零。
- [x] Phase 3a：crm_public_web 域提取（2026-06-12）：首个领域 owner `crm_public_web_owner.py` 落地；orchestrator 保留全部公私方法签名等价 facade、调用点零改动；`regression_matrix` 映射更新；独立 AST 级验证（逐方法对比 git HEAD）。
- [ ] Phase 3b–3d：excel_intake → profile_fetch → acquisition（仅 command 层）按同一 playbook 提取；`run_worker_recovery_once` 的 drain 绑定改注册式。
- [ ] Phase 4：纠缠核心重设计（recovery phase 编排 registry 化；projection/candidate_source/asset_population 网随 M3–M5 拆解）。

### Track B — 存储与测试基建（与 A 并行）
- [x] 测试环境契约 v2（2026-06-11）：每 run = (PG schema + runtime dir) 配对 + `.ephemeral-test-env.json` 标记；teardown `DROP SCHEMA CASCADE`（仅删自建 schema，`pre_existing` 守卫）；孤儿 janitor `scripts/prune_test_schemas.py`（先快照后扫描、活跃连接守卫、仅限本地 DSN、dry-run 默认）。
- [x] Mac 本地 PG Docker 方案（2026-06-11）：`local_postgres_docker.py` + `make local-pg-up/down/status`；容器 55432 复用既有 DSN 发现机制零侵入；PG 强制模式下 durable runtime 套件真实执行验证。
- [x] PG 测试 fixture 试点（2026-06-12）：`tests/pg_store_fixture.py`（`PGControlPlaneStoreTestMixin`：per-class schema + `pg_tables` 截断复用）；8 个文件先行迁移；试点即捕获一个生产缺陷（见下条）。
- [x] PG ON CONFLICT 唯一索引缺口类修复（2026-06-12）：SQLite UNIQUE 约束从未镜像进 PG bootstrap，postgres_only 下 `upsert_criteria_pattern` 等触发 InvalidColumnReference；修复 = `_CONTROL_PLANE_UNIQUE_INDEXES` 幂等唯一索引 + 建索引前去重（有时近列保留最新，无时近列 loud failure）+ 复发守卫 `tests/test_pg_onconflict_guard.py`（机械扫描 ON CONFLICT 目标 vs 索引清单）。
- [x] PG fixture 迁移批次 2（2026-06-12）：12 个 store 级文件（asset consolidation/reuse、authoritative serving repair、job result lifecycle、legacy public web retirement、manual review、organization assets、search seed registry、serving projection storage/writer、smoke runtime seed、snapshot materialization backfill）；12/12 零新增失败零跳过，合并验证 52 通过 + 恰好 3 个基线内 pre-existing；本批未再爆出 PG 产品缺陷。
- [x] PG fixture 迁移批次 3（2026-06-12）：10 个中型文件（asset consolidation audit、cloud asset import、company asset completion/supplement、excel intake、organization execution profile、person asset crm projection、runtime rebuild、storage profile registry、target candidate public web）；合并验证 143 通过 + 11 个故意跳过 + 恰好 1 个基线内 pre-existing。**迁移再捕获 2 个真实 PG 产品缺陷并已修**（profile registry lease 缺行哨兵 None vs {}；refill plan PG bulk 路径丢 terminal 行 source_jobs 合并→postgres_only 下 per-job scope summary 少算）——invariant 7 的双路径语义分歧族第 3、4 例。
- [x] PG fixture 迁移批次 4（2026-06-12）：server/daemon 4 文件（`test_cli` 55/55、`test_workflow_explain` 恰预算 1 失败、`test_worker_recovery_daemon` 21/21、`test_crm_public_web_runtime_boundary` 32/32）；合并验证 124 通过 + 23 子测试。两个 PG 模式测试修法值得复用：dead-local-process stub 需同时 patch `control_plane_live_postgres` 模块内绑定；时间回拨必须改 PG 权威行而非 SQLite shadow。
- [x] PG fixture 迁移批次 5（2026-06-12）：三巨头完成——`test_candidate_artifacts` 58/58（顺带捕获并修复 canonical fallback 被 hot-cache 视图压制的产品缺陷,第 5 个迁移捕获缺陷）、`test_enrichment` 38→12、`test_results_api` 36→3（contract-cited 修复:proof seeding、W6 command-owned 断言、410 canonical endpoint;反伪造护栏零触碰）。直接实例化 SQLite store 的迁移**全部完成**。
- [ ] 收尾项：6 个已走 `PGDurableRuntimeTestMixin` 的可选统一；`test_pipeline`（42k 行，永不全量跑）单独设计；PG 适配器自测 3 个豁免（保持）。
- [x] advisory lock key 按 schema 命名空间化（2026-06-12，owner 批准趁 systemd 全量重启部署窗口落地）：7 个锁点统一走 `_advisory_lock_key()`（schema 前缀，空 schema 归一为 `public`）；跨 schema 互不争用 + 同 schema 互斥 + 默认前缀确定性均有实测锁定（`test_control_plane_pool.py`）。**部署约束：锁身份已变，上线必须全停重启，禁止新旧进程共存热部署**（现行 systemd 部署天然满足；Track C 容器化滚动部署前无需再协调）。
- [ ] 之后：按表组把 292 个双路径方法重写为 PG-pure 并删 mirror；最后移除内存 SQLite 影子。引入正式 migration 机制（PG DDL 目前在 `control_plane_live_postgres.py` 手工第二份）。**每个方法重写时必须特征化"行不存在"语义与 SQLite fallback 一致**（已知分歧族：ON CONFLICT 唯一索引缺口、`update_agent_runtime_session_status` 在 PG-only 下对缺行 raise 而 SQLite 静默 no-op——后者已修，见 `WORKFLOW_BEHAVIOR_GUARDRAILS.md` invariant 7）。

### Track C — Serving Runtime（目标 ~20 并发用户）
- [x] psycopg_pool 连接池（2026-06-11）：per-adapter 懒加载池（`SOURCING_CONTROL_PLANE_PG_POOL_MIN/MAX`，默认 1/8）；25 个调用点事务语义逐一核验不变；实测 200 次顺序操作 1.516s→0.743s、新建连接 200→1；`ControlPlaneStore.close()` 接线。
- [ ] 重活出请求线程：plan compile / `/api/jobs` / 导出统一为 enqueue + 轮询（后续 SSE）。
- [ ] worker 与 API 进程分离（`worker_daemon` 独立进程成为唯一模式）。
- [ ] 最小鉴权 + 用户身份（token；`requester_id/tenant_id` 列已存在但来自未认证 payload）。
- [x] FastAPI + uvicorn 传输层等价重写 api.py（2026-06-12）：同路由/同 payload/同状态码/同 headers；CORS allowlist + localhost 自动放行 + header 回显；Apify webhook token 校验保留；双道信号量改 middleware（HarvestAPI 并发约束保留至 M2 provider 预算落地）；`create_server` 兼容垫片包 uvicorn（serve_forever/shutdown/port-0）；`tests/test_api_transport_parity.py` 传输等价测试。
- [ ] FastAPI 第二步：handler 签名 pydantic 模型化 → OpenAPI 成为前端 contract 生成源；SSE 推送替代 1s/5s 轮询。
- [ ] 多用户 Agent serving 拓扑（2026-06-11 确认，详见 plan doc revision 节）：按角色容器化（api/agent-worker/provider-worker，docker-compose 起步）；`agent_session`/`agent_turn` PG checkpoint + per-session 单写者 lease；`agent_events` SSE tail；per-user 并发限额；凭证只在 provider worker 层。**不做 per-user 常驻容器**；沙箱仅在将来加代码执行/浏览器工具时按工具调用租用。
- [ ] 之后：对象存储读穿（company_assets/media 出本地盘；`object_storage.py` 抽象已存在）。
- 明确不做：Redis、LISTEN/NOTIFY（当前规模不需要）。

### Track D — 强 Agent 化
- [ ] ModelClient 升级：streaming + tool-calling（现有 14 个单发方法、阻塞 requests、无流式）。
- [ ] Agent Session 契约：服务端 agentic loop；工具面 = M1 manifest 导出 + 只读上下文工具 + model_native_search/fetch 转正；效果全部走 typed AgentAction（边界已由 `AGENT_OPERATION_CONTRACT.md` 规定）。
- [ ] 第一垂直切片：公司身份自验证 loop（搜索→fetch 验证→歧义才升级人工），替代 PlanCard 手动修正 LinkedIn URL。
- [ ] 之后：plan review 对话化、intent→plan 前门流式化；OpenClaw/Claude 作为可插拔外脑。

### Track E — 治理
- [x] 文档治理（2026-06-11 完成，PR #15）：21 份归档、PROGRESS/NEXT_TODO 轮转、INDEX 分层、治理规则成文、决策记录迁入 PRE_AGENT_CONTRACT_REVIEW。
- [x] `runtime/test_env` TTL 清理（2026-06-11 applied）：648 个目录、回收 130,498,437,051 字节（~121.5 GiB）、0 失败；磁盘可用 68Gi→183Gi；test_env 117G→1.4G。记录：`runtime/asset_governance/ttl_apply_20260611/ttl_local_rebuildable_apply_v2.json`。过程中修复了活动进程检测器的 PID 复用误报（身份比对 + 新鲜度窗口）。
- [ ] 后续例行：`make prune-test-env`（TTL 默认 14 天）目标待加；测试 harness teardown 钩子随 Track B 契约 v2 落地。
- [ ] M1 后：contract 文档 per-command 段落由 CommandSpec registry 生成；守卫测试改对 registry。

### M2 Provider Task Runtime 设计要求（新增约束）
- Provider 级并发预算：HarvestAPI profile-fetch 有 ~8 并发 actor 的隐性限制（"too many requests"），旧 8 槽 API 信号量即源于此——保护必须移到 provider 层（per-provider+key 的信号量/令牌桶），HTTP 入口的并发上限才能放开。
- API key 池化扩容；高需求下避免 profile fetch batch 过度碎片化。

## 已知失败预算（必须归因后入账，不得静默增长）

> 规则：新失败先做 control-environment 归因（在无该变更的对照环境复现）再入账；修复后移除条目。2026-06-12 凌晨批次的 8 个被举报失败已全部归因并修复（3× 测试自身 Thread.start 全局 stub 扼杀 psycopg_pool 工作线程、1× 测试未随 06-07 workspace fail-closed 契约更新、1× c942874 携带的 get_job_api include_details 压缩丢字段、3× c942874 携带的 completion-policy/W6/canonical-projection 漂移——其中投影读路径丢 job-scoped 标记是真实产品缺陷，已修）。

- `tests/test_results_api.py`：36 → **3**（2026-06-12 迁移+契约修复后）。剩余 = 看板合并计数桶（卡片详情已合入看板 112/297 vs 186/297、115/140 vs 140/140、population floor 80≠297）——pre-handoff 投影/看板漂移，无可引用已提交契约。
- `tests/test_enrichment.py`：38 → **12**（2026-06-12 迁移+修复后）。剩余 = pre-handoff 调度器重构的 envelope/packing/coalescing 漂移（批大小、波次预留、tiny-coalescing；契约只存在于未跟踪文档）+ 1 个手写 store stub 缺 typed-command 面。**与调度器重构 reconciliation 一起修**；同主题 owner 决策项：storeless enricher 零派发却报 completed 的 fail-open（`enrichment.py:3056-3060`，invariant 7 族）。
- `tests/test_workflow_explain.py::test_explain_workflow_does_not_use_legacy_standard_bundle_as_hidden_full_coverage_proof`（2026-06-11 归因：pre-handoff uncommitted worktree state，与 CommandKernel/registry 提取无关）。
- `tests/test_control_plane_live_postgres.py::test_serving_projection_foundation_uses_live_postgres_tables`（同上）。

## Decisions Log (2026-06-11)

- M1 重定义为 CommandSpec registry + manifest 导出物；M0.9 独立审批流程取消（10 个冷备已 sha256 验证，源目录随 TTL 清理）；"先删 SQLite fallback"被否（51 测试文件 + 内存影子依赖）；Redis 不引入；FastAPI 重写批准；Mac PG 用 Docker；Independent Review Gate 范围收窄到不可重建资产与 contract-heavy 变更。

## Resume Checklist

1. `git status --short` 确认 worktree 状态后再动文件；只按显式路径 stage。
2. 读 `docs/INDEX.md` 的 Current Stage Checkpoint 与本文件 Track Structure。
3. 跑 `bash ./scripts/dev_status.sh --runtime-dir runtime --api-port 8765 --frontend-port 4173` 了解本地服务状态。
4. 改 contract 相关代码前先全库搜符号引用。

## Useful Commands

```sh
# 状态
bash ./scripts/dev_status.sh --runtime-dir runtime --api-port 8765 --frontend-port 4173
df -h . && du -sh runtime runtime/test_env output 2>/dev/null
# 测试（targeted）
PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/<file>.py -q
make ci-pre-agent-contract
# 网络只读预检
make agent-network-preflight
# 文档 banner 巡检
python3 - <<'PY'
from pathlib import Path
paths = [Path("README.md"), Path("PROGRESS.md"), *sorted(Path("docs").glob("*.md"))]
missing = [str(p) for p in paths if not any(l.startswith("> Status:") for l in p.read_text(errors="replace").splitlines()[:8])]
print("\n".join(missing) or "all banners present", f"\nchecked={len(paths)} missing={len(missing)}")
PY
```
