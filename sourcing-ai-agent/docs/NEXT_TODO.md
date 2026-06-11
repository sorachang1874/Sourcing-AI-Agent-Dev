# Next TODO

> Status: Living tracker, rewritten 2026-06-11 as a short rolling file (keep under ~150 lines; rotate completed/superseded content into `docs/archive/`). Full pre-restructure snapshot: `docs/archive/NEXT_TODO_2026-06-10_full.md`.

## Session Resume Rule

- Tests: `PYTHONPATH=src ./.venv-tests/bin/python -m pytest <targets>`（永远跑 targeted 目标，`tests/test_pipeline.py` 全量 457 个测试不要整体跑）。
- 启动任何运行时前先读 `docs/RUNTIME_PREFLIGHT.md`；网络诊断只读（`make agent-network-preflight`），不得改 proxy/VPN 状态。
- PG-only normal path；不要 `git add .`；不可重建资产的破坏性操作仍走 reviewed 流程。

## Track Structure (decided 2026-06-11)

详细依据见 `docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md` 的 "2026-06-11 Plan Revision" 一节。

### Track A — Orchestrator 分解
- [/] **Phase 0 测试前置修缮**（进行中）：`tests/source_inspection.py` AST 解析助手；`test_crm_public_web_runtime_boundary` 与 `test_pre_agent_contract_review` 的源码切片断言改造；`_utc_now_iso` 与 projection 分页常量的 patch 接缝；regression_matrix 未映射文件守卫；边界扫描 rglob 化。
- [ ] Phase 1：提取 `CommandKernel`（~30 个 store-only 协议方法）。
- [ ] Phase 2：CommandSpec registry（落 `durable_runtime`，合并 4 张字典 + 3-4 个 policy set 族 + metrics 表 + orchestrator 3 张 per-type 映射；manifest 与 Agent tool spec 是它的导出物 = 重定义后的 M1）。
- [ ] Phase 3：逐域提取（顺序：crm_public_web → excel_intake → profile_fetch → acquisition 仅 command 层；facade 保留全部公私方法名；`run_worker_recovery_once` 的 drain 绑定改注册式）。
- [ ] Phase 4：纠缠核心重设计（recovery phase 编排 registry 化；projection/candidate_source/asset_population 网随 M3–M5 拆解）。

### Track B — 存储与测试基建（与 A 并行）
- [ ] 测试环境契约 v2：每 run = (PG schema + runtime dir) 配对；teardown `DROP SCHEMA CASCADE`；孤儿 schema janitor；seed 资产默认 symlink；advisory lock key 按 schema 命名空间化。
- [ ] 51 个直接实例化 SQLite `ControlPlaneStore` 的测试文件迁移到共享 PG fixture（推广 `tests/pg_durable_runtime.py` 模式）。
- [ ] Mac 本地 PG：Docker 方案（`local_postgres.py` 当前 Linux 专用）；CI 用 testcontainers。
- [ ] 之后：按表组把 292 个双路径方法重写为 PG-pure 并删 mirror；最后移除内存 SQLite 影子。引入正式 migration 机制（PG DDL 目前在 `control_plane_live_postgres.py` 手工第二份）。

### Track C — Serving Runtime（目标 ~20 并发用户）
- [ ] psycopg_pool 连接池（最大单项杠杆；`control_plane_live_postgres.py:_connect` 每查询新建连接）。
- [ ] 重活出请求线程：plan compile / `/api/jobs` / 导出统一为 enqueue + 轮询（后续 SSE）。
- [ ] worker 与 API 进程分离（`worker_daemon` 独立进程成为唯一模式）。
- [ ] 最小鉴权 + 用户身份（token；`requester_id/tenant_id` 列已存在但来自未认证 payload）。
- [ ] FastAPI + uvicorn 重写 api.py（已批准；pydantic→OpenAPI 反向成为前端 contract 生成源）；SSE 推送替代 1s/5s 轮询。
- [ ] 之后：对象存储读穿（company_assets/media 出本地盘；`object_storage.py` 抽象已存在）。
- 明确不做：Redis、LISTEN/NOTIFY（当前规模不需要）。

### Track D — 强 Agent 化
- [ ] ModelClient 升级：streaming + tool-calling（现有 14 个单发方法、阻塞 requests、无流式）。
- [ ] Agent Session 契约：服务端 agentic loop；工具面 = M1 manifest 导出 + 只读上下文工具 + model_native_search/fetch 转正；效果全部走 typed AgentAction（边界已由 `AGENT_OPERATION_CONTRACT.md` 规定）。
- [ ] 第一垂直切片：公司身份自验证 loop（搜索→fetch 验证→歧义才升级人工），替代 PlanCard 手动修正 LinkedIn URL。
- [ ] 之后：plan review 对话化、intent→plan 前门流式化；OpenClaw/Claude 作为可插拔外脑。

### Track E — 治理
- [/] 文档治理（进行中）：21 份归档、PROGRESS/NEXT_TODO 轮转、INDEX 分层、治理规则成文。
- [/] `runtime/test_env` TTL 清理（进行中）：prune 工具加 local-rebuildable 模式（免 review artifact，保留 dry-run/保护名单/活动进程检查）；TTL=14 天 dry-run 待用户确认后 apply（预计回收 ~110G）。
- [ ] M1 后：contract 文档 per-command 段落由 CommandSpec registry 生成；守卫测试改对 registry。

### M2 Provider Task Runtime 设计要求（新增约束）
- Provider 级并发预算：HarvestAPI profile-fetch 有 ~8 并发 actor 的隐性限制（"too many requests"），旧 8 槽 API 信号量即源于此——保护必须移到 provider 层（per-provider+key 的信号量/令牌桶），HTTP 入口的并发上限才能放开。
- API key 池化扩容；高需求下避免 profile fetch batch 过度碎片化。

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
