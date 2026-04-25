# Next TODO

> Status: Living tracker. Use the latest entries as the source of truth, and assume older bullets may describe superseded intermediate states.


## Goal

这份清单记录当前工作 backlog。它不是默认 onboarding 文档；接手项目时应先读 `docs/INDEX.md` 和 `../PROGRESS.md`，再回到这里看待办。

## Session Resume Rule

- 每次 context compact / 会话恢复后，先检查最近交互记录与 `PROGRESS.md` 的最新条目，再开始执行命令或重新排查。
- 开始跑命令前，再额外确认两件事：
  - 当前 `workdir` 是否已经回到真实仓库根目录
  - Python / 测试命令是否显式走仓库 `venv`（当前默认应为 `./.venv-tests/bin/python`）
- 目标是先恢复“上一次做到哪里、哪些结论已确认、哪些命令已经跑过”，避免重复做同一轮环境探测、回归或根因分析。

## Highest Priority

### Evening session tracker

- `2026-04-23 20:00` 之后的具体问题与收尾状态，现集中记录在：
  - [SESSION_TRACKER_2026-04-23_PM.md](./SESSION_TRACKER_2026-04-23_PM.md)
- 这份 tracker 现已关闭，保留作 closure record。
- 开始新一轮工作时，可先快速看它了解 4 月 23 日晚间那轮结论，但活跃 backlog 以本文件和 `PROGRESS.md` 为准。

### Active long-tail hardening tracker

- `2026-04-24` 剩余架构尾巴与验证记录集中维护在：
  - [SESSION_TRACKER_2026-04-24_LONG_TAIL.md](./SESSION_TRACKER_2026-04-24_LONG_TAIL.md)
- 每次 context compact / 会话恢复后，先读这份 tracker、`../PROGRESS.md` 最新条目和本文件，再继续执行。
- 这轮 checklist 已收口，保留为防回退记录：
  - runner/recovery ownership
  - plan hydration request-signature dedupe
  - runtime 独立 PG/schema bootstrap
  - Harvest 全局 in-flight budget/backpressure
  - asset governance/promotion
  - cold materialization 优化
  - SQLite compatibility surface 最后清理
- 后续如果继续优化，不应重新打开旧双轨或补丁分支；只能在同一 contract 上继续深化：
  - materialization source-index / writer-budget 的真实大 snapshot benchmark
  - workflow recovery / plan hydration / provider budget 的更多 scripted scenario gate
  - SQLite legacy tooling 的 UI/CLI banner 与迁移出口

### Active productization tracker

- `2026-04-25` 新一轮产品化尾巴集中维护在：
  - [SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md](./SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md)
- 已完成第一批可执行 contract：
  - runtime service cooperative shutdown / workflow cancel API
  - multi-snapshot coverage 从 mode proxy 收到显式 `coverage_proof`
  - 后端 `execution_strategy_label` 作为前端检索策略 label 的优先来源
  - SQLite legacy runtime CLI banner / migration exit
- 已完成第二批产品化 contract：
  - materialization coalescing / writer-budget report
  - workflow smoke `materialization_streaming` 指标与 aggregate guardrail
  - scripted provider scenario coverage validator + 429/timeout/partial/staged-ready fixture
  - asset governance default pointer / canonical replacement / historical retention plan helper
  - Excel intake `throughput_plan`
  - product journey regression matrix entries
- 已完成第三批产品化 contract：
  - asset governance default pointer/history 已有 DB/API/CLI writer
  - Excel intake 完成后可导入 target candidates 并导出 profile bundle
  - Harvest profile scraper 支持 provider batch response callback，enrichment 会即时消费单 batch 返回
  - snapshot candidate-document sync 已纳入 shared `materialization_writer` budget，writer slot 支持 reentrant 防死锁
- 继续时不要重新打开旧 proxy / fallback：
  - `preferred_snapshot_subset` 不能单独作为 coverage proof
  - 非 PG-only runtime 只能是 dev/emergency legacy
  - 前端不应绕开后端 execution semantics 自行推断核心检索策略

### Current closeout checklist before opening the next session

- 这轮最终收尾任务已同步到：
  - [SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md](./SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md)
- 下次 context compact / 新会话恢复时，先读该 tracker 的 `Session Closeout Checklist`，不要重新凭记忆判断当前进度。
- closeout 当前状态：
  - [x] 复核测试体系是否覆盖真实用户关心的 workflow 行为指标，而不仅是“脚本跑完”
  - [x] 跑 scripted / simulated test runtime / browser E2E / frontend build / typecheck / full pytest
  - [x] 验证暴露的 browser plan semantics 问题已按 shared contract 修复并补测试
  - [x] 全量 review Markdown status，重点更新 ECS 操作部署、PG-only/runtime isolation、venv、前后端启动、测试入口和踩坑记录
  - [x] review `AGENTS.md` 与开发习惯文档，补充根因识别、避免双轨制、共享抽象、TODO/进度记录、防止回退错误版本等规则
  - [x] 新建 Git branch，谨慎 stage 目标文件，避免把 runtime/cache/vendor/build output 推上 GitHub
  - [x] push 到 GitHub 后输出 session summary / handoff，便于用户新开会话继续
- GitHub handoff 已完成：
  - branch: `productization-2026-04-25-stable`
  - remote: `origin/productization-2026-04-25-stable`
  - commits:
    - `ef1ea4c Stabilize productization workflow contracts`
    - `53d30f1 Add workspace development guardrails`
- 新会话 handoff 已落到：
  - [SESSION_HANDOFF_2026-04-25.md](./SESSION_HANDOFF_2026-04-25.md)

### Recently Completed

- PG `acquisition_shard_registry` former/current physical split is complete.
  - PG writer/sync/migration/export now converge on split physical tables plus one logical compatibility view.
  - This item should no longer stay in backlog as “remaining pure Postgres cutover”.
- `/api/jobs/{job_id}` default payload has been reduced to summary-first.
  - callers that truly need `events` / `intent_rewrite` must use `?include_details=1`
  - default frontend polling should no longer pull full job detail blobs by accident
- full repo `pytest` and repo-configured `mypy` have both been rerun green after the split/API cleanup pass.
- `asset_reuse_planning.py` no longer relies on a separate large-org-only reuse branch for primary planning decisions.
  - the main path now uses a unified authoritative-baseline completeness contract
  - the old “large-specific helper” remains only as a compatibility wrapper for tests/callers
- plan submit path has been split:
  - frontend now uses `POST /api/plan/submit`
  - route transition no longer blocks on full synchronous `explain_workflow()`
  - `frontend_history_link.metadata.plan_generation` is now the shared pending/running/completed/failed contract
- Excel intake relaunch gate is closed:
  - homepage entry is enabled again by default
  - browser upload now posts real `FormData` to `/api/intake/excel/workflow`
  - local same-origin dev/preview has proxy support plus loopback fallback for `localhost/127.0.0.1`
  - multi-company split-job flow is covered by a real-browser regression

### 1. Harden `start-workflow` runner-spawn and background recovery

- `start-workflow` 当前虽然已经默认 non-blocking，但 runner-spawn / recovery daemon 仍需要更稳定的 ownership 和重试语义。
- 目标是不依赖当前终端存活，也能让 job 从 `plan_review -> background acquisition -> results` 自己收口。
- 需要把 job-scoped runner / daemon / worker 状态边界继续收敛，减少“主 job 已启动，但后台实际没接上”的不透明状态。

### 2. Run a fresh end-to-end quality pass on top of the stabilized Anthropic/Humans& workflow

- Anthropic 当前已经有可复用的 durable snapshot `20260409T080131`，不应再把“重新抓大组织全量 roster”当成默认验证方式。
- 更合理的下一轮验证应该聚焦：
  - query interpretation
  - retrieval quality
  - ranking / filtering
  - manual review routing
  - result explanation
- `Humans&` 仍然适合作为中等规模真实组织继续做一次完整质量测试。
- Anthropic 更适合作为“大组织增量复用、former fallback、adaptive shard”的长期 regression fixture。

### Plan hydration follow-ups

- 这轮已经解决“进入检索方案页要等同步 explain”的问题，并继续完成了：
  - request-signature 级 inflight dedupe / coalescing
  - plan hydration queue metrics（queued/running/wait ms）落到 history metadata
- 后续如果继续优化，重点不再是 dedupe，而是：
  - plan hydration metrics 接入更统一的 smoke / runtime report
  - `build_plan` / `execution_bundle` 再细拆，判断哪些 detail 可以继续延后到“进入方案页后再补”

### Public Web Stage 2 relaunch contract

- 当前已完成的收口：
  - default workflow 不再自动运行 `Public Web Stage 2`
  - default `/api/workflows` submission 已切回 `single_stage`
  - 只有显式 `two_stage` opt-in 才会把 `enrich_public_web_signals` 放进 plan
- 下一轮若要重启这条能力，不应再回到“默认开启并阻塞 board readiness”：
  - 需要显式 user opt-in
  - 更理想的是单独的 UI/endpoint，从已完成 job 上手动触发 stage2 augment，而不是重新把它塞回默认主链路
  - 重新上线前，需要先证明它确实能提供稳定增量，而不是只额外消耗 wall-clock

### Workflow behavior analysis in scripted/simulated tests

- 这层第一轮已经 productized 到 scripted smoke report/assertion，并有 guardrail 文档作为 active contract：
  - [WORKFLOW_BEHAVIOR_GUARDRAILS.md](./WORKFLOW_BEHAVIOR_GUARDRAILS.md)
- 当前已经接入：
  - 重复 provider dispatch signature 统计
  - disabled-stage violation 统计
  - prerequisite-ready 到 downstream start 的 waiting gap
  - `Final Results` 到 board non-empty 的 lag
  - monotonic counter regression 与 `manual_review_count` backlog reduction 分口径统计
  - terminal job 后的 recoverable background worker settle，避免 smoke 把“主链 completed”误判成“profile tail completed”
- 目标是让 scripted/simulate 不只回答“有没有跑完”，还能回答：
  - workflow 是否按预期编排
  - 哪些 wall-clock 是有效工作
  - 哪些 wall-clock 只是错误的串行等待
- 下一轮如果继续做，不应再补小规则，而应把这些结构化指标推广到更多 hosted/scripted case 和 CI/high-signal gate。

### 3. Productize asset governance and snapshot promotion

- 第一层 `draft / partial / canonical / superseded / archived / empty` lifecycle gate 已落成可执行规则：
  - `canonical` 可参与 promotion / authoritative baseline
  - `draft / partial / superseded / archived / empty` 可补充资产，但不可自动成为 coverage baseline
- `2026-04-25` 已新增 default pointer / canonical replacement helper：
  - `src/sourcing_agent/asset_governance.py`
  - default pointer key 为 `(company_key, scope_kind, scope_key, asset_kind)`
  - replacement plan 会显式返回新 canonical pointer、旧 superseded pointer、historical retention 摘要
  - 非 canonical lifecycle 不允许成为 default pointer
- 后续要继续做的是治理产品化，而不是再补 promotion 分支：
- company asset / scoped asset default pointer 的持久化 writer 已完成，后续不要再让调用方手工改 `latest_snapshot.json`。
- 下一步应做 governance UI / review flow，以及 promotion-time 自动 stamping `coverage_proof`，而不是恢复旧的隐式 authoritative promotion。
- 继续扩充“可用于排序和分析的长期 roster 资产”，优先考虑：
  - roster 在 Google Scholar / publication surface 上的论文标题、方向、摘要
  - roster 在 X / Substack 上的公开内容资产
  - roster 的公司邮箱、个人主页邮箱、论文中的学术邮箱
  - 从个人主页和论文附录中抽取 email 时的 lineage / evidence 落盘规则

### Excel intake relaunch gate

- 这条 rollout blocker 已收尾：
  - 首页 `Excel Intake` 入口默认重新展示；如需灰度关闭可设 `VITE_ENABLE_EXCEL_INTAKE_WORKFLOW=false`
  - 本地 Vite dev 和 preview 都支持 same-origin `/api/*` proxy
  - 如果本地 same-origin 意外回落到前端 HTML shell，浏览器客户端会自动尝试 `127.0.0.1:8765` / `localhost:8765`
  - `/api/intake/excel/workflow` multipart upload、base64 materialization、多公司拆 job、history/progress 可见已纳入测试
- 后续不再把“能不能上传 Excel”列为 4 月 23 日尾巴；如果继续增强，应作为产品能力迭代：
  - Excel intake 大批量 profile fetch 的吞吐 benchmark
  - 行级人工复核 continuation UI
  - 批量导入完成后的 target candidates / export 联动
- `2026-04-25` 已补上 backend `throughput_plan`：
  - 多公司分组摘要
  - direct LinkedIn fetch batch / worker 计划
  - search-required count
  - row-level continuation support
  - target candidates / export linkage gates
- target candidates / export linkage 已接成真正 UI action：
  - 完成后的 Excel group 可导入目标候选人
  - 完成后的 Excel group 可导出候选人包
- 下一步保留为产品增强：
  - 行级人工复核 continuation UI
  - 大批量吞吐 benchmark 与进度展示

### Runtime isolation hardening

- 第一轮已从 provider-mode guard 升级为 runtime namespace contract：
  - 新增 `SOURCING_RUNTIME_ENVIRONMENT`
  - Harvest shared provider cache 改为 `provider_cache/<runtime_environment>/live/<logical_name>`
  - `simulate/scripted/replay` 不再读写 shared Harvest provider cache，也不走 live_tests bridge
  - `scripts/dev_backend.sh` 在 test/scripted/replay/simulate runtime 下自动写入 isolated local-postgres sentinel，阻断 repo-level PG fallback
  - `scripts/run_hosted_trial_backend.sh` 默认 `production + live + PG-only`，生产非 live provider mode 需要显式 override
- 当前 contract 文档：
  - [RUNTIME_ENVIRONMENT_ISOLATION.md](./RUNTIME_ENVIRONMENT_ISOLATION.md)
- 后续如果继续增强，应聚焦：
  - 为 production/test 分别提供独立 PG database/schema 的自动 bootstrap
  - 为清理脚本增加按 namespace 删除 provider cache / jobs / assets 的 dry-run report

### Harvest profile hydration throughput

- `_fetch_harvest_profiles_for_urls(...)` 的前台 live hydration 这轮已经收成 bounded-window 并行：
  - `live`: 最多 2 个 batch 并发
  - `non-live`: 最多 4 个 batch 并发
- `company_asset_completion` 的后台 live profile completion 这轮也已对齐到同一套 adaptive window：
  - 不再固定 `150 x 1 worker`
  - live 下改为按 URL 总量 + `source_shards_by_url` 的来源混合决定 batch / concurrency
- `blocked full-roster -> resume from partial baseline` 这条 barrier 已完成第一轮收口：
  - 当 `acquire_full_roster` 已经有 search-seed / candidate baseline
  - 且剩余 critical worker 仅是后台 `harvest_company_employees`
  - workflow 现在允许继续进入 enrichment / normalize，而不是继续卡在 blocked
  - `acquire_full_roster` 自身在 former/search-seed baseline ready 时也会直接继续主链，并提前派发 profile prefetch
  - 同时也已收成对称 contract：
    - current roster ready / former-search background -> continue
    - former/search-seed ready / current-roster background -> continue
  - 这解决的是“阶段级机械等待 / recovery tick 等待”，不是最终的 request-level streaming ingest
- `search_seed_snapshot` 现在也不再只作为后续 enrichment 的输入：
  - `acquire_search_seed_pool`
  - `background_search_seed_reconcile`
  - `pre_retrieval_refresh`
  - 这三条路径在 search-seed entries 落地后都会立刻尝试 background profile prefetch
  - 最新一轮又继续前推了一层：
    - `search_seed_acquirer.discover(...)` / `_provider_people_search_fallback(...)` 已支持 query-level incremental callback
    - 单个 query 返回 usable entries 后，就能立即做 job-local dedupe 并派发 profile prefetch
    - overlapping query results 不会在同一 job 里重复派发相同 URL
    - `harvest_company_employees` completed worker/shard output 现在也已并入 shared snapshot apply / reconcile contract
    - running `pre_retrieval_refresh` 与 completed-job `background reconcile` 现都会增量 merge root roster snapshot / candidate documents
    - segmented roster 也允许 partial restore，但 partial roster 不会再解锁 cached reuse
    - 最新这轮又继续把 remaining 两层收进主链：
      - worker recovery completion callback 现在会立即 apply `harvest_company_employees` / `harvest_profile_batch`
      - `harvest_profile_batch` 已具备 shared delta-merge -> same-kind micro-batch sync/materialize
      - completed-job `background_harvest_prefetch_reconcile` 也不再走另一套 `CompanyAssetCompletionManager` 双轨
      - in-process segmented roster 的 completed local shard 也已直接接入同一 inline callback
  - 当前剩下的尾巴不再是“有没有这条 contract”，而是“是否还要继续把它推得更细”：
    - same-kind micro-batch + per-job single-writer 已落地
    - report-level time-window coalescing / writer-budget contract 已补
    - running job inline incremental sync 已接入 shared `materialization_writer` in-flight slot
    - provider batch response-level streaming 已推入 live adapter，enrichment 已消费 callback
    - snapshot candidate-document sync 已纳入 shared `materialization_writer` budget
    - 还没继续做的是：
      - 如果 Harvest API 支持 partial dataset/streaming fetch，再从 batch-response callback 深化到 partial dataset callback
      - 更完整的跨进程 writer queue / backpressure 执行器，覆盖多进程部署下的 writer 竞争
- 这条线的第一轮 shared runtime tuning / smoke report 收口已经完成到：
  - scheduler lane limits / lane budget caps
  - public-web exploration worker parallelism
  - workflow/smoke 级 `workflow_wall_clock_ms`
  - `effective_acquisition_mode` / `dispatch_strategy` 维度的 strategy rollups
  - background prefetch submit 的 bounded-window parity
- 最新 isolated smoke 已落到 `output/scripted_smoke_v4/`，当前可作为这一层的新参考：
  - `openai_reuse`: total 约 `1505.78ms`
  - `google_scoped_cold`: total 约 `5144.26ms`
  - `xai_live_roster`: total 约 `18825.6ms`
  - xAI case 已能稳定观测到：
    - `queued_worker_count=3`
    - `waiting_remote_harvest_count=3`
    - `pending_worker_count=3`
- 但这还不是整条链路的终态：
  - provider-safe backpressure 仍主要依赖 batch/window，而不是更完整的 shared in-flight budget
- 需要进一步把这条链路从“阶段 barrier”升级成真正的流式 contract：
  - search-seed 这一支已经做到“单个 query 返回 -> job-local dedupe -> dispatch profile prefetch”
  - company-roster 这一支目前已做到“completed worker/shard output -> root snapshot apply -> prefetch / same-kind micro-batch sync”
  - 下一轮要继续收的是主 acquisition provider：
    - 当前仍主要靠 worker completion 粒度，不是 provider HTTP response streaming 粒度
    - 如果后续继续做，应再往 provider HTTP response / request-level incremental ingest 前推，而不是再回退到 worker 完结后整轮补课
  - 物化这条线现在已有第一版终态：
  - 单个 scraper batch 返回 -> delta merge
  - 同 kind completed worker 先聚成 micro-batch，再触发一次 shared sync/materialize
  - completed job 也复用同一条 shared delta apply，而不是另一条 legacy completion manager
  - nonblocking `harvest_profile_batch` 现在仍会继续 recovery/reconcile；nonblocking 只表示不阻塞 board readiness，不表示可以跳过 profile tail
  - snapshot sync / inline incremental apply 后会统一失效 asset-population 读缓存，避免 `/results?include_candidates=1` 或候选人看板读取旧 payload
  - `2026-04-25` 已把更细的 coalescing / writer budget 做成 report-level 可执行 contract，并把 running job inline sync 接进 shared writer slot
  - 下一轮若继续做，应接入真实 writer queue / provider HTTP response callback，不该再回退到整轮结束后一次性全量补课
- 运行时预算建议收成可执行约束：
  - per job 只允许 1 个 inline materialize writer
  - 多个近邻 completed worker/batch 先聚合成 micro-batch，再做一次 delta materialization
  - 2c4g hosted 默认应更偏保守地限制 global materialization concurrency
- 下一轮可继续做：
  - 把 in-flight provider budget / backpressure 也接入 shared runtime tuning，而不是只收 batch/window
  - 继续扩 workflow benchmark/report，使搜索 returned / roster returned / fetched profile / board ready 等口径都能统一落到同一报告

### Hosted smoke teardown hardening

- 当前修复已落地：
  - hosted harness teardown 现在会等待 `background-outreach-layering-*` / `background-snapshot-materialization-*`
  - 同时等待 runtime-control / watchdog / shared-recovery / plan-hydration / excel-intake / organization-warmup 等会写 runtime 的后台线程
  - tempdir cleanup 对后台线程抢先删除子文件造成的 transient `ENOENT` 做安全处理，并继续对 `ENOTEMPTY` / `EBUSY` 做有限重试
  - 完整 `tests/test_hosted_workflow_smoke.py` 与全仓 `pytest -q` 已通过
- 后续如果继续增强，应把 shutdown/cancel contract 做成生产 runtime 的显式 API，而不是只靠测试 harness thread-name wait。

### Planning Rule Consolidation

- 当前 `baseline reuse / scoped search / delta_from_snapshot / full_local_asset_reuse / exact_membership` 这整套规则分散在多个 helper 与 fallback 分支里，心智负担和回归风险都偏高，需要单独做一轮收口。
- 目标不是继续堆补丁，而是把“先判 shard / lane coverage，再判 population-default fallback，再产出 dispatch semantics”的顺序收成单一 contract。
- 需要把当前仍偏粗糙的 `selected_snapshot_ids > 1` proxy 继续替换掉：
  - 正确语义不是“multi-snapshot 一律不能 reuse”
  - 而是：
    - `multi-snapshot authoritative aggregate + coverage proof -> can reuse`
    - `multi-snapshot historical union / unclear coverage -> delta`
- 这条规则的第一轮收口已经完成到 `source_snapshot_selection.mode`：
  - `all_history_snapshots` 仍视为历史并集，不解锁 population-default reuse
  - `preferred_snapshot_subset` 可作为显式 aggregate proof 的第一层 proxy
  - 下一轮若继续做，应再把它从“selection mode proxy”升级为真正的 promoted aggregate contract，而不是停留在 mode 字符串
- `2026-04-25` 已升级：
  - `source_snapshot_selection.mode` 不再是 coverage proof
  - `preferred_snapshot_subset` 单独存在时仍会被 directional multi-snapshot block 拦住
  - 只有显式 `coverage_proof` / `promoted_aggregate_coverage` / legacy explicit `coverage_proven` 才能解锁 multi-snapshot authoritative aggregate reuse
  - 下一轮应补的是 promotion-time writer 自动生成并审计 `coverage_proof`，而不是再让 reader 侧猜测
- `exact_membership` 需要保留，但应明确降级为“风险探测 / gap 诊断”而不是对同 authoritative snapshot 的 tiny gap 一票否决。
- 前后端展示也要共用同一套 execution semantics，避免后端已判 `reuse_snapshot_only`，前端仍显示成 `Scoped search + Baseline 复用增量`。
- 对大组织还要进一步收口成两层心智模型，而不是“公司大小硬编码 + 例外表”：
  - `Anthropic` 这类已有公司级 authoritative baseline -> company-level reuse
  - `Google / OpenAI` 这类当前尚无 full-roster authoritative baseline -> family-scoped reuse
  - 已覆盖 family 可 reuse，未覆盖 family 继续 delta，混合 query 则做 `covered families reuse + missing families delta`
- 需要补一组长期 regression fixtures，至少覆盖：
  - OpenAI / Anthropic / Google / Reflection AI
  - full local reuse
  - scoped search with true delta
  - same-snapshot tiny exact-membership gap
  - former/current lane asymmetry
  - history recovery 后的 plan label 一致性
- 这轮已经补上“stale authoritative row 但 newer snapshot 更适合当前 query family”时的 baseline candidate 选择；
  下一轮仍值得继续收的是：
  - 审计剩余是否还有 direct writer 绕过 shared promotion helper，导致 authoritative row 再次滞后
  - 继续把 planner 的 request-scoped best-plan compare 与 organization-asset promotion contract 再做更清晰的分层说明，避免 helper 继续长成“既管 inventory 又管 query scoring”的混合体

## Lower Priority

### 4. Continue service evolution work

- 把当前 hybrid 方向继续推进为：
  - cloud control plane
  - cloud asset plane
  - local acquisition-capable runner
- 先服务化 retrieval-only / asset-backed 请求，再考虑托管 acquisition。

### 4.1 Audit remaining SQLite-shaped type residues in PG write paths

- `acquisition_shard_registry.provider_cap_hit` 这次已经证明：即使主路径切到 PG，某些旧 SQLite `INTEGER 0/1` 语义仍可能藏在真实存量库或 writer payload 里。
- 需要做一轮更系统的 audit：
  - 找出 PG 主表里仍可能从 SQLite 时代继承下来的 `INTEGER-as-BOOLEAN` / text-encoded enum 残留
  - 把归一化收口到 PG contract boundary，而不是分散在 storage caller
  - 为“真实 PG 存量库 + lazy schema ensure”补更多 regression，而不是只测 clean fixture

### 5. Tighten planning_mode and model-assisted planning

- 当前 `heuristic / model_assisted / llm_brief / product_brief_model_assisted` 的职责边界还不够清晰，需要收敛。
- `model_assisted` 至少要稳定产出可验证的结构化 `intent_brief` / planning JSON，而不是只做 request normalize。
- company identity resolve 不应继续过度依赖 heuristic；需要为 model-assisted path 定义更稳的组织解析 contract。
- 需要补一轮针对 team / org / facet / role / keyword 组合 query 的 planning regression cases。
- 需要把 plan review 的自然语言 operator instruction 继续产品化，从当前 CLI helper 演进到真正的会话式 plan editing。

### 6. Batch the DataForSEO async SERP lane

- 当前 DataForSEO Standard Queue 实现仍是单 query -> 单 `task_post`，没有利用一个 POST 最多携带多 task 的能力。
- 需要把 exploration / search seed discovery 的 Google organic lane 改成 batch submit、batch-ready check、incremental task_get。
- 需要把 batch job 的 request payload、task ids、provider attempts、resume checkpoint 统一落盘，避免再靠 payload hash 反推。
- 前台 retrieval 不应被全量 exploration batch 阻塞；默认先交付 baseline 结果，batch SERP 作为后台 enrich lane。

### 7. Extend adaptive Harvest sharding beyond the first Anthropic policy

- 当前 Anthropic 已从静态 `US + Engineering / exclude Engineering` 升级为 live probe-driven adaptive sharding。
- 第一层 live probe 已验证：
  - `US root = 2837`，超过 provider cap
  - `US + Engineering = 1098`
  - `US + exclude Engineering = 1928`
  - 当前可执行 shard set 已收敛为 `Engineering` + `Remaining after Engineering`
- 下一步要继续补：
  - 当某个 branch 本身仍高于 cap 时的二级 splitter
  - location-based overflow policy
  - plan/review 阶段对 shard policy 的更细粒度可编辑能力
  - 不同大组织的 live validation 与耗时统计沉淀

### 8. Continue query guardrail and shorthand UX refinement

- 当前 `intent_rewrite` 已能把 `华人成员 / 泛华人` 解释成 `greater_china_outreach`。
- 下一步不再是“能不能 rewrite”，而是：
  - 让更多 query shorthand 有稳定、可解释、前后端一致的 rewrite contract
  - 让 plan/review/result 页面都能更自然地展示 rewrite 前后语义
  - 让 operator 更容易在 review 阶段调整 rewrite 后的 targeting 意图

### 9. Expand scripted external-provider scenarios

- `SOURCING_EXTERNAL_PROVIDER_MODE=scripted` 已落地，当前可以用 scenario 文件驱动 Harvest / search / model / semantic provider 的低成本行为模拟。
- artifact/materialize 级 benchmark 已补到 `scripts/run_candidate_artifact_benchmark.py`，现在至少可以结构化比较：
  - `payload_build_total`
  - `state_upsert`
  - `generation_register`
  - `finalize_total`
  - optimized vs legacy-like 的 wall-clock speedup
- strict view 在“与 canonical 完全等价”时已经能 alias 复用 canonical serving artifacts，full-build 不再为这类 case 再做一套重复 materialization。
- 下一步不是“有没有 scripted mode”，而是把 scenario 体系继续扩成更接近真实长尾 API 的测试资产：
  - 更多 ready/fetch 分批场景
  - 更丰富的 `429` / `IncompleteRead` / timeout / partial result fixtures
  - 更贴近真实召回质量的 fixture 库
  - 让大组织 workflow regression test 可以直接复用标准 scenario catalog
- provider-grade workflow case report 的第一层 contract 已补到 `workflow_smoke.py`：
  - 单 case 会落 `provider_case_report`
  - 汇总会带 `stage wall-clock` 与 `board_ready` 聚合
  - 当前已能结构化看 `query_count / search_seed_added_entry_count / fetched_profile_count / board ready`
- workflow behavior guardrails 的第一层也已接入 scripted smoke：
  - `duplicate_provider_dispatch`
  - `disabled_stage_violations`
  - `prerequisite_gaps`
  - `final_results_board_consistency`
- 下一步不是从 0 到 1，而是继续把这层 report 向“更像真实 provider benchmark”推进：
  - 为 `scoped search + baseline`、`small-company live roster`、`full local reuse` 建立固定 fixture catalog
  - 继续补 `search returned count / roster returned count / new delta count` 的更精确来源，而不是只依赖当前 summary/reconcile 抽取
  - 把 `materialize / finalize / candidate board ready` 的 wall-clock 继续细拆到更稳定的后端 event/source
  - 把 case report 正式接进回归预算门禁，而不是只在手工 smoke 时查看
- 存储侧仍有一个明确瓶颈没有收完：
  - `candidate_materialization_state` 在 PG full build 下仍是 dominant hotspot，当前 320-row cold full 的 `state_upsert` 仍约 `~7.0s`
  - 这说明单纯把 bulk upsert 从 temp-table 改成 direct-values 不够，下一轮要继续看更深的 PG writer 策略，而不是再做表层 patch
- 补充了一条真实大 snapshot cold build 观测后，瓶颈判断还需要继续更新：
  - Google `20260423T040115`（`5897` candidates）隔离 runtime cold full 约 `14.4min`
  - 这次 `state_upsert` 只有 `~107ms`，`finalize_total` 只有 `~1.2s`
  - 最大热点反而落在 `prepare_candidates` / `payload_build_total` / `view_write_total`
  - 其中：
    - `prepare_candidates` 约 `376s`
    - `payload_build_total` 约 `376.6s`
    - `view_write_total` 约 `381.3s`
  - 所以下一轮优化优先级应继续上提 per-candidate payload/materialization 主路径，而不是默认先打 PG merge
  - 具体动作优先级：
    - 继续拆 `candidate_artifacts.py` 的 `prepare_candidates` 热点，确认哪些步骤仍是逐候选人串行 CPU/JSON 组装
    - 评估 canonical view 与 strict view 是否还能共享更多中间结果，避免 full build 重复计算
    - 给真实大 snapshot cold build 补更细粒度 benchmark/report，避免后续优化只看到总时长、看不到热点迁移
- 目标是让 Google 这类大组织 workflow 可以先在近零外部成本下复现：
  - 长耗时 Harvest pending
  - 分批搜索 ready/fetch
  - 恢复 daemon 接管
  - stage summary 推进
  - preview / retrieval / resume 行为

### 10. Keep workflow explain lightweight on large real runtimes

- `POST /api/workflows/explain` / `cli explain-workflow` 现在已经能稳定返回完整 dry-run 结构，并且 explain-only 回归已独立成 `scripts/run_explain_dry_run_matrix.py`。
- explain 计时现在已经细拆到 `prepare_request` 内部，至少能看到：
  - `llm_normalize_request`
  - `deterministic_signal_supplement`
  - `canonicalize_request_payload`
  - `final_request_materialization`
- 当前剩余的轻量化问题主要不再是“有没有 explain 观测”，而是：
  - 真实 runtime 上 `dispatch_preview` 仍可能因为历史 family match / reuse explanation 较重而接近 `~1s`
  - explain-only dry-run 已经能挡 planning regression，但还没有浏览器级前后端 E2E
- 这仍不是当前 bottleneck，也不应抢在 runner/recovery / execution correctness 之前处理。
- 后续可考虑：
  - explain 输入的轻量缓存
  - reuse-history/materialized family match 结果缓存
  - 对 explain 专用的精简解释视图做单独索引
- 优先级：`low`，仅在主工作流稳定后再做。

### 11. Same-origin `/api/*` ingress

- 这条上线 blocker 已完成：
  - `demo.111874.xyz/api/*` 通过 Pages Functions 代理到 hosted backend
  - hosted 前端继续使用 `VITE_API_BASE_URL=same-origin`
  - 本地 `dev` / `preview` 也对齐 same-origin proxy 语义，避免本地与线上 API 形态分叉
- 后续只保留运维检查项，不再作为功能 TODO：
  - 发布前确认 `/api/runtime/health` 与 `/health` 同域可达
  - 确认 Pages / CDN / 反代层不会缓存动态 API 响应
  - 如果未来迁移到 nginx/caddy 或独立 Worker route，再更新 [CLOUDFLARE_SAME_ORIGIN_PROXY.md](./CLOUDFLARE_SAME_ORIGIN_PROXY.md)

### 12. Finish prod-grade hot-cache governance loop

- 这对应当前存储升级里暂缓的 `7`。
- 当前 hot-cache 已可用，但还没有完全收成 prod 级闭环：
  - 明确的 size budget
  - 更细的 LRU / heat tracking
  - publish 后自动 compaction
  - 更稳的 per-company / per-snapshot retention
  - 更清晰的治理观测指标
- 目标是让 hosted 环境把本地磁盘只当成可治理的 read-through cache，而不是继续积累不可控 runtime 残留。

### 13. Remove the last SQLite compatibility surface

- 这对应当前存储升级里暂缓的 `8`。
- 目标不是再做一轮 bridge，而是把 SQLite 明确压到仅限 dev portability / emergency tooling：
  - candidate / evidence 的剩余兼容 fallback 彻底退出 live 主路径
  - backup / portable / admin 链路改成显式 legacy mode，而不是默认可漂移到 live path
  - 对所有剩余 SQLite-only 命令补清晰的 banner、文档和迁移出口
- 做完后，hosted / ECS 环境的默认心智模型应只剩：
  - Postgres control plane
  - generation/object-storage data plane
  - local hot-cache

### 14. Revisit the old WSL VM only if it still needs to stay alive after the Mac migration

- 当前主优先级已经从“继续在旧虚拟机里修 PG / Cursor / WSL bridge”切换成“把环境迁到新的 Mac 并在那里继续开发”。
- 因此旧 VM 上这些问题先明确降级成 TODO，而不是继续阻塞主线：
  - 历史 `SQL_ASCII` 本地 PG 残留的彻底清理
  - 旧 `.local-postgres` data / extract / backup 目录的回收
  - WSL / Cursor bridge `E_UNEXPECTED` 的专项排障
- 如果迁移完成后旧 VM 只保留为只读备份节点，那就不值得继续为它投入高风险修复。

### 15. Finish the last business-journey test gaps

- 这轮已经把两类高价值缺口收进默认回归：
  - `reuse query -> workflow completed -> follow-up explain`
  - `history_id -> completed results -> frontend history recovery`
- 但还有几条常用用户链路尚未完全自动化：
  - supplement / manual review 写回后，registry 检索与 target candidates 页面联动
  - 轻量本地复用 query 与大组织 live/scoped query 的并发隔离
  - 导出链路（CSV + profile bundle zip）在历史结果页上的稳定性
  - 浏览器端时间展示与跨时区 history 排序的一致性
  - `SearchPage` 剩余的 workflow launch / progress polling orchestration 继续抽离，减少页面层分支密度
- 这些更像“产品级回归矩阵”而不是单一模块测试，后续应继续补成默认 smoke，而不是只保留人工 checklist。

## Resume Checklist

切换账号之后，先做：

1. 阅读 `docs/INDEX.md`
2. 阅读 `../PROGRESS.md`
3. 阅读最近的 canonical asset / validation 文档
4. 检查 provider 和 object storage 配置
5. 再回到这份 `docs/NEXT_TODO.md`

## Useful Commands

```bash
cd "sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli build-company-candidate-artifacts --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli complete-company-assets --company thinkingmachineslab --profile-detail-limit 12 --exploration-limit 2
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-snapshot-bundle --company anthropic --snapshot-id 20260409T080131
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company anthropic
PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json
```
