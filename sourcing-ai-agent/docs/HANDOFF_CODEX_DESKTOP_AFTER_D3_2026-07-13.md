# Codex Desktop 接管手册：D-3 收口后进入 Track C（2026-07-13）

> Status: Author-complete handoff handbook. D-3 implementation 已固定并完成非 live 作者验收；formal review
> artifact 仍 pending，故尚未取得 live/W6/manual/product/里程碑签收。Track C C1a 尚未开始实施。

## 0. 使用方式

本文是 Codex Desktop Agent 的进场入口，不替代仓库合同和残差台账。接管者应先按第 10 节恢复上下文，
再按第 11 节执行第一批命令。不要只依据聊天摘要继续修改共享合同。

本文区分三种状态：

1. **owner 已决**：产品或架构方向已批准，可以按合同实施。
2. **author evidence**：作者侧测试或本地审计证据，只能证明实现候选，不等于独立 review `GO`。
3. **promotion/signoff**：live、W6/nightly、人工产品验收或里程碑签收，必须等待对应 scope 的有效独立
   review artifact；测试全绿不能替代该 artifact。

## 1. 工程目标与仓库快照

当前目标是把招聘自动化/公开资料补全产品从脚本式、进程内执行逐步重塑为 PG-first、可恢复、可审计的
服务化 Agent。眼前边界是：

- D-3 revised (a) 的 canonical serving projection 合同闭环已固定为 `82d69a1`；继续守住主候选人同步、
  分页、导出、CRM、卡片详情和 profile/capture readiness 的独立 owner，不恢复 fallback 真源。
- R-027 formal review pending 不阻断开发。下一唯一实现批是 Track C C1a 的无存储、无 provider/model
  transport/client 修正。
- Track C C1c-e 的 durable Plan 改造必须等待四张 owner 决策卡，不得猜测 schema、公开响应桥或 freshness。

| 项目 | 2026-07-13 本文观察值 | 接管规则 |
|---|---|---|
| Git root | `/Users/changyuyi/projects/Sourcing AI Agent Dev` | 主代码位于 `sourcing-ai-agent/`；git 路径可能显示 `sourcing-ai-agent/...` |
| 工作目录 | `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent` | Python、Makefile 和前端命令从这里开始 |
| 分支 | `governance-phase0-ttl-20260611` | 不新开/切换分支，除非 owner 明确要求 |
| D-3 提交前并发基线 | `606c124` (`docs: record round-7 state ...`) | Track D 文档已并发前移；禁止 reset 回该值 |
| review runner 修复 | `dc5af51` | 已适配 Codex 0.144 多 thread / non-inline-items transcript |
| D-3 implementation commit | `82d69a1` | 38 个 implementation/contract/test 文件；不含台账、本文和 Track C 设计 |
| Evidence / handbook commit | 本文件所在提交；用 `git log -1 --format=%h -- docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md` 解析 | 自引用提交不能在同一 commit 内写入自身 hash；不要 amend 并发历史 |

接管时若看见 Track D 文档或其他并发提交在 D-3 之前/之后进入历史，视为预期共享工作区状态。不要重排、
amend、squash 或回退不是你创建的提交。

## 2. D-3 Revised (a)：已批准的合同

owner 于 2026-07-13 批准修订后的 D-3(a)，权威文本在
`docs/TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md` §7。必须同时保持以下不变量：

1. exact canonical visible membership 独立拥有 `N`。主候选人同步、canonical 分页、projection-sourced
   export/CRM source total 使用 `N/N`。
2. exact `card_ready` / `display_ready` 独立拥有 `C/N`。基础 roster 行在 exact membership 可分页后即可显示，
   `C=0` 不应隐藏或清空 roster。
3. `profile_ready`、`card_ready`、explicit profile capture、needs-profile-completion、low-richness 都是独立证据。
   不存在 `card_ready <= profile_ready` 的跨维度约束，也不得互相代填。
4. `projection.membership_revision` 是 member-publication UoW 独占写入的 opaque equality token。只能比较相等/
   不等，不能按时间、大小、patch sequence 或 count 排序。
5. summary、page、readiness、export、CRM source 必须绑定同一个 exact revision，并在可能跨多个查询的读取中做
   pre/post fence。缺 token、token 改变、count 非 exact、source 错误或 fallback 被使用都必须 fail closed。
6. 新 exact publication 可以把 `N`、`C` 和各独立聚合向上、向下或归零纠正。禁止 `max(old,new)`、loaded-row
   merge、legacy summary 或 patch count 补数。
7. exact `N=0` 是真实空集合：同步应显示 `0/0`，publication 可为 complete。它必须与 authoritative read
   unavailable 区分；后者不能伪装成 `0/0`。
8. public facet 只由 revision-fenced `projection_person_search_index` 拥有。初始 member publication 的 facet
   状态为 pending/unavailable，不能扫描 members 冒充 exact facet。
9. 前端异步 hydration/polling 必须防止旧 revision 的延迟响应覆盖新 revision；revision 或 count 漂移时应丢弃
   stale batch，重新读取 authoritative summary，并支持 `140 -> 115 -> 0` 的 replacement/clear。

## 3. D-3 当前实现与收口清单

D-3 实现已提交为 `82d69a1`，涉及 serving projection repository/reader、orchestrator/API/CRM/export、
workflow smoke、前端 adapter/cache/hydration、合同文档和回归测试。它是 author-complete、formal-review-pending
的固定候选，不是 live/product 签收版本；接管者应从该提交和本手册继续，不要重复实现。

收口提交必须证明下列路径全部闭合：

- Repository 聚合只在所有可见 member 都有相应显式 `projection_metrics` 证据时发布 optional owner count；
  缺证据保持 unavailable，真实空 membership 才发布 exact zero。
- Reader 在聚合前后验证 membership revision，且不会泄漏旧 persisted readiness 值。
- Candidate page、export member snapshot、CRM source snapshot 在多查询读取前后验证同一 revision；mutation test
  在中途发布 replacement 时必须变红并返回 not-ready/fail-closed。
- Export/CRM 分开暴露 canonical source total `N` 与本次实际导出/选择的 record count，避免 subset/limit 改写
  source denominator。
- Orchestrator 的 exact zero、authoritative unavailable、status text、publication status 与 board phase 语义一致。
- 前端 polling、hydration batch 和 authoritative refresh 都比较请求开始/完成时的 dashboard revision shape，旧请求
  不能覆盖较新的 cache。
- public facets 只在 search-index product 与 membership revision 匹配时 exact-ready。
- 锁序固定为 `operation_dispatch` 后 `serving_projection_publication`。pooled xact contender 使用
  `pg_try_advisory_xact_lock`；busy 时 rollback、归还连接，再 capped backoff，成功连接持锁贯穿同一 UoW。
  真实 pool max=1 **有限竞争** publication/cancel 回归有界完成，且等待者不占住唯一 pooled connection。
  现有 helper 没有总 acquisition deadline，永久锁持有者仍可让 caller 重试；该 operational budget 缺口留在
  R-019，Track C 不得照搬成无界 API/worker loop。
- cancel 等锁后只允许一次 structured、committed、仍非终态 status-advance 重试；terminal winner 或第二次冲突
  fail closed。该重试不取消已计划 command，queued-command/process-crash 因果边界仍由 R-019 跟踪。
- stale-input operation + linked action + event 已进入固定 UoW；projection CRM selection 的三表 UoW 只闭合
  revision TOCTOU/partial domain write，legacy identity writer 与 command completion 边界仍由 R-028 跟踪。
- `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md`、`docs/FRONTEND_API_CONTRACT.md`、
  `docs/WORKFLOW_PROGRESS_CONTRACT.md`、Track B batch record、`NEXT_TODO` 和 `RESIDUAL_LEDGER` 与实现一致。

### 3.1 最终验收记录

下表是 `82d69a1` 与随后 evidence 文档树的最终非 live 作者证据。不要把它解释成 formal review `GO`。

| Gate | 最终证据 |
|---|---|
| D-3 implementation | `82d69a1`；38 files，7,573 insertions / 893 deletions |
| `tests/test_results_api.py` full | **314 passed / 2 failed / 4 skipped** |
| 三个修正后的 results 精确节点 | **3 passed** |
| `tests/test_serving_projection_writer.py` | **24 passed + 3 subtests** |
| `tests/test_control_plane_live_postgres.py` | **62 passed + 4 subtests** |
| `tests/test_operation_runtime.py` | **129 passed** |
| `tests/test_storage_surface_guardrails.py` | **59 passed**；state-sync ratchet 实际 **26** |
| PG projection/CRM contract suites | **58 passed + 4 subtests** |
| `tests/test_export_async_task.py` | **5 passed** |
| 前端 contract/hydration Python suites | **50 passed** |
| `frontend-demo` production build | 通过；80 modules，517.22 kB，仅既有 chunk-size warning |
| `tests/test_workflow_smoke.py` | **187 passed / 1 failed + 3 subtests**；失败为 clean-baseline 同败 R-009 |
| pool max=1 finite-contention regression | helper + concurrent dispatch/publication/cancel 精确复验 **2 passed**；线程均有界退出且单 command/event；永久持锁 deadline 未宣称，见 R-019 |
| `make lint` | **58 files** Ruff/format 全绿 |
| `make typecheck` | **81 errors / 4 files**（1/58/1/21）；R-011 从 87/4 下调后的新棘轮 |
| `make ci-pre-agent-contract` | **349 passed / 0 skipped + 2/11/1/2 passed**；`dry_run_ready failures=[]` |
| baseline worktree 对照 | results 两个 entity-delta collision 在 clean `5f14ed8` **2/2 同败**；workflow 唯一失败 **1/1 同败** |
| Markdown status / diff check | `tests/test_markdown_status.py`: **2 passed**；`git diff --check` 通过 |

全量 results 的两个剩余失败均为 `workflow_entity_deltas ... delta_kind` immutable identity collision；相同两个
精确节点在独立 clean `5f14ed8` worktree 复现。workflow smoke 的一个失败也在该 clean worktree 复现。故验收为
green-modulo-ledger，而不是伪称零失败。Implementation 与 evidence 分提交；后续只按显式路径 stage，禁止
`git add .` 或 amend 并发历史。

## 4. 独立 Review Gate 与 Codex 0.144 Runner

### 4.1 已修复的 transport 问题

旧 runner 假设单 thread，且要求 `turn/completed` 内联 final items。Codex 0.144 app-server 会创建并行 child
threads，并允许 completion 使用 `itemsView=notLoaded`，final item 通过 durable `item/completed` 到达。因此模型
turn 可以全部 `completed, error: null`，旧 runner 仍正确地 fail closed 为 `invalid_transport`。

`dc5af51` 已升级 runner/verifier：验证 root exclusivity、每个 observed turn 的 start/completion 配对、不同 child
thread identity、唯一 durable final、hash-bound transcript/rollout/effective config 和 pinned Git scope。相关 runner
author evidence 为 55 runner tests、50 retention/verifier tests、60 pre-agent tests，另有真实 transcript parity；这只
证明 carrier 修复，不会把旧 invalid artifact 升格为 verdict。

### 4.2 目前仍阻塞正式重发的配置

本文检查到 `~/.codex/config.toml` 的 top-level 配置为：

```toml
model = "gpt-5.6-sol"
model_reasoning_effort = "medium"
service_tier = "priority"
```

`[profiles.openai]` 虽然有 `model_reasoning_effort = "ultra"`，canonical runner 继承的是 top-level operator
配置，不能用 profile 值假装最高 effort 已生效。该文件是 operator-owned：Agent 不应悄悄修改。由 owner/operator
把 top-level effort 设为模型最高支持档后，再从独立、non-author、read-only Codex Desktop session 重发。

R-025、R-026 的旧 artifact 仍是 `invalid_transport`；R-027 已 pin `82d69a1`，但正式 artifact 仍 pending。R-014、
R-015、R-016、R-017、R-018、R-021、R-022 等其他 pending/re-review scope 以
`docs/RESIDUAL_LEDGER.md` 为唯一状态表，不要在本手册复制判决。

### 4.3 并行开发规则

- targeted tests 和 fast contract preflight 先绿，提交/pin scope，记录 review request 后即可继续下一个无关批次。
- pending、timeout、invalid、no-output 或 `NO-GO` 不冻结整个工程；只冻结对应 scope 的 live、W6/nightly、人工
  product signoff 和里程碑签收。
- `NO-GO` 的真实 finding 用 follow-up commit fixed-forward，false positive 写入 artifact/台账。不要为等待 review
  回滚已提交的无关工作。
- 作者 session、子 Agent 静态审计、测试全绿、提取出的 substantive reviewer 文本都不是正式 `GO`。
- 正式 review 必须使用 `docs/INDEPENDENT_REVIEW_GATE.md` 的 canonical runner，并验证 artifact 的
  `reviewer_exit_code=0`、active model/effort/tier、rollout、transcript、scope digest 与最终 GO/NO-GO。

## 5. Provider、凭据与网络硬约束

Apify Token/API Keys 已由 owner 刷新，当前 checkout 不具备 live 验证条件。本阶段一律 simulate/scripted：

- 永远不要设置 `SOURCING_LIVE_PROVIDER_CONFIRM`。
- 永远不要设置 `SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS`。
- 永远不要设置 `SOURCING_EXTERNAL_PROVIDER_MODE=live`。
- 不运行 live provider、live model、W6/nightly 或手工 live gate，除非 owner 在当前 session 明确授权并提供完整
  新凭据/双确认合同。历史授权不能复用。
- 不把测试 mock、缓存命中或 relay 连接成功描述为 live 验证。

GitHub/model backend 失败后先只读运行 `make agent-network-preflight`。不得设置/清除 proxy 环境变量作为自动
workaround，不得修改 git proxy、Clash/Mihomo GLOBAL、TUN、DNS、System Proxy、VPN 或 controller socket。
preflight 若报告 fake-ip、GLOBAL=DIRECT 或 `gh auth` 失败，只报告给 owner。

Claude/Reclaude 需要专用 `reclaude` 容器和 daemon，不适合作为当前 canonical gate。现阶段允许独立 Codex-only
review；模型家族不同是偏好，不是必须条件，session/author separation 才是硬约束。

## 6. Git、测试与本地 PG 纪律

### 6.1 禁止纳入提交的已知杂散路径

以下路径不属于当前交付，不能 stage、删除或“清理”：

- `../.github/workflows/containerized-pre-release.yml`
- `../ai-assisted-engineering-playbook/`
- `configs/scripted/samples/`
- `logs/`

`docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md` 与本文同属本次 evidence/handoff 提交，不是杂散文件；它只批准
C1a/C1b 的接续边界，不代表 C1a 已实施或四张 owner 卡已批准。

### 6.2 测试边界

- Python 使用仓内环境：`PYTHONPATH=src .venv/bin/python -m pytest <suite> -q`。
- 本地 PG 使用 `make local-pg-up`；不要为正常路径增加 SQLite DDL、fallback 或 SQLite-backed test。
- 永不全量运行 `tests/test_pipeline.py`（>90 分钟）。只运行明确节点。
- 新失败先在 `docs/RESIDUAL_LEDGER.md` 查未 closed 行。台账外失败必须用干净 `git worktree` 固定基线对照；
  不使用 stash，不在共享 worktree reset。
- 验收口径为 green-modulo-ledger。基线相同失败要记录 current/base 命令和数字，不要仅写“pre-existing”。
- `make lint` 必须绿；`make typecheck` 当前 R-011 棘轮为 **81 errors / 4 files**。87/4 仅为历史 accepted
  ceiling；今后不得回升或增加模块。
- `make ci-pre-agent-contract` 是最终 fast contract lane；改动后以实际计数为准，不沿用旧 185/321 等数字。

## 7. 已裁决的 Track B 决策

| Card | Owner 决定 | 实施边界 |
|---|---|---|
| D-1 jsonb/timestamptz | 选择 (a)：Track B ② 全域收官后立即排 production migration 窗口 | 只批准排期；实际 ALTER 仍需 scope review GO、readiness、备份/恢复演练和 owner execute GO |
| D-2 on-disk SQLite 工具 | 选择 (b)：立即标记 deprecated/migration-only/no-new-callers/no-maintenance，③ 完成后自动删除 | 它不是 rollback；PG snapshot/export/restore 不在退役范围 |
| D-3 lovable_board 分子 | 批准修订 (a)：exact membership `N/N` 与 card detail `C/N` 分 owner | 本手册第 2-3 节负责收口；不等于 live/W6/manual GO |
| D-4 positional bulk write | 选择 (a)，②.3c 已完成全部 4 点显式 keyword 修复和全局守卫 | 不恢复 shared positional inference |

## 8. Track C 的下一步：先做 C1a

权威草案是 `docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md`。C1a 是无 schema、无 provider/model 的小批，
可以在 D-3 提交并 pin review 后立即开始。因为 C1a 与 D-3 都会触碰 `api.py`、前端 `api.ts`/types/tests，必须
先把 D-3 worktree 稳定提交，再从新 HEAD 做 C1a，避免混合 scope。

### 8.1 C1a Scout 已确认的有界改动

1. `src/sourcing_agent/api.py::_request_priority_lane` 增加**精确** light-lane 路由：
   - `POST /api/projections/export`
   - `POST /api/crm/records/public-web-export`
   - `GET /api/exports/{single-segment-id}`
   - `/artifact` 二进制下载、trailing slash、额外 segment、错误 method 和空 id 仍走 shared lane。不要用宽 prefix。
2. 前端 export transport 拆开 submit 与 wait/download，并 fail closed 使用服务端 `artifact.handle`：
   - 只接受 exact `/api/exports/${encodeURIComponent(taskId)}/artifact`。
   - URL normalization 前拒绝 `.`、`..`、encoded slash/backslash、其他 task id/path。
   - 不再由客户端自行拼一个“看起来合理”的下载 URL。
3. 建立一个前端 workflow status registry，统一 `frontend-demo/src/types.ts`、`lib/api.ts`、
   `lib/sourcingBackend.ts`、`pages/SearchPage.tsx` 和 `components/ExcelWorkflowIntakePanel.tsx`：
   - `queued/running/blocked/completed/failed` 使用明确映射。
   - `cancelled/canceled/detached/superseded` 终止为 cancelled。
   - unknown/missing 终止为 failed，不能映射回 running 后永久 polling。
   - Python `async_task_contract.py` 当前 unknown -> running 是 C1b 必须修的 authority debt；C1a 不偷改 durable
     owner 合同，但应在 batch record 标明这段有意的暂时差异。
4. `SearchPage` 的 Plan submit client 先容忍 top-level `pending | queued`。服务端直接切 top-level 202 属于 C1e，
   受 D-C1-2 约束，C1a 不提前切换。

C1a 要补精确路由正/反例、artifact-handle 攻击输入、terminal-total status mapping 和 `pending|queued` submit
回归；运行 targeted tests、前端 build、lint、typecheck 和 `ci-pre-agent-contract`。批 settle 后 pin commit 并发出
异步 scope review，然后可继续 C1b characterize。

C1b 的 AST guard 先棘轮为“唯一 legacy hydration-thread owner、零新增 direct compile/thread caller”；因为 C1d
仍保持 legacy submit，不能提前伪称零 caller。C1e durable cutover 删除 fallback 后再把同一 guard 收紧到零。

## 9. Track C 四张 Owner 决策卡

下列推荐尚不是批准。C1a/C1b characterization 可继续；C1c schema、publisher 和 public cutover 不得越过卡片。

| Card | 单一问题 | 草案推荐 | 超时默认 |
|---|---|---|---|
| D-C1-1 | Plan public handle/fanout/ownership 的物理模型 | (a) 独立 `plan_task_consumers` relation，history 保持 UI projection | 不做 schema 或 durable submit cutover |
| D-C1-2 | tolerant client 后直接 top-level 202，还是一版 nested bridge | (a) inventory 无独立旧 client 后直接 top-level 202 | 保留当前 200/pending，dark owner 可继续 |
| D-C1-3 | 每 consumer generation 的 review/criteria publication 唯一键/UoW | (a) `(public_task_id, generation, checkpoint_hash)` 级 publication key/constraint 或小型 result-link relation | 不发布 durable Plan result；dark compute only |
| D-C1-4 | completed checkpoint 的 bounded freshness | (a) DB-clock TTL + monotonic generation CAS，初始上限 300 秒 | 已 attach 可收敛；新 consumer 不复用 completed checkpoint |

向 owner 升级时使用 handbook §7 的决策卡格式：单一问题、选项+证据、推荐、截止、超时默认。不要把四张卡
合并成一个模糊的“批准 Track C”问题。

## 10. 新 Session 精确阅读顺序

1. 工作区 `../AGENTS.md` 和仓库 `AGENTS.md`。
2. 本文；确认 D-3 implementation=`82d69a1`、R-027 formal artifact pending、C1a 未开始。
3. `git status --short --branch`、最近提交和当前 diff；确认并发 Track D/D-3/C1 文件归属。
4. `docs/TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md` §4、§7；
   `docs/TRACK_B_PG_PURE_STORE_DESIGN.md` §6 最后一个 batch record。
5. `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md`、`docs/WORKFLOW_PROGRESS_CONTRACT.md`、
   `docs/FRONTEND_API_CONTRACT.md`，再看 D-3 实现与回归测试。
6. `docs/RESIDUAL_LEDGER.md`、`docs/NEXT_TODO.md`、`PROGRESS.md` 的最新条目。
7. `docs/INDEPENDENT_REVIEW_GATE.md` 和 `docs/INDEPENDENT_REVIEW_BRIEF.md`；核对 R-027 和 operator effort。
8. `docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md`，重点 §0、§2、§4、§8、§11-16。
9. C1a 编辑前，机械搜索 `_request_priority_lane`、export artifact handle、workflow status mapper、Plan submit
   response 的所有调用方和测试；共享字段不得只改一个 endpoint/component。

## 11. 第一批命令

所有命令从仓库目录运行。第一组只读，不做网络/Provider 调用：

```bash
cd "/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
git status --short --branch
git log -8 --oneline --decorate
git rev-parse --show-toplevel
git show --stat --oneline 82d69a1
git log -1 --format='%h %s' -- docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md
rg -n 'FINAL_D3_[A-Z]' docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md
git diff --name-only
git diff --cached --name-only
rg -n 'R-027|R-025|R-026' docs/RESIDUAL_LEDGER.md docs/NEXT_TODO.md
rg -n '^(model|model_reasoning_effort|service_tier)|^\[profiles\.openai\]' ~/.codex/config.toml
```

`82d69a1` 应存在且上述占位符搜索应返回零行。若任一条件不成立，先报告历史/文档漂移；不要自行 reset、
重做 D-3 或立即重复全量测试。C1a Scout 从全调用方搜索开始：

```bash
rg -n '_request_priority_lane|/api/exports|artifact\.handle|artifact_handle' src tests frontend-demo contracts
rg -n 'cancelled|canceled|superseded|unknown_domain_status|status.*running' frontend-demo/src tests
rg -n '/api/plan/submit|status.*pending|status.*queued' frontend-demo/src tests contracts
```

C1a 实现后按其 batch record 跑 targeted/API/frontend gate；D-3 源文件没有变化时无需先重跑 11 分钟的 full
results。最终批门仍包括：

```bash
make local-pg-up
PYTHONPATH=src .venv/bin/python -m pytest tests/test_serving_projection_writer.py -q
PYTHONPATH=src .venv/bin/python -m pytest tests/test_frontend_candidate_filters.py tests/test_frontend_candidate_sync_summary.py tests/test_frontend_dashboard_hydration.py -q
PYTHONPATH=src .venv/bin/python -m pytest tests/test_workflow_smoke.py -q
cd frontend-demo
npm run build
cd ..
make lint
make typecheck
make ci-pre-agent-contract
PYTHONPATH=src .venv/bin/python -m pytest tests/test_markdown_status.py -q
```

不要运行 full `tests/test_pipeline.py`。若遇到 baseline 疑似失败，用单独 clean worktree 跑同一精确节点；不要
stash 当前共享 worktree。完成后先显式 stage 列表并复查：

```bash
git add -- <逐个列出的本批文件>
git diff --cached --name-only
git diff --cached --check
```

## 12. 可直接粘贴给 Codex Desktop Agent 的 Prompt

```text
你现在接管 sourcing-ai-agent 的服务化重构。工作区是：
/Users/changyuyi/projects/Sourcing AI Agent Dev
主代码目录：sourcing-ai-agent/
分支：governance-phase0-ttl-20260611

第一步不要改代码。依次阅读：
1. 工作区 AGENTS.md 与 sourcing-ai-agent/AGENTS.md
2. sourcing-ai-agent/docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md
3. 手册 §10 列出的其余合同、台账和 Track C 设计
然后运行手册 §11 第一组只读命令，报告当前 HEAD、dirty files、D-3/evidence commits、FINAL_D3_* 搜索结果、
R-027 状态和 operator 当前 model/effort/tier。不要依据本 prompt 猜测共享工作区现状。

当前固定事实：D-3 revised (a) implementation=`82d69a1`，evidence/handoff commit 用
`git log -1 -- docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md` 解析。作者侧已完成 results 314/2/4
（两失败均 clean 5f14ed8 同败）、writer 24+3、adapter 62+4、operation 129、storage 59、PG contracts 58+4、
frontend Python 50 + build、workflow 187/1 baseline-identical +3、lint 58、mypy 81/4、contract lane
349+2+11+1+2。R-027 formal artifact 仍 pending，所以 D-3 live/W6/manual/product/里程碑签收仍冻结；不要
重复实现 D-3，也不要把作者证据写成正式 GO。

下一唯一实现批是 Track C C1a（手册 §8、Track C design §9）：精确 light-lane route classifier、export
artifact.handle fail-closed、frontend workflow terminal-total status registry、Plan submit pending|queued tolerant。
C1a 无 schema、无 provider/model；先完成全调用方/共享字段 Scout，再同步 backend/frontend/tests/docs，独立提交。
R-027 pending 不阻断 C1a。operator 把 top-level effort 调到模型最高支持档后，可由独立 non-author read-only
Codex session 异步评审 82d69a1；评审等待期间继续无关开发。

硬约束：
- Apify Token/API Keys 已刷新，当前版本禁止 live provider/model 验证。绝不设置
  SOURCING_LIVE_PROVIDER_CONFIRM、SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS，绝不把
  SOURCING_EXTERNAL_PROVIDER_MODE 设为 live，除非 owner 在本 session 明确授权。
- 不修改或自动清除 proxy env、git proxy、Clash/Mihomo、TUN、DNS、System Proxy、VPN。网络错误后只读运行
  make agent-network-preflight。
- 不使用 git add .，不删除/纳入手册 §6.1 的杂散路径，不 reset/revert/amend 其他 Agent 的并发改动。
- Python 用 PYTHONPATH=src .venv/bin/python；PG 用 make local-pg-up。永不全量运行 tests/test_pipeline.py。
- 新失败先查 RESIDUAL_LEDGER；台账外失败用 clean git worktree 跑相同精确节点对照，不 stash。
- mypy 当前棘轮为 81 errors / 4 files，只能下降；87/4 只是历史 ceiling。最终报告实际
  lane/test/build/lint/typecheck 数字。
- canonical Codex runner 已由 dc5af51 适配 0.144，但 ~/.codex/config.toml top-level effort 在 handoff 时仍是
  medium；profile 的 ultra 不算。operator 配成最高支持 effort 前，不要伪造正式 review GO，也不要修改旧
  invalid_transport artifact。

D-3 必须保留的并发边界：锁序 operation_dispatch -> serving_projection_publication；pooled xact lock 竞争用
pg_try_advisory_xact_lock，busy 时 rollback/归还连接后再 capped backoff，成功连接持锁贯穿 UoW；pool max=1
有限竞争 publication/cancel 已证明有界完成。现有 helper 无总 acquisition deadline，永久锁持有者 budget 未闭合；
cancel 只允许一次 committed nonterminal advance retry，terminal/第二次冲突 fail closed。这不解决已计划 command，
上述两项都由 R-019 pending；CRM legacy identity/command completion 由 R-028 pending。

Track C 边界：C1a 无 schema、无 provider/model，可先做；C1c-e 必须等待 D-C1-1..4 owner 决策。现有推荐是
独立 plan_task_consumers、tolerant client 后直接 top-level 202、per-consumer-generation publication key/UoW、
DB-clock TTL 最大 300 秒，但推荐不等于批准。需要裁决时按 handbook §7 的单问题决策卡升级。
Track C repository 必须另加 250 ms monotonic lock-acquire budget、typed retryable `plan_task_lock_busy`、永久持锁
PG 回归；last-consumer cancel 后的新 consumer 必须原子推进并收敛到一个 successor freshness generation。C1b
只守唯一 legacy thread owner，C1e cutover 后才收紧到零 thread/compile caller。

工作方式：先做全调用方/共享字段影响分析，再编辑；每次共享合同修改同步 backend、frontend、worker、API、
tests 和 docs。验证优先于叙述。每批提交后记录精确数字并继续下一批，不因异步 review pending 停住全局开发。
```

## 13. 主 Agent 交付前检查

- [x] `rg -n 'FINAL_D3_[A-Z]' docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md` 返回 0 行。
- [x] D-3 implementation、batch record、R-027 与本手册一致；evidence hash 由本文件 git log 解析。
- [x] 最终测试数字来自稳定实现树，不是中间 evidence。
- [x] 本手册通过最终 `tests/test_markdown_status.py` 和 `git diff --check`。
- [x] stage 计划不含四个杂散路径，也不吞入并发 Track D 文件。
- [x] 正式 review 未 GO 时，表述始终是 pending/invalid，不写“已签收”。
