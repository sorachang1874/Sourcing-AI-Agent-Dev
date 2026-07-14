# Codex Desktop 接管手册：Track C / Track D 独立 Session（2026-07-14）

> Status: handoff snapshot. 本文把 `sourcing-ai-agent` 从当前 X-First session 中拆出；接管 session 只负责
> `sourcing-ai-agent/`。不得修改、暂存、提交或回退 `x-first-researcher-sourcing/` 的并发工作。

## 0. 接管目标

接管者先完成 Track C C2.8 的单 finding fixed-forward，再按 Track D 权威设计恢复 D1 后续工作。不要重复实现
C2.7 已闭合部分，也不要把 scope-local advisory review 写成 formal `GO`。

状态含义保持三分：

1. **author evidence**：实现作者的测试证据；
2. **scope-local advisory review**：独立非作者对固定提交的工程复审；
3. **formal review artifact**：canonical runner 产物，才可用于 live/W6/manual/product/milestone signoff。

前两者都不能替代第三者。Pending review 只冻结对应 scope 的签收，不阻断无关 non-live 开发。

## 1. 固定快照

| 项目 | 固定值 / 接管规则 |
| --- | --- |
| Git root | `/Users/changyuyi/projects/Sourcing AI Agent Dev` |
| 主代码目录 | `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent` |
| 分支 | `governance-phase0-ttl-20260611` |
| D-3 implementation | `82d69a1`；不得重复实现，R-027 formal review pending |
| D-3 evidence / 旧 handoff | `f7a642b` |
| C2.7 第一版 | `2624897`；独立复审 `NO-GO`，已由后续提交 fixed-forward |
| C2.7 当前候选 | `4b2370aadfeaf113b28317bae1f7e918fdfb3b3b`；author-complete，但 pinned review 仍 `NO-GO` |
| D1b 当前候选 | `97a81d04db579ef9edb1914d8567ef77af2b07c7`；scope-local advisory `GO`，formal pending |
| 当前 mypy 棘轮 | `81 errors / 4 files`；只能下降 |
| 当前 top-level Codex 配置快照 | `model=gpt-5.6-sol`、`model_reasoning_effort=medium`、`service_tier=default`；operator-owned，不自行修改 |

共享分支在这些提交之间包含 X-First 提交是预期状态。不要 reset、revert、amend、squash 或重排它们；只按
`sourcing-ai-agent/...` 精确路径 stage。

已知不属于本交付、不得删除或纳入提交的路径：

- `.github/workflows/containerized-pre-release.yml`
- `ai-assisted-engineering-playbook/`
- `sourcing-ai-agent/configs/scripted/samples/`
- `sourcing-ai-agent/logs/`

另一个活跃 session 可能在 `x-first-researcher-sourcing/` 留下 tracked dirty files。它们不构成本 session 的失败，
但也绝不能被 stage、格式化或回退。

## 2. 进场阅读顺序

第一步不改代码，依次读：

1. `/AGENTS.md` 与 `/sourcing-ai-agent/AGENTS.md`；
2. 本文；
3. `docs/TRACK_C_C2_6_AUTH_OWNER_FENCING_IMPLEMENTATION.md`；
4. `docs/TRACK_C_C2_7_CONDITIONAL_OWNER_CLOSURE_IMPLEMENTATION.md`；
5. `docs/NEXT_TODO.md` 的 Track C / Track D 段；
6. `docs/RESIDUAL_LEDGER.md` 的 R-019、R-023、R-027、R-028；
7. Track D 继续前再读：
   - `docs/TRACK_D_AGENT_RUNTIME_PLAN.md` §5a 与 §6；
   - `docs/DESIGN_INVARIANT_CHECKLIST.md`；
   - `docs/TRACK_D_D1A_ACTION_REQUEST_SURFACE_CHARACTERIZATION.md`；
   - `docs/TRACK_D_D1B_DISPATCH_ADAPTER_REGISTRY_IMPLEMENTATION.md`。

旧的 `docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md` 仍是 D-3/C1 的历史合同入口；本文只覆盖本次
C2.7/D1b 后的新增状态，二者冲突时以较新的固定提交、`NEXT_TODO` 与 `RESIDUAL_LEDGER` 为准。

## 3. 第一批只读核验

从 Git root 执行：

```bash
git branch --show-current
git rev-parse HEAD
git status --short
git cat-file -e 4b2370aadfeaf113b28317bae1f7e918fdfb3b3b^{commit}
git cat-file -e 97a81d04db579ef9edb1914d8567ef77af2b07c7^{commit}
git show --stat --oneline 4b2370a
git show --stat --oneline 97a81d0
rg -n '^(model|model_reasoning_effort|service_tier)\s*=' ~/.codex/config.toml
```

报告 HEAD、dirty files、两个提交是否存在、R-027/R-028 状态，以及 top-level model/effort/tier。不要因
`x-first-researcher-sourcing/` 的并发 dirty 而 stash/reset。

## 4. Track C 当前证据

### 4.1 `2624897` 的复审 findings（已在 `4b2370a` fixed-forward）

`2624897` 的非作者复审发现：

- 客户端可控 `promotion_id` 可跨 workspace 覆盖；
- criteria 三路可在 owner denial 前先写；
- owner-only CAS 可覆盖 terminal job；
- CRM stale version 被错误映射 HTTP 400。

`4b2370a` 已实现：

- promotion 固定 `crm_record row -> schema-scoped promotion-id advisory -> promotion row` 锁序；only exact replay，
  cross workspace/record/signal/action/payload drift 为 HTTP 409；
- Stage2/cancel 使用 PG row-lock typed CAS：`applied | owner_miss | state_conflict`；
- terminal winner 不会被旧快照复活/改写，授权后的 conflict 使用业务状态投影；
- CRM stale version 与 promotion idempotency conflict 均为 HTTP 409；
- criteria 显式 rerun/source baseline 在写前做 owner preflight。

作者证据：

- fast + transport：`109 passed + 65 subtests`；transport 单独 `15 passed`；
- PG adapter + owner regressions：`71 passed + 4 subtests`；focused PG `9 passed`；
- CRM adjacency：`2 passed`；CRM runtime boundary：`34 passed`；
- 精确 pipeline 六节点：`4 passed / 2 failed`；相同两个缺表 setup failure 在 clean `97a81d0` 同节点复现；
- `make lint` 绿；mypy `81 / 4`；`py_compile` 与 diff check 绿；
- 未运行完整 `tests/test_pipeline.py`，未调用 provider/model/live。

### 4.2 `4b2370a` 的当前唯一 blocking finding

Pinned review：`NO-GO`，P0/P1/P2/P3=`0/1/0/0`。

`_preflight_criteria_rerun_job_ownership` 在 `rerun_retrieval` 缺省或 false 时直接 ready，导致 authenticated：

- feedback 可把 foreign `job_id` 写入 `criteria_feedback`；
- recompile 可针对 foreign `job_id/baseline_job_id` 写 version/compiler；
- suggestion review 可修改由 foreign source job 派生的 suggestion。

复现实证没有 owner `get_job` read，却已产生 feedback/review write。现有新回归只覆盖
`rerun_retrieval=true`，而 C2.7 文档错误声称所有 caller-supplied refs 都在主域写前预检。

因此 `4b2370a` 不可用于本 scope live/manual/product/milestone signoff。

## 5. 下一唯一实现批：C2.8 criteria owner preflight totality

先 Scout 全部调用方和字段，再实施；不要顺手重开其他 C2.7 代码。

实现合同：

1. `rerun_retrieval` 只决定 mutation 后是否 rerun，绝不决定 authorization。
2. authenticated 请求只要携带显式 `job_id`、`baseline_job_id`，或 suggestion 指向 source job，就必须在
   feedback/review/version/compiler/result/derived-job 任何写入前验证 exact requester+tenant owner。
3. missing 与 foreign 使用同一个 canonical `job_not_found`，并且零主域写、零 derived job。
4. 没有任何 job/source ref 的合法 criteria-only 请求保持原合同；open-mode operator 兼容面不得被误收紧。
5. automatic baseline 继续使用 requester+tenant scoped selector；最终执行前仍 re-read owner。
6. 文档不得把 author evidence 或本地 review 写成 formal `GO`。

必须新增至少以下回归矩阵：

- feedback：`rerun_retrieval` missing / false × foreign / missing job；
- explicit recompile：missing / false × foreign / missing `job_id|baseline_job_id`；
- suggestion review：missing / false × foreign/missing source job；
- 每格断言 owner read 发生且 feedback/review/version/compiler/result/derived-job 写集合为空；
- 同 owner 正向、无 job ref 正向、open-mode compatibility 正向。

建议精确验证：

```bash
cd '/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent'
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_request_scope_owner_fencing.py \
  tests/test_api_request_scope.py \
  tests/test_api_transport_parity.py
make local-pg-up
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_request_scope_owner_fencing_pg.py
make lint
make typecheck  # expected nonzero only at the recorded 81 errors / 4 files ceiling
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/sourcing_agent/api.py \
  src/sourcing_agent/orchestrator.py \
  src/sourcing_agent/storage.py \
  src/sourcing_agent/control_plane_live_postgres.py
git diff --check -- \
  sourcing-ai-agent/src/sourcing_agent \
  sourcing-ai-agent/tests \
  sourcing-ai-agent/docs
```

不要全量运行 `tests/test_pipeline.py`。若新增失败不在台账中，使用 clean worktree 跑相同精确节点对照；禁止 stash。

完成 C2.8 后：精确 stage、独立提交、发起 fresh non-author pinned review。Review pending 不阻断无关 non-live
Track D Scout，但 `NO-GO` 继续冻结 C2.8 scope 签收。

## 6. Track D 状态与接续边界

`97a81d0` 的 D1b fifth false-green fixed-forward 只改 D1b test/doc，不改 production dispatch。它把运行时 raw
class method `CodeType` 与从磁盘 `orchestrator.py` fresh `compile`（never exec）的 code fingerprint 绑定，同时保留
AST shape/dependency 与 15-action registry mutation 行为证明。

Scope-local non-author review：`GO`，P0-P3=0；证据：

- D1a+D1b `29 passed`；
- adjacent PG `15 passed`；
- command specs `16 passed`；
- Ruff/format、focused mypy、diff check 绿。

这仍不是 canonical formal artifact。D1b 只完成 explicit dispatch adapter prerequisite，served Agent tool population
仍为零。后续 D1 仍需 owner-bound target source、versioned request schema/physical residual ledger、immutable
schema version/digest copy/verify、revisioned model-safe result schema、full served predicate 与 simulate serializer
preflight。

C2.8 提交并发出 review 后，Track D 首步只能按第 2 节阅读顺序重新核对 Plan §6/OB-ID，Scout 全调用方并写出
下一有界 batch；不要仅凭本文猜 schema、served predicate 或 durable owner，也不要越过未决 owner cards。

## 7. 硬约束

- sourcing session 不得修改 `x-first-researcher-sourcing/`。
- 禁止 `git add .`；只 stage 精确文件。
- 不 reset/revert/amend 其他 Agent 或 X-First 提交。
- 绝不设置三个 sourcing live 环境变量，绝不调用 live provider/model/W6/nightly。
- 凭据目录 `/Users/changyuyi/projects/temporary/sourcing-ai-agent-input` 不属于本批；不要读取、复制或提交。
- 网络错误只读运行 `make agent-network-preflight`；不得改 proxy/VPN/Clash/Mihomo/TUN/DNS/System Proxy。
- Python 使用 `PYTHONPATH=src .venv/bin/python`；PG 使用 `make local-pg-up`。
- 永不全量运行 `tests/test_pipeline.py`。
- formal review 仍使用 `docs/INDEPENDENT_REVIEW_GATE.md` canonical runner；operator 配置不得由 Agent 暗改。

## 8. 交付与下一次 handoff

每个批次记录：固定 commit、exact files、targeted/PG/transport/lint/mypy 数字、baseline 对照、review verdict 与
blocked-vs-unblocked scope。验证优先于叙述；不要用“全绿”代替精确计数。

配套的可复制启动提示位于：
`docs/PROMPT_CODEX_DESKTOP_CONTINUE_TRACK_C_D_2026-07-14.md`。
