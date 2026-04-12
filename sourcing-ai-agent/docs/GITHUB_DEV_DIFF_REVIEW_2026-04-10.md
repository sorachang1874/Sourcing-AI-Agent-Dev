# GitHub Dev Diff Review (2026-04-10)

这份文档记录本地工作区相对 `origin/dev` 的一次审阅结果，目标是给后续 PR 审核一个可读、可复盘的变更摘要。

## 1. 对比基线

对比命令：

```bash
git diff --stat origin/dev
git diff --name-status origin/dev
```

当前本地对 `origin/dev` 的主要差异规模：

- `32` 个已跟踪文件发生改动
- 约 `6709` 行新增 / `301` 行删除（以 `git diff --stat origin/dev` 为准）
- 另有未跟踪新文件（配置、outreach layering、profile registry 相关模块与测试）

## 2. 变更主题分组

### 2.1 Workflow 编排与调度

核心文件：

- `src/sourcing_agent/orchestrator.py`
- `src/sourcing_agent/cli.py`
- `src/sourcing_agent/api.py`
- `src/sourcing_agent/storage.py`

变化方向：

- workflow 非阻塞执行与恢复交互增强
- worker / scheduler / daemon 相关状态可见性增强
- query dispatch（join/reuse）与审计查询能力增强

### 2.2 Acquisition / Search / Harvest 策略

核心文件：

- `src/sourcing_agent/acquisition_strategy.py`
- `src/sourcing_agent/acquisition.py`
- `src/sourcing_agent/seed_discovery.py`
- `src/sourcing_agent/company_shard_planning.py`
- `src/sourcing_agent/harvest_connectors.py`

变化方向：

- large-org keyword-first shard 策略增强
- former 搜索与 paid fallback 行为强化（并集、并发、probe 相关）
- provider cap（2500）与参数治理强化
- filter hints 与 search seed 生成逻辑扩展

### 2.3 Profile 资产复用与 registry

核心文件：

- `src/sourcing_agent/enrichment.py`
- `src/sourcing_agent/candidate_artifacts.py`
- `src/sourcing_agent/company_asset_completion.py`
- `src/sourcing_agent/company_asset_supplement.py`
- `src/sourcing_agent/profile_registry_backfill.py`（新增）
- `src/sourcing_agent/profile_registry_utils.py`（新增）

变化方向：

- profile 抓取前复用/去重能力增强
- 历史资产回填、补全与增量路径加强

### 2.4 Query 解释与分层

核心文件：

- `src/sourcing_agent/domain.py`
- `src/sourcing_agent/planning.py`
- `src/sourcing_agent/plan_review.py`
- `src/sourcing_agent/query_intent_rewrite.py`
- `src/sourcing_agent/outreach_layering.py`（新增）
- `src/sourcing_agent/scoring.py`

变化方向：

- query rewrite / scope disambiguation / plan review gate 强化
- outreach layering 从补充路径向主链路收敛

### 2.5 Frontend Contract 与文档

核心文件：

- `contracts/frontend_api_contract.ts`
- `contracts/frontend_api_contract.schema.json`
- `contracts/frontend_api_adapter.ts`
- `docs/FRONTEND_API_CONTRACT.md`
- `README.md`
- `PROGRESS.md`

变化方向：

- API contract 与 adapter 同步更新
- 文档与交互约定向“可审计、可复用”收敛

## 3. 重点行为变化（面向评审）

1. 大组织（如 Google）路径下，`company-employees` 已强调 keyword shard 与 provider cap 保护。
2. former 路径的 query 生成与 paid fallback 从“单 query 首命中”向“多 query union”演进。
3. workflow 的状态管理与恢复能力增强，但需要重点关注 `running/blocked` 的边界一致性。
4. profile registry/backfill 增强后，理论上重复抓取成本会下降，但需观察回填规则与 URL 归一策略是否稳定。

## 4. 风险与建议核查项

建议在 PR 审核时重点核查：

1. 关键词提取与 query 生成是否会回退到过于泛化的模板词。
2. `former` 与 `current` 的 filter hints 是否被错误复用（尤其是 job title 约束）。
3. workflow 恢复路径是否会产生重复 stage 事件或“看似 running 但无进展”状态。
4. profile registry 回填后，是否仍有跨 snapshot 重复记录导致统计偏差。
5. 新增 contract 字段是否与前端适配器和文档一致。

## 5. 建议的推送前自检

```bash
git status --short
python3 -m py_compile src/sourcing_agent/acquisition_strategy.py
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_humansand_coding_researchers.json
```

如果本机具备测试环境，再补跑：

```bash
python3 -m pytest -q
```

（当前某些环境可能缺少 `pytest`，需在 PR 描述中如实注明。）
