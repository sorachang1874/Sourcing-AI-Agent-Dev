# Test Provenance Contract v1

> Status: ACTIVE governance contract（harness R0 批,operator 裁决 2026-07-22）。Gate = `tests/test_provenance.py`（backend-ci docs-gate 块）;扫描器 = `scripts/check_test_provenance.py`;祖父基线 = `tests/provenance_baseline.py`（只减不增）;退役墓碑 = `docs/governance/REGRESSION_INDEX.md` §Tombstones。

```
status: active          owner: operator
canonical-path: sourcing-ai-agent/docs/governance/TEST_PROVENANCE.md
ratified: 2026-07-22
last-verified: 2026-07-22
```

## 背景

普查（2026-07-22 @69b7423）:252 个测试文件中仅 6% 携带任何溯源标记;2026-07 新写的事故回归测试 100% 可追溯,旧文件 60% 完全无法追溯其存在理由——踩坑变成了测试但 WHY 没被记录,现在难以裁决保留/退役。本契约把 2026-07 已被验证的 docstring 风格（`test_pg_onconflict_guard.py`、`test_recovery_remote_wait_orphan.py`、`test_artifact_cache.py`）固化为门禁,只约束**新文件与主动变更**,绝不批量补写。

## 标准（v1）

每个**新建**测试模块,以及每个**离开祖父基线**的既有模块,模块 docstring 必须包含:

1. **一句 Scope**:本模块拥有哪个不变量/合同面;
2. **至少一个机器可查的溯源锚点**（正则见扫描器）:
   - 日期事件 token `YYYY-MM-DD`（事故/决策日）;
   - 残差台账 id `R-\d{3}`;
   - 文档路径 `docs/....md`;
   - 里程碑/阶段 id（`C2.1`、`D1n`、`S1e2b`、`FT2`、`M2`、`Phase 4`、`Track A-D`、`harness reorg R5` 等）;
3. **事故型回归**另需一句因果链:观察到的失败 → 机制 → 本断言为何能防复发（`test_recovery_remote_wait_orphan.py` 风格）。

放宽:>30 测试或 >2,000 行的文件可在**顶层类 docstring** 粒度满足 2/3,前提是模块 docstring 路由到各类。溯源确实不可考时,诚实写 `provenance unknown; characterization adopted YYYY-MM-DD`——**诚实的缺失优于编造的历史**;禁止任何批量后补(post-hoc rationale laundering)。

## 机制

- **祖父基线**:`tests/provenance_baseline.py` 冻结生成时点的不合规文件清单与 `MAX_GRANDFATHERED` 字面量。基线只能缩小;某祖父文件补齐 docstring 后,同一变更内必须将其移出基线（与 mypy 棘轮同纪律）。
- **删除即墓碑**:基线内文件从磁盘消失而 `REGRESSION_INDEX.md` §Tombstones 无对应行 → gate 红。墓碑行 schema:`id | test 文件/家族 | 保护过什么 | 退役原因（合同退役/被X取代/产品行为变更） | 日期 | superseded_by`。
- **强制路径**:gate 只在 pytest 内做无 git 的检查;变更文件级强制（touched-grandfathered-file 必须出基线）依赖评审惯例与后续 CI changed-file step（fast-follow,见 master plan WS3+WS5）。

## 路由

- 测试在哪跑/为什么在 lane 里 → `docs/governance/REGRESSION_INDEX.md`（lane -k 选择的 WHY 首次成文于 `tests/lane_manifest.py`）。
- 已接受失败/运行约束（如 R-009 永不全量跑 test_pipeline.py）→ `docs/RESIDUAL_LEDGER.md`。
- 机器侧 changed-path → 套件映射 → `src/sourcing_agent/regression_matrix.py`（reason 标签为准,本索引只引用不复述）。
