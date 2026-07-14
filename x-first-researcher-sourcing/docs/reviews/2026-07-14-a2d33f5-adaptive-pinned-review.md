# Adaptive Grok pinned adversarial review

## Evidence

- Verdict: `NO-GO`
- Reviewer: `/root/precommit_adaptive_recall_audit`，非作者、只读。
- Base: `dccb61f19cddb756461de06beb029641349017aa`
- Reviewed: `a2d33f5444df68ecaad8e28a8a2c3b475dab321f`
- Tree: `e29805ec995d4514b405031ae0dfc603cc434a99`
- Binary diff SHA-256: `7919cb370d90006619395be62745a5b1162aa7dfba4556d81a739467b40751e1`
- Diff: 9 files，1,022 insertions，126 deletions。
- Detached worktree: `/private/tmp/x-first-adaptive-review-a2d33f`，始终 clean。
- 未读取后续 dirty query/Stage2；未访问网络、Grok、X、provider 或真实凭据。所有 live 路径验证均为本地 synthetic fake-live。

## Validation

- Python 3.12.13 focused: 43/43
- Python 3.14.4 focused: 43/43
- Python 3.12.13 full: 309/309
- Python 3.14.4 full: 309/309
- 双 Python `x_first.contracts`: valid
- Ruff 0.15.11 check 与 format: passed
- 双 Python compileall: passed
- 5 个变更 JSON/schema/config: parsed
- `git diff --check`: clean
- Production registry 的 7 个 OpenAI prompt hash、统一 target tuple 均精确匹配；production `fixture_only` 行无法签发 live grant。

Severity: `P0=0 / P1=5 / P2=1`

## Findings

### P1-1 — 实际 native-X operand 的保护边界仍可绕过

Adaptive transcript 只调用共享的单一 subject predicate；该 predicate 只选择一个 `query` 字段，且 v2 词表缺少常见保护属性表达。

完整 fake-live 复现接受了 `autistic`、`LGBTQ`、`wheelchair user`，以及 `{query: "OpenAI pretraining", exclude: "women researchers"}`，并生成 verified session proof 与空 bundle errors。`China professional experience` 作为正向边界仍按预期允许。

需要一个共享 owner 同时关闭工具参数 shape、所有文本 operand 和受控保护值，而不是只扫描一个选定字段。

### P1-2 — run-root 自身的持久化失败仍会产生 pre-intent orphan

`_create_run_root` 在 `mkdir` 后执行 fsync，但清理 context 要到调用返回后才建立。注入 runtime-root fsync 失败后留下无 intent 空目录，purge 报 `run_lease_open_failed`。

清理必须位于 `_create_run_root` 内部，使所有 intent 前磁盘失败都不会毒化全局 purge。

### P1-3 — Child-writable session tree 可暴露或永久保留 OAuth

Session scanner 校验类型、uid、link，却不校验目录 `0700` 和普通文件 `0600`；删除使用裸 `shutil.rmtree`。

Child 将 home 改为 `0755`、auth 改为 `0644` 时，run 仍为 completed/verified。将 home 改为 `000` 时，run 与 recovery 都抛 `PermissionError`，home/auth 永久残留。

需要运行中和最终扫描的严格 owner-only mode 校验，以及不跟随 symlink、能够修复限制权限的 fd-safe 删除。Mode `000` 不得阻断 auth 删除或 recovery。

### P1-4 — 完整 timeout 被错误归类为 session-tree technical limit

Executor 超时后，post-executor measurement 重用已过期执行 deadline，terminal precedence 又优先选择 technical limit。

真实 sleeper 得到 `status=technical_limit_exceeded`、`timed_out=true`、`technical_limit_kind=session_tree_scan_deadline`，完整 bundle validator 仍接受。清理扫描应有独立、有限预算，timeout 保留终态所有权，validator 应拒绝矛盾组合。

### P1-5 — Prompt-policy 正常扩展会使历史 bundle 和 TTL purge 永久失效

Bundle replay 使用当前整个 policy 文件 SHA，而非执行时的不可变 entry snapshot。向 registry 增加无关 lab 行后，历史 bundle 出现 `command_binding_request_replay_mismatch` 与 `grant_replay_invalid`，purge fail-closed 并永久保留 run。

应绑定可长期重放的 entry-scoped semantic snapshot，或等价的 append-only version owner；历史删除不能依赖当前 registry 全文件 bytes。

### P2-1 — Depth overflow receipt 与公开 JSON Schema 不一致

Scanner 先记录 depth 65 再触发 64 上限；runtime receipt validator 没有对应上界，但公开 schema 拒绝 65。应统一 overflow 表示、schema/runtime 校验，并增加 artifact schema parity 回归。

## Confirmed closures

- Grant、target、prompt policy 在 prompt 读取和 run-root 创建前完成预检。
- Production registry 精确拥有 7 个 prompt；fixture 行不能获得 live authority。
- 普通 executor exception、raw publication failure、structured parse crash 均立即删除 auth，随后可恢复为 `crash_recovered`。
- Directory、entry、unexpected socket、depth 和 traversal deadline 的基本检测已实现。
- Synthetic timeout 的进程组能够 TERM/KILL 并确认死亡；阻塞点是终态分类而非本次复现中的进程泄漏。

## Promotion boundary

此结论仅覆盖 adaptive slice。离线 fixture、修复及无关开发可继续；该固定提交不能用于真实 Grok/X 执行、provider-costing validation、campaign admission、产品/外联写入或里程碑签收。

`NO-GO`
