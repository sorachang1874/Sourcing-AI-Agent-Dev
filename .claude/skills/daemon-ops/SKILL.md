---
name: daemon-ops
description: worker daemon 的启动/停止/诊断/队列处置（run-worker-daemon-service、恢复扫描、命令队列 park/cancel、僵尸处置）。当用户要求启动或重启 daemon、查 daemon 日志/自旋/饿死、处置 queued/running 命令，或提到 orphan admission 时使用。
---

# Worker daemon 运维

## 启动（当前 daemon 处于停机状态是操作者决定——先确认再启）

- 产品路径：`scripts/dev_backend.sh`（API + daemon 同起；runtime 根 = DEV_RUNTIME_DIR）。
  独立路径：`PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli run-worker-daemon-service
  --poll-seconds 5`，env 需 `set -a; source .local-postgres.env; set +a` +
  `SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=sourcing_live_tml_path_20260719`。
- **runtime 根必须显式选定**（`runtime/` vs `runtime/test_env_live` 的分裂见
  PROGRESS.md；错误的根曾触发 2026-07-22 热缓存自毁事故——已修 guard，但根仍要选对）。
- orphan admission：alt-ref 不对称已修（2026-07-22），默认 900s 窗口安全；
  `WORKER_RECOVERY_REMOTE_WAIT_ORPHAN_SECONDS=0` 仍可整体禁用。

## 队列处置（2026-07-21 事故的定式）

- 永不对已终局化的付费命令 retry/resume/requeue（= 一个 tick 内重复付费提交）。
- park = `not_before_at` 推到远期（drain 门 `not_before_at <= now_text` 字典序比较）；
  cancel = 直接置 `cancelled`；两者都要行数守卫事务、先展示命令 ID 清单。
- 僵尸（status=running 租约过期）只允许就地置终态，不走 queued/retry_wait、不修租约。
- 日志自旋（tick 无 sleep、starvation marker）→ 先中和队列再重启，顺序不可反。

## References

- 恢复驱动机制：`sourcing-ai-agent/docs/RECOVERY_TAKEOVER_INTENT_DESIGN.md` + workflow-runtime 模块路由
- 2026-07-21 中和台账（命令 ID 全清单）：operator memory `artifacts-intake-20260721/neutralization_20260721.md`
- preflight：`sourcing-ai-agent/docs/RUNTIME_PREFLIGHT.md`
