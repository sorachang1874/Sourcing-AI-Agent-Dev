import { useEffect, useState } from "react";
import { StatusBadge } from "../components/Badges";
import {
  approveOperationAction,
  cancelOperationRun,
  cancelWorkflowCommand,
  dispatchOperationRun,
  getOperationRunProvenance,
  listWorkflowActivities,
  listWorkflowActivityAttempts,
  listWorkflowEntityDeltas,
  listOperationActions,
  listOperationRuns,
  rejectOperationAction,
  resumeOperationRun,
  resumeWorkflowCommand,
  retryOperationRun,
  retryWorkflowCommand,
  type OperationActionRecord,
  type OperationRunProvenance,
  type OperationRunRecord,
  type WorkflowActivityAttemptRecord,
  type WorkflowActivityRecord,
  type WorkflowCommandExecutionSummary,
  type WorkflowEntityDeltaRecord,
} from "../lib/api";

interface CommandDrilldown {
  readonly commandId: string;
  readonly activities: readonly WorkflowActivityRecord[];
  readonly attempts: readonly WorkflowActivityAttemptRecord[];
  readonly deltas: readonly WorkflowEntityDeltaRecord[];
}

function statusLabel(value: string): string {
  return value || "unknown";
}

function statusClassName(value: string): string {
  return statusLabel(value).replace(/[^a-z0-9_-]/gi, "-") || "unknown";
}

function statusText(value: string): string {
  const normalized = value || "unknown";
  const labels: Record<string, string> = {
    approval_required: "待确认",
    approved: "已确认",
    cancelled: "已取消",
    canceled: "已取消",
    completed: "已完成",
    failed: "失败",
    not_required: "无需确认",
    planned: "已计划",
    queued: "排队中",
    rejected: "已拒绝",
    retry_wait: "等待重试",
    failed_terminal: "最终失败",
    blocked: "受阻",
    claimed: "执行中",
    stale: "可能过期",
    skipped: "已跳过",
    running: "执行中",
    succeeded: "已完成",
    unknown: "未知",
  };
  return labels[normalized] || "未知状态";
}

function controlActionText(value: string): string {
  const labels: Record<string, string> = {
    cancel: "取消",
    dispatch: "开始执行",
    resume: "继续",
    retry: "重试",
  };
  return labels[value] || value;
}

function phaseLabel(run: OperationRunRecord): string {
  return run.statusSummary?.operationPhase || String(run.progress.phase || "") || "pending";
}

function commandSummary(run: OperationRunRecord): string {
  const summary = run.statusSummary;
  if (!summary) {
    return "暂无步骤摘要";
  }
  const statusCounts = Object.entries(summary.commandStatusCounts)
    .map(([status, count]) => `${statusText(status)} ${count}`)
    .join(" / ");
  return statusCounts || `执行步骤 ${summary.workflowCommandCount}`;
}

function commandControlSummary(command: { controlState?: Record<string, unknown> }): string {
  const controlState = command.controlState || {};
  const allowedActions = Array.isArray(controlState.allowed_actions)
    ? controlState.allowed_actions.map((item) => String(item)).filter(Boolean)
    : [];
  const disabledReasons =
    controlState.disabled_reasons && typeof controlState.disabled_reasons === "object" && !Array.isArray(controlState.disabled_reasons)
      ? (controlState.disabled_reasons as Record<string, unknown>)
      : {};
  if (!allowedActions.length) {
    if (Object.keys(disabledReasons).length) {
      return `暂无可用操作：${Object.values(disabledReasons).map((item) => String(item)).join(" / ")}`;
    }
    return "暂无可用操作";
  }
  const cancelMode = String(controlState.cancel_mode || "").trim();
  const summaryParts = [
    `可用操作：${allowedActions.map(controlActionText).join(" / ")}${
      cancelMode && allowedActions.includes("cancel") ? ` (${cancelMode})` : ""
    }`,
  ];
  const resumeReason = String(disabledReasons.resume || disabledReasons.running_resume || "").trim();
  if (!allowedActions.includes("resume") && resumeReason) {
    summaryParts.push(`继续受限：${resumeReason}`);
  }
  return summaryParts.join(" · ");
}

function commandControlPolicySummary(command: {
  controlPolicy?: {
    runningControlCategory?: string;
    runningControlCategories?: readonly string[];
    runningControlMaturity?: string;
    runningControlGapStatus?: string;
    fallbackStatus?: string;
  };
}): string {
  const policy = command.controlPolicy;
  if (!policy) {
    return "控制规则暂不可用";
  }
  const category = policy.runningControlCategory || "running_control_category_missing";
  const auditedCategories = (policy.runningControlCategories || []).join(" / ");
  const maturity = policy.runningControlMaturity || "running_control_maturity_missing";
  const gapStatus = policy.runningControlGapStatus || "running_control_gap_status_missing";
  const fallbackStatus = policy.fallbackStatus || "fallback_status_missing";
  return `Control category: ${category}${auditedCategories ? ` (${auditedCategories})` : ""} · ${maturity} · ${gapStatus} · ${fallbackStatus}`;
}

function countSummary(counts: Record<string, number>): string {
  return Object.entries(counts)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([status, count]) => `${statusText(status)} ${count}`)
    .join(" / ");
}

function commandExecutionCounts(summary?: WorkflowCommandExecutionSummary): string {
  if (!summary) {
    return "执行证据暂不可用";
  }
  const activityCounts = countSummary(summary.activityStatusCounts);
  const attemptCounts = countSummary(summary.attemptStatusCounts);
  const deltaCounts = countSummary(summary.entityDeltaStatusCounts);
  const kindCounts = countSummary(summary.entityDeltaKindCounts);
  const parts = [
    `活动记录 ${summary.activityCount}${activityCounts ? ` (${activityCounts})` : ""}`,
    `尝试记录 ${summary.attemptCount}${attemptCounts ? ` (${attemptCounts})` : ""}`,
    `结果变更 ${summary.entityDeltaCount}${deltaCounts ? ` (${deltaCounts})` : ""}`,
  ];
  if (kindCounts) {
    parts.push(`类型 ${kindCounts}`);
  }
  return parts.join(" · ");
}

function recordString(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
    if (typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
  }
  return "";
}

function nestedRecordString(record: Record<string, unknown>, objectKeys: string[], valueKeys: string[]): string {
  for (const objectKey of objectKeys) {
    const nested = record[objectKey];
    if (nested && typeof nested === "object" && !Array.isArray(nested)) {
      const value = recordString(nested as Record<string, unknown>, valueKeys);
      if (value) {
        return value;
      }
    }
  }
  return "";
}

function latestExecutionReason(summary?: WorkflowCommandExecutionSummary): string {
  if (!summary) {
    return "";
  }
  const deltaReason = recordString(summary.latestEntityDelta, [
    "reason",
    "delta_kind",
    "entity_type",
    "status",
  ]);
  if (deltaReason) {
    return deltaReason;
  }
  const attemptReason =
    recordString(summary.latestAttempt, [
      "reason",
      "error",
      "error_message",
      "error_text",
      "status",
      "provider",
    ]) ||
    nestedRecordString(summary.latestAttempt, ["output", "result", "metadata"], [
      "reason",
      "error",
      "error_message",
      "no_op_reason",
      "status",
    ]);
  if (attemptReason) {
    return attemptReason;
  }
  return recordString(summary.latestActivity, ["phase", "status", "activity_type", "owner"]);
}

function latestExecutionEntity(summary?: WorkflowCommandExecutionSummary): string {
  if (!summary) {
    return "";
  }
  const entityType = recordString(summary.latestEntityDelta, ["entity_type", "entityType"]);
  const entityKey = recordString(summary.latestEntityDelta, ["entity_key", "entityKey"]);
  if (entityType && entityKey) {
    return `${entityType}:${entityKey}`;
  }
  return entityType || entityKey;
}

function compactRow(values: string[]): string {
  return values.filter(Boolean).join(" · ");
}

function commandExecutionEffect(summary?: WorkflowCommandExecutionSummary): string {
  if (!summary) {
    return "执行证据暂不可用";
  }
  const parts = [];
  const effectStatus = summary.latestEffectStatus || recordString(summary.latestEntityDelta, ["status"]);
  if (effectStatus) {
    parts.push(`结果 ${effectStatus}`);
  }
  const reason = latestExecutionReason(summary);
  if (reason) {
    parts.push(`原因 ${reason}`);
  }
  const entity = latestExecutionEntity(summary);
  if (entity) {
    parts.push(`对象 ${entity}`);
  }
  if (summary.sampleTruncated) {
    parts.push(`样本已截断 ${summary.sampleLimit || "limit"}`);
  }
  return parts.join(" · ") || "暂无执行证据";
}

function readOnlyEvidenceSummary(record: {
  mutationContract?: string;
  moduleStateMutated?: boolean;
}): string {
  const contract = record.mutationContract || "read_only_contract_missing";
  const mutationState = record.moduleStateMutated ? "异常写入" : "只读证据";
  return `${contract} · ${mutationState}`;
}

function operationControlAllows(run: OperationRunRecord, action: "cancel" | "retry" | "resume" | "dispatch"): boolean {
  return run.controlState?.allowedActions.includes(action) === true;
}

function operationControlReason(run: OperationRunRecord, action: "cancel" | "retry" | "resume" | "dispatch"): string {
  return run.controlState?.disabledReasons[action] || "";
}

function commandControlAllows(
  command: { controlState?: Record<string, unknown> },
  action: "cancel" | "retry" | "resume",
): boolean {
  const allowedActions = Array.isArray(command.controlState?.allowed_actions)
    ? command.controlState.allowed_actions.map((item) => String(item)).filter(Boolean)
    : [];
  return allowedActions.includes(action);
}

function commandControlReason(
  command: { controlState?: Record<string, unknown> },
  action: "cancel" | "retry" | "resume",
): string {
  const disabledReasons = command.controlState?.disabled_reasons;
  if (!disabledReasons || typeof disabledReasons !== "object" || Array.isArray(disabledReasons)) {
    return "";
  }
  return String((disabledReasons as Record<string, unknown>)[action] || "");
}

function commandDisplayLabel(command: { commandType?: string; displayContract?: Record<string, unknown> }): string {
  const displayLabel = String(command.displayContract?.display_label || "").trim();
  return displayLabel || "Display contract missing";
}

function commandDisplayCategory(command: { owner?: string; displayContract?: Record<string, unknown> }): string {
  const category = String(command.displayContract?.display_category || "").trim();
  return category || "display_contract_missing";
}

function commandDisplayDescription(command: { displayContract?: Record<string, unknown> }): string {
  return String(command.displayContract?.description || "").trim();
}

function operationDisplayLabel(operation: { displayContract?: Record<string, unknown> }): string {
  const displayLabel = String(operation.displayContract?.display_label || "").trim();
  return displayLabel || "Display contract missing";
}

function operationDisplayCategory(operation: { ownerModule?: string; displayContract?: Record<string, unknown> }): string {
  const category = String(operation.displayContract?.display_category || "").trim();
  return category || "display_contract_missing";
}

function operationDisplayDescription(operation: { displayContract?: Record<string, unknown> }): string {
  return String(operation.displayContract?.description || "").trim();
}

export function OperationsPage() {
  const [pendingActions, setPendingActions] = useState<readonly OperationActionRecord[]>([]);
  const [runs, setRuns] = useState<readonly OperationRunRecord[]>([]);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [provenance, setProvenance] = useState<OperationRunProvenance | null>(null);
  const [commandDrilldown, setCommandDrilldown] = useState<CommandDrilldown | null>(null);
  const [isLoadingCommandDrilldown, setIsLoadingCommandDrilldown] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [isMutating, setIsMutating] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");

  const refreshRuns = async (nextSelectedRunId = selectedRunId) => {
    const [nextActions, nextRuns] = await Promise.all([
      listOperationActions({ status: "approval_required", limit: 50 }),
      listOperationRuns({ limit: 50, includeStatusSummary: true }),
    ]);
    setPendingActions(nextActions);
    setRuns(nextRuns);
    const resolvedSelectedRunId = nextSelectedRunId || nextRuns[0]?.operationRunId || "";
    setSelectedRunId(resolvedSelectedRunId);
    setCommandDrilldown(null);
    if (resolvedSelectedRunId) {
      setProvenance(await getOperationRunProvenance(resolvedSelectedRunId));
    } else {
      setProvenance(null);
    }
  };

  useEffect(() => {
    let isMounted = true;
    setIsLoading(true);
    refreshRuns()
      .catch((error) => {
        if (isMounted) {
          setErrorMessage(error instanceof Error ? error.message : "任务队列暂不可用。");
        }
      })
      .finally(() => {
        if (isMounted) {
          setIsLoading(false);
        }
      });
    return () => {
      isMounted = false;
    };
    // The initial load should not refetch when selection changes locally.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const selectRun = async (operationRunId: string) => {
    setSelectedRunId(operationRunId);
    setCommandDrilldown(null);
    setErrorMessage("");
    try {
      setProvenance(await getOperationRunProvenance(operationRunId));
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "执行详情暂不可用。");
    }
  };

  const applyControl = async (
    action: "cancel" | "retry" | "resume" | "dispatch",
    operationRunId: string,
  ) => {
    setIsMutating(true);
    setErrorMessage("");
    try {
      let nextRun: OperationRunRecord | null = null;
      if (action === "cancel") {
        nextRun = await cancelOperationRun(operationRunId);
      } else if (action === "retry") {
        nextRun = await retryOperationRun(operationRunId);
      } else if (action === "resume") {
        nextRun = await resumeOperationRun(operationRunId);
      } else {
        nextRun = await dispatchOperationRun(operationRunId);
      }
      await refreshRuns(nextRun?.operationRunId || operationRunId);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : `Operation ${action} failed.`);
    } finally {
      setIsMutating(false);
    }
  };

  const applyCommandControl = async (
    action: "cancel" | "retry" | "resume",
    commandId: string,
  ) => {
    setIsMutating(true);
    setErrorMessage("");
    try {
      if (action === "cancel") {
        await cancelWorkflowCommand(commandId);
      } else if (action === "retry") {
        await retryWorkflowCommand(commandId);
      } else {
        await resumeWorkflowCommand(commandId);
      }
      await refreshRuns(selectedRunId);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : `Command ${action} failed.`);
    } finally {
      setIsMutating(false);
    }
  };

  const loadCommandDrilldown = async (commandId: string) => {
    setIsLoadingCommandDrilldown(true);
    setErrorMessage("");
    try {
      const [activities, attempts, deltas] = await Promise.all([
        listWorkflowActivities({ commandId, limit: 25 }),
        listWorkflowActivityAttempts({ commandId, limit: 25 }),
        listWorkflowEntityDeltas({ commandId, limit: 25 }),
      ]);
      setCommandDrilldown({ commandId, activities, attempts, deltas });
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "执行证据明细暂不可用。");
    } finally {
      setIsLoadingCommandDrilldown(false);
    }
  };

  const decideAction = async (decision: "approve" | "reject", actionId: string) => {
    setIsMutating(true);
    setErrorMessage("");
    try {
      const result =
        decision === "approve"
          ? await approveOperationAction(actionId)
          : await rejectOperationAction(actionId);
      await refreshRuns(result.operationRun?.operationRunId || selectedRunId);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : `Action ${decision} failed.`);
    } finally {
      setIsMutating(false);
    }
  };

  const selectedRun = runs.find((run) => run.operationRunId === selectedRunId) || null;

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">Agent Workbench</p>
          <h2>任务审批与执行</h2>
          <p className="muted">
            集中处理需要确认的高成本或高影响动作，并跟踪已排队任务的执行状态；候选人、公开信息和导出结果由各自模块写入，这里只做审批、控制和审计。
          </p>
        </div>
        <div className="stat-pill-row">
          <StatusBadge label={`${runs.length} 个任务`} />
          {selectedRun ? <StatusBadge label={statusText(selectedRun.status)} /> : null}
        </div>
      </header>

      {errorMessage ? (
        <section className="warning-card error-card">
          <strong>任务队列暂不可用</strong>
          <p>{errorMessage}</p>
        </section>
      ) : null}

      <div className="content-grid two-column">
        <section className="panel">
          <div className="panel-header operation-panel-header">
            <div>
              <h3>待确认操作</h3>
              <p className="muted">导出、付费补全、批量状态调整等动作会先停在这里；确认后才会进入执行队列。</p>
            </div>
            <button
              type="button"
              className="ghost-button operation-refresh-button"
              disabled={isLoading || isMutating}
              onClick={() => void refreshRuns(selectedRunId)}
            >
              刷新队列
            </button>
          </div>

          {pendingActions.length ? (
            <div className="history-list operation-approval-list">
              {pendingActions.map((action) => (
                <article key={action.actionId} className="history-item">
                  <div className="history-select operation-action-card">
                    <div className="history-item-head">
                      <strong>{operationDisplayLabel(action)}</strong>
                      <span className={`phase-pill phase-${statusLabel(action.status)}`}>
                        {statusText(action.approvalStatus || action.status)}
                      </span>
                    </div>
                    <p>{operationDisplayDescription(action) || "该操作需要确认后才会进入执行队列。"}</p>
                    <div className="history-item-foot">
                      <span>{operationDisplayCategory(action)}</span>
                      <span>{statusText(action.approvalStatus || action.status)}</span>
                    </div>
                    <details className="operation-technical-details">
                      <summary>查看技术标识</summary>
                      <p className="muted">{action.actionId}</p>
                      <p className="muted">{action.approvalPolicy || "approval_required"}</p>
                    </details>
                    <div className="action-row">
                      <button
                        type="button"
                        className="ghost-button"
                        disabled={isMutating}
                        onClick={() => void decideAction("approve", action.actionId)}
                      >
                        确认执行
                      </button>
                      <button
                        type="button"
                        className="ghost-button"
                        disabled={isMutating}
                        onClick={() => void decideAction("reject", action.actionId)}
                      >
                        拒绝
                      </button>
                    </div>
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="warning-card">
              <strong>暂无待确认操作</strong>
              <p>当 Agent 或系统提出需要确认的导出、付费补全或批量修改时，会先出现在这里。</p>
            </div>
          )}

          <div className="divider" />

          <div className="panel-header">
            <div>
              <h3>执行队列</h3>
              <p className="muted">选择一个任务查看当前阶段、可用控制和审计记录。</p>
            </div>
          </div>

          {isLoading ? (
            <div className="results-skeleton">
              <div className="skeleton-line short" />
              <div className="skeleton-line" />
              <div className="skeleton-line" />
            </div>
          ) : runs.length ? (
            <div className="history-list">
              {runs.map((run) => (
                <article
                  key={run.operationRunId}
                  className={`history-item${run.operationRunId === selectedRunId ? " active" : ""}`}
                >
                  <button
                    type="button"
                    className="history-select"
                    onClick={() => void selectRun(run.operationRunId)}
                  >
                    <div className="history-item-head">
                      <strong>{operationDisplayLabel(run)}</strong>
                      <span className={`phase-pill phase-${statusLabel(run.status)}`}>{statusText(run.status)}</span>
                    </div>
                    <p>{operationDisplayDescription(run) || "该任务正在等待执行或已进入模块执行链路。"}</p>
                    <div className="history-item-foot">
                      <span>{operationDisplayCategory(run)}</span>
                      <span>{statusText(run.status)}</span>
                      <span>{commandSummary(run)}</span>
                    </div>
                  </button>
                </article>
              ))}
            </div>
          ) : (
            <div className="warning-card">
              <strong>暂无执行任务</strong>
              <p>确认操作或提交 Agent 任务后，执行队列会显示当前进度。</p>
            </div>
          )}
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <h3>执行详情</h3>
              <p className="muted">展示任务从确认到执行的审计记录；只有后端允许的控制按钮才会启用。</p>
            </div>
          </div>

          {selectedRun ? (
            <div className="stack">
              <div className="metric-grid">
                <div className="metric-card">
                  <span className="muted">状态</span>
                  <strong>{statusText(selectedRun.status)}</strong>
                </div>
                <div className="metric-card">
                  <span className="muted">阶段</span>
                  <strong>{phaseLabel(selectedRun)}</strong>
                </div>
                <div className="metric-card">
                  <span className="muted">执行步骤</span>
                  <strong>{selectedRun.statusSummary?.workflowCommandCount ?? 0}</strong>
                </div>
              </div>

              <div className="action-row">
                <button
                  type="button"
                  className="ghost-button"
                  disabled={isMutating || !operationControlAllows(selectedRun, "dispatch")}
                  title={operationControlReason(selectedRun, "dispatch")}
                  onClick={() => void applyControl("dispatch", selectedRun.operationRunId)}
                >
                  开始执行
                </button>
                <button
                  type="button"
                  className="ghost-button"
                  disabled={isMutating || !operationControlAllows(selectedRun, "resume")}
                  title={operationControlReason(selectedRun, "resume")}
                  onClick={() => void applyControl("resume", selectedRun.operationRunId)}
                >
                  继续
                </button>
                <button
                  type="button"
                  className="ghost-button"
                  disabled={isMutating || !operationControlAllows(selectedRun, "retry")}
                  title={operationControlReason(selectedRun, "retry")}
                  onClick={() => void applyControl("retry", selectedRun.operationRunId)}
                >
                  重试
                </button>
                <button
                  type="button"
                  className="ghost-button"
                  disabled={isMutating || !operationControlAllows(selectedRun, "cancel")}
                  title={operationControlReason(selectedRun, "cancel")}
                  onClick={() => void applyControl("cancel", selectedRun.operationRunId)}
                >
                  取消
                </button>
              </div>

              <div className="divider" />

              <div className="timeline">
                {(provenance?.workflowCommands || []).map((command) => (
                  <div key={command.commandId} className="timeline-item">
                    <div className="timeline-dot" />
                    <div>
                      <div className="timeline-row">
                        <strong>{commandDisplayLabel(command)}</strong>
                        <span className={`phase-pill phase-${statusClassName(command.status)}`}>
                          {statusText(command.status)}
                        </span>
                      </div>
                      <p className="muted">
                        {commandDisplayCategory(command)} · {commandExecutionCounts(command.executionSummary)}
                      </p>
                      {commandDisplayDescription(command) ? (
                        <p className="muted">{commandDisplayDescription(command)}</p>
                      ) : null}
                      <p className="muted">{commandExecutionEffect(command.executionSummary)}</p>
                      <p className="muted">{commandControlSummary(command)}</p>
                      <details className="operation-technical-details">
                        <summary>查看技术标识</summary>
                        <p className="muted">{command.commandType}</p>
                        <p className="muted">status: {command.status || "unknown"}</p>
                        <p className="muted">{command.owner || "unknown owner"}</p>
                        <p className="muted">{commandControlPolicySummary(command)}</p>
                      </details>
                      <div className="action-row">
                        <button
                          type="button"
                          className="ghost-button"
                          disabled={isMutating || !commandControlAllows(command, "cancel")}
                          title={commandControlReason(command, "cancel")}
                          onClick={() => void applyCommandControl("cancel", command.commandId)}
                        >
                          取消步骤
                        </button>
                        <button
                          type="button"
                          className="ghost-button"
                          disabled={isMutating || !commandControlAllows(command, "resume")}
                          title={commandControlReason(command, "resume")}
                          onClick={() => void applyCommandControl("resume", command.commandId)}
                        >
                          继续步骤
                        </button>
                        <button
                          type="button"
                          className="ghost-button"
                          disabled={isMutating || !commandControlAllows(command, "retry")}
                          title={commandControlReason(command, "retry")}
                          onClick={() => void applyCommandControl("retry", command.commandId)}
                        >
                          重试步骤
                        </button>
                        <button
                          type="button"
                          className="ghost-button"
                          disabled={isLoadingCommandDrilldown}
                          onClick={() => void loadCommandDrilldown(command.commandId)}
                        >
                          查看执行证据
                        </button>
                      </div>
                    </div>
                  </div>
                ))}
                {provenance && provenance.workflowCommands.length === 0 ? (
                  <div className="warning-card">
                    <strong>暂无执行步骤</strong>
                    <p>该任务尚未生成可执行步骤，或它只是一次只读查询。</p>
                  </div>
                ) : null}
              </div>

              {commandDrilldown ? (
                <div className="warning-card">
                  <strong>执行证据明细</strong>
                  <p className="muted">{commandDrilldown.commandId}</p>
                  <div className="history-item-foot">
                    <span>活动记录 {commandDrilldown.activities.length}</span>
                    <span>尝试记录 {commandDrilldown.attempts.length}</span>
                    <span>结果变更 {commandDrilldown.deltas.length}</span>
                  </div>
                  <div className="divider" />
                  <div className="stack">
                    <div>
                      <strong>活动记录</strong>
                      {commandDrilldown.activities.length ? (
                        commandDrilldown.activities.map((activity) => (
                          <p key={activity.activityRunId} className="muted">
                            {compactRow([
                              activity.activityType,
                              activity.status,
                              activity.phase,
                              activity.owner,
                              activity.activityRunId,
                            ])}
                            {" · "}
                            {readOnlyEvidenceSummary(activity)}
                          </p>
                        ))
                      ) : (
                        <p className="muted">暂无活动记录。</p>
                      )}
                    </div>
                    <div>
                      <strong>尝试记录</strong>
                      {commandDrilldown.attempts.length ? (
                        commandDrilldown.attempts.map((attempt) => (
                          <p key={attempt.attemptId} className="muted">
                            {compactRow([
                              attempt.activityType,
                              attempt.status,
                              attempt.provider,
                              attempt.owner,
                              attempt.attemptId,
                            ])}
                            {" · "}
                            {readOnlyEvidenceSummary(attempt)}
                          </p>
                        ))
                      ) : (
                        <p className="muted">暂无尝试记录。</p>
                      )}
                    </div>
                    <div>
                      <strong>结果变更</strong>
                      {commandDrilldown.deltas.length ? (
                        commandDrilldown.deltas.map((delta) => (
                          <p key={delta.deltaId} className="muted">
                            {compactRow([
                              delta.entityType,
                              delta.deltaKind,
                              delta.status,
                              delta.reason,
                              delta.entityKey,
                              delta.deltaId,
                            ])}
                            {" · "}
                            {readOnlyEvidenceSummary(delta)}
                          </p>
                        ))
                      ) : (
                        <p className="muted">暂无结果变更。</p>
                      )}
                    </div>
                  </div>
                </div>
              ) : null}
            </div>
          ) : (
            <div className="warning-card">
              <strong>请选择任务</strong>
              <p>从左侧执行队列选择任务后，会显示阶段、可用控制和审计记录。</p>
            </div>
          )}
        </section>
      </div>
    </section>
  );
}
