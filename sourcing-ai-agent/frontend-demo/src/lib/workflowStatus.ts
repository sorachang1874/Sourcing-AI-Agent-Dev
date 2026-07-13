import type { WorkflowRunStatus } from "../types";

export type WorkflowStatusOutcome = "active" | "blocked" | "succeeded" | "failed" | "cancelled";

export interface WorkflowStatusContract {
  status: WorkflowRunStatus;
  terminal: boolean;
  outcome: WorkflowStatusOutcome;
  reason: string;
}

const canonical = (
  status: WorkflowRunStatus,
  terminal: boolean,
  outcome: WorkflowStatusOutcome,
): WorkflowStatusContract => ({ status, terminal, outcome, reason: "canonical_status" });

export const WORKFLOW_STATUS_REGISTRY: Readonly<Record<string, WorkflowStatusContract>> = Object.freeze({
  queued: canonical("queued", false, "active"),
  running: canonical("running", false, "active"),
  blocked: canonical("blocked", false, "blocked"),
  completed: canonical("completed", true, "succeeded"),
  failed: canonical("failed", true, "failed"),
  cancelled: canonical("cancelled", true, "cancelled"),
  canceled: { status: "cancelled", terminal: true, outcome: "cancelled", reason: "status_alias" },
  detached: { status: "cancelled", terminal: true, outcome: "cancelled", reason: "status_alias" },
  superseded: { status: "cancelled", terminal: true, outcome: "cancelled", reason: "status_alias" },
});

export function resolveWorkflowStatus(value: unknown): WorkflowStatusContract {
  const sourceStatus = typeof value === "string" ? value.trim().toLowerCase() : "";
  const registered = WORKFLOW_STATUS_REGISTRY[sourceStatus];
  if (registered) {
    return registered;
  }
  return {
    status: "failed",
    terminal: true,
    outcome: "failed",
    reason: sourceStatus ? "unknown_domain_status" : "missing_domain_status",
  };
}

export function normalizeWorkflowStatus(value: unknown): WorkflowRunStatus {
  return resolveWorkflowStatus(value).status;
}

export function normalizeWorkflowLaunchStatus(value: unknown, matchedJobStatus: unknown): WorkflowRunStatus {
  const launchStatus = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (launchStatus === "reused_completed_job") {
    return "completed";
  }
  if (launchStatus === "joined_existing_job") {
    return normalizeWorkflowStatus(matchedJobStatus);
  }
  return normalizeWorkflowStatus(launchStatus);
}

export function isWorkflowStatusTerminal(value: unknown): boolean {
  return resolveWorkflowStatus(value).terminal;
}

export function isWorkflowStatusCompleted(value: unknown): boolean {
  return resolveWorkflowStatus(value).status === "completed";
}

export function isPlanSubmitPendingStatus(value: unknown): boolean {
  const status = typeof value === "string" ? value.trim().toLowerCase() : "";
  return status === "pending" || status === "queued";
}
