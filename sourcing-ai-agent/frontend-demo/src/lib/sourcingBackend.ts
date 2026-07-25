import {
  approvePlanReview,
  type FrontendHistoryRecoveryEnvelope,
  continueWorkflowStage2,
  exportProjectionCandidatesArchive,
  startExcelIntakeWorkflow,
  getFrontendHistoryRecovery,
  getDashboard,
  getRunProjectionId,
  submitPlanEnvelope,
  getRunStatus,
  importTargetCandidatesFromJob,
  startWorkflowRun,
} from "./api";
import { normalizeWorkflowLaunchStatus, normalizeWorkflowStatus } from "./workflowStatus";
import type {
  CohortLocationSelection,
  CohortSelection,
  DashboardData,
  DemoPlan,
  PlanReviewDecision,
  RunStatusData,
} from "../types";

export interface NaturalLanguagePlanResult {
  plan: DemoPlan | null;
  reviewId: string;
  historyId: string;
  status: string;
  raw: unknown;
  explain: unknown;
}

export interface WorkflowLaunchResult {
  jobId: string;
  runStatus: RunStatusData;
  raw: unknown;
}

export interface ExcelWorkflowLaunchResult {
  batchId: string;
  inputFilename: string;
  totalRowCount: number;
  createdJobCount: number;
  groupCount: number;
  unassignedRowCount: number;
  unassignedRows: Array<{
    rowKey: string;
    name: string;
    company: string;
    title: string;
  }>;
  groups: Array<{
    status: string;
    jobId: string;
    historyId: string;
    queryText: string;
    targetCompany: string;
    rowCount: number;
    sourceCompanies: string[];
    runStatus: RunStatusData | null;
  }>;
  raw: unknown;
}

export type RecoveredHistoryResult = FrontendHistoryRecoveryEnvelope;

function buildLaunchRunStatus(jobId: string, raw: unknown): RunStatusData {
  const payload =
    raw && typeof raw === "object" && !Array.isArray(raw)
      ? (raw as Record<string, unknown>)
      : {};
  const dispatch =
    payload.dispatch && typeof payload.dispatch === "object" && !Array.isArray(payload.dispatch)
      ? (payload.dispatch as Record<string, unknown>)
      : {};
  const rawLaunchStatus = String(payload.status || "").trim().toLowerCase();
  const status = normalizeWorkflowLaunchStatus(payload.status, dispatch.matched_job_status);
  const stage = String(payload.stage || "planning");
  const launchTitle =
    status === "completed"
      ? "Workflow completed"
      : status === "failed"
        ? "Workflow launch failed"
        : status === "cancelled"
          ? "Workflow cancelled"
          : status === "blocked"
            ? "Workflow blocked"
            : status === "running"
              ? "Workflow running"
              : "Workflow queued";
  const launchDetail =
    status === "failed"
      ? rawLaunchStatus === "failed"
        ? "The backend reported that the workflow launch failed."
        : "The workflow launch response had a missing or unknown status."
      : status === "cancelled"
        ? "The workflow is terminal and will not be polled again."
        : status === "completed"
          ? "An existing completed workflow was reused."
          : status === "blocked"
            ? "The workflow is waiting for an explicit continuation action."
            : "Workflow progress polling is starting.";
  return {
    jobId,
    status,
    currentStage: stage === "planning" ? "Workflow" : stage,
    startedAt: "unknown",
    currentMessage: launchTitle,
    awaitingUserAction: "",
    metrics: [
      { label: "总候选人数量", value: "0" },
      { label: "需人工审核候选人", value: "0" },
    ],
    timeline: [
      {
        id: "workflow_launch",
        stage: "planning",
        title: launchTitle,
        detail: launchDetail,
        status,
        startedAt: "",
        completedAt: "",
        sourceTags: [],
      },
    ],
    workers: [],
  };
}

export class SourcingBackendClient {
  async planNaturalLanguageSearch(
    queryText: string,
    historyId = "",
    cohortSelection?: CohortSelection,
    cohortLocations?: CohortLocationSelection,
  ): Promise<NaturalLanguagePlanResult> {
    const trimmed = queryText.trim();
    // Location state is request-owned (review finding 1): the caller passes
    // the presence-aware selection explicitly; the sibling fields ride
    // alongside (never inside) the cohort object on the wire.
    return submitPlanEnvelope(
      trimmed,
      historyId,
      cohortSelection,
      cohortLocations?.targetLocations,
      cohortLocations?.excludeTargetLocations,
    );
  }

  async approvePlan(reviewId: string, plan: DemoPlan | null, decision?: PlanReviewDecision): Promise<void> {
    if (!reviewId) {
      return;
    }
    await approvePlanReview(reviewId, decision, plan?.reviewGate?.editableFields || []);
  }

  async startWorkflowFromReviewedPlan(reviewId: string, historyId = ""): Promise<WorkflowLaunchResult> {
    const { jobId, raw } = await startWorkflowRun(reviewId, historyId);
    return {
      jobId,
      runStatus: buildLaunchRunStatus(jobId, raw),
      raw,
    };
  }

  async startExcelIntakeWorkflow(payload: {
    file: File;
    filename?: string;
    historyId?: string;
    queryText?: string;
  }): Promise<ExcelWorkflowLaunchResult> {
    const launched = await startExcelIntakeWorkflow({
      file: payload.file,
      filename: payload.filename || payload.file.name,
      historyId: payload.historyId,
      queryText: payload.queryText,
      attachToSnapshot: true,
      buildArtifacts: true,
    });
    return {
      batchId: launched.batchId,
      inputFilename: launched.inputFilename,
      totalRowCount: launched.totalRowCount,
      createdJobCount: launched.createdJobCount,
      groupCount: launched.groupCount,
      unassignedRowCount: launched.unassignedRowCount,
      unassignedRows: launched.unassignedRows,
      groups: await Promise.all(
        launched.groups.map(async (group) => ({
          status: normalizeWorkflowStatus(group.status),
          jobId: group.jobId,
          historyId: group.historyId,
          queryText: group.queryText,
          targetCompany: group.targetCompany,
          rowCount: group.rowCount,
          sourceCompanies: group.sourceCompanies,
          runStatus: group.jobId
            ? await getRunStatus(group.jobId).catch(() => null)
            : null,
        })),
      ),
      raw: launched.raw,
    };
  }

  async recoverHistory(historyId: string): Promise<RecoveredHistoryResult> {
    return getFrontendHistoryRecovery(historyId);
  }

  async getWorkflowProgress(jobId: string): Promise<RunStatusData> {
    return getRunStatus(jobId);
  }

  async continueStage2(jobId: string): Promise<RunStatusData> {
    await continueWorkflowStage2(jobId);
    return getRunStatus(jobId);
  }

  async importTargetCandidatesFromJob(jobId: string, historyId = ""): Promise<number> {
    const result = await importTargetCandidatesFromJob({
      jobId,
      historyId,
      followUpStatus: "pending_outreach",
    });
    return result.importedCount;
  }

  async exportTargetCandidatesForJob(jobId: string, historyId = ""): Promise<{ blob: Blob; filename: string }> {
    void historyId;
    const projectionId = await getRunProjectionId(jobId);
    const dashboard = await getDashboard(jobId, { forceRefresh: true });
    const expectedMembershipRevision = String(dashboard.boardRuntimeState?.rowPublicationRevision || "").trim();
    if (!expectedMembershipRevision) {
      throw new Error("Canonical projection membership revision is not ready for export.");
    }
    const result = await exportProjectionCandidatesArchive({
      projectionId,
      expectedMembershipRevision,
    });
    return {
      blob: result.blob,
      filename: result.filename,
    };
  }

  async getWorkflowResults(jobId: string): Promise<DashboardData> {
    return getDashboard(jobId, {
      forceRefresh: true,
    });
  }
}

export const sourcingBackendClient = new SourcingBackendClient();
