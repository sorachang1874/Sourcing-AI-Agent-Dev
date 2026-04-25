import {
  approvePlanReview,
  dashboardHasRenderableCandidates,
  type FrontendHistoryRecoveryEnvelope,
  continueWorkflowStage2,
  exportTargetCandidatesArchive,
  startExcelIntakeWorkflow,
  getFrontendHistoryRecovery,
  getDashboard,
  submitPlanEnvelope,
  getRunStatus,
  importTargetCandidatesFromJob,
  peekDashboardCache,
  startWorkflowRun,
} from "./api";
import type { DashboardData, DemoPlan, PlanReviewDecision, RunStatusData } from "../types";

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

export class SourcingBackendClient {
  async planNaturalLanguageSearch(queryText: string, historyId = ""): Promise<NaturalLanguagePlanResult> {
    const trimmed = queryText.trim();
    return submitPlanEnvelope(trimmed, historyId);
  }

  async approvePlan(reviewId: string, plan: DemoPlan | null, decision?: PlanReviewDecision): Promise<void> {
    if (!reviewId) {
      return;
    }
    await approvePlanReview(reviewId, decision, plan?.reviewGate?.editableFields || []);
  }

  async startWorkflowFromReviewedPlan(reviewId: string, historyId = ""): Promise<WorkflowLaunchResult> {
    const { jobId, raw } = await startWorkflowRun(reviewId, historyId);
    const runStatus = await getRunStatus(jobId);
    return {
      jobId,
      runStatus,
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
          status: group.status,
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
    const result = await exportTargetCandidatesArchive({
      jobId,
      historyId,
    });
    return {
      blob: result.blob,
      filename: result.filename,
    };
  }

  async getWorkflowResults(jobId: string): Promise<DashboardData> {
    const cachedDashboard = peekDashboardCache(jobId);
    return getDashboard(jobId, {
      forceRefresh: !dashboardHasRenderableCandidates(cachedDashboard),
    });
  }
}

export const sourcingBackendClient = new SourcingBackendClient();
