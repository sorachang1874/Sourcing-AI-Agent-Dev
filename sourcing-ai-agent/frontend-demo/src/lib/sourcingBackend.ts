import {
  approvePlanReview,
  getDashboard,
  getPlanEnvelope,
  getRunStatus,
  startWorkflowRun,
} from "./api";
import type { DashboardData, DemoPlan, RunStatusData } from "../types";

export interface NaturalLanguagePlanResult {
  plan: DemoPlan;
  reviewId: string;
  raw: unknown;
}

export interface WorkflowLaunchResult {
  jobId: string;
  runStatus: RunStatusData;
}

export class SourcingBackendClient {
  async planNaturalLanguageSearch(queryText: string): Promise<NaturalLanguagePlanResult> {
    const trimmed = queryText.trim();
    return getPlanEnvelope(trimmed);
  }

  async approvePlan(reviewId: string): Promise<void> {
    if (!reviewId) {
      return;
    }
    await approvePlanReview(reviewId);
  }

  async startWorkflowFromReviewedPlan(reviewId: string): Promise<WorkflowLaunchResult> {
    const { jobId } = await startWorkflowRun(reviewId);
    const runStatus = await getRunStatus(jobId);
    return {
      jobId,
      runStatus,
    };
  }

  async getWorkflowProgress(jobId: string): Promise<RunStatusData> {
    return getRunStatus(jobId);
  }

  async getWorkflowResults(jobId: string): Promise<DashboardData> {
    return getDashboard(jobId);
  }
}

export const sourcingBackendClient = new SourcingBackendClient();
