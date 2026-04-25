import type { FrontendHistoryRecoveryEnvelope } from "./api";
import { summarizeSearchQuery } from "./historySummary";
import {
  buildReusedCompletedTimelineSteps,
  isReusedCompletedHistory,
} from "./workflow";
import type {
  DashboardData,
  DemoPlan,
  PlanReviewDecision,
  SearchHistoryItem,
} from "../types";

const INTERRUPTED_PLAN_RECOVERY_WINDOW_MS = 20_000;

function extractPlanGenerationPayload(metadata: unknown): Record<string, unknown> {
  const normalized = normalizeHistoryMetadata(metadata);
  const planGeneration = normalized.plan_generation;
  return planGeneration && typeof planGeneration === "object" && !Array.isArray(planGeneration)
    ? (planGeneration as Record<string, unknown>)
    : {};
}

function extractPlanGenerationStatus(metadata: unknown): string {
  return String(extractPlanGenerationPayload(metadata).status || "").trim().toLowerCase();
}

function extractPlanGenerationError(metadata: unknown): string {
  return String(extractPlanGenerationPayload(metadata).error_message || "").trim();
}

export function isPlanHydrationPending(item: Pick<SearchHistoryItem, "phase" | "plan" | "jobId" | "errorMessage" | "historyMetadata">): boolean {
  if (item.phase !== "plan" || item.plan || item.jobId || item.errorMessage) {
    return false;
  }
  const status = extractPlanGenerationStatus(item.historyMetadata);
  return status === "queued" || status === "running" || status === "pending";
}

export function cloneReviewDecision(plan: DemoPlan | null): PlanReviewDecision {
  const defaults = plan?.reviewDecisionDefaults;
  return {
    confirmedCompanyScope: [...(defaults?.confirmedCompanyScope || [])],
    targetCompanyLinkedinUrl: defaults?.targetCompanyLinkedinUrl || "",
    extraSourceFamilies: [...(defaults?.extraSourceFamilies || [])],
    precisionRecallBias: defaults?.precisionRecallBias || "",
    acquisitionStrategyOverride: defaults?.acquisitionStrategyOverride || "",
    useCompanyEmployeesLane: defaults?.useCompanyEmployeesLane,
    keywordPriorityOnly: defaults?.keywordPriorityOnly,
    formerKeywordQueriesOnly: defaults?.formerKeywordQueriesOnly,
    providerPeopleSearchQueryStrategy:
      defaults?.providerPeopleSearchQueryStrategy || "all_queries_union",
    providerPeopleSearchMaxQueries: defaults?.providerPeopleSearchMaxQueries ?? null,
    largeOrgKeywordProbeMode: defaults?.largeOrgKeywordProbeMode,
    forceFreshRun: defaults?.forceFreshRun,
    reuseExistingRoster: defaults?.reuseExistingRoster,
    runFormerSearchSeed: defaults?.runFormerSearchSeed,
  };
}

export function normalizeHistoryMetadata(metadata: unknown): Record<string, unknown> {
  return metadata && typeof metadata === "object" && !Array.isArray(metadata)
    ? (metadata as Record<string, unknown>)
    : {};
}

function normalizeRecoveredPlan(plan: unknown): DemoPlan | null {
  if (!plan || typeof plan !== "object" || Array.isArray(plan)) {
    return null;
  }
  const record = plan as Record<string, unknown>;
  return Object.keys(record).length > 0 ? (record as unknown as DemoPlan) : null;
}

function isPlanlessWorkflow(item: Pick<SearchHistoryItem, "historyMetadata">): boolean {
  const metadata = normalizeHistoryMetadata(item.historyMetadata);
  return String(metadata.workflow_kind || "").trim().toLowerCase() === "excel_intake";
}

export function normalizeHistoryItem(item: SearchHistoryItem): SearchHistoryItem {
  const normalizedPlan = normalizeRecoveredPlan(item.plan);
  const recoveredErrorMessage =
    item.errorMessage || extractPlanGenerationError(item.historyMetadata);
  return {
    ...item,
    plan: normalizedPlan,
    updatedAt: item.updatedAt || item.createdAt,
    errorMessage: recoveredErrorMessage,
    reviewDecision: item.reviewDecision || cloneReviewDecision(normalizedPlan),
    reviewChecklistConfirmed:
      typeof item.reviewChecklistConfirmed === "boolean"
        ? item.reviewChecklistConfirmed
        : (normalizedPlan?.reviewGate?.confirmationItems.length || 0) === 0,
    historyMetadata: normalizeHistoryMetadata(item.historyMetadata),
  };
}

export function recoverInterruptedPlan(
  item: SearchHistoryItem,
  now = Date.now(),
): SearchHistoryItem {
  if (item.plan || item.jobId || item.phase !== "plan" || item.errorMessage) {
    return item;
  }
  if (isPlanHydrationPending(item)) {
    return item;
  }
  const createdAt = new Date(item.createdAt).getTime();
  const expired =
    Number.isFinite(createdAt) && now - createdAt > INTERRUPTED_PLAN_RECOVERY_WINDOW_MS;
  if (!expired) {
    return item;
  }
  return {
    ...item,
    errorMessage: "检索方案生成未完成，请重新提交搜索。",
  };
}

export function shouldRecoverHistoryFromBackend(item: SearchHistoryItem): boolean {
  return (
    Boolean(item.id) &&
    item.phase === "results" &&
    Boolean(item.jobId) &&
    !item.errorMessage &&
    Object.keys(normalizeHistoryMetadata(item.historyMetadata)).length === 0
  );
}

export function prepareHistoryForHydration(
  item: SearchHistoryItem,
  now = Date.now(),
): {
  item: SearchHistoryItem;
  shouldHydratePlan: boolean;
  shouldPollProgress: boolean;
  shouldRecoverFromBackend: boolean;
  shouldHydrateResults: boolean;
} {
  const normalizedItem = normalizeHistoryItem(item);
  const shouldRecoverFromBackend = shouldRecoverHistoryFromBackend(normalizedItem);
  const hydratedItem = shouldRecoverFromBackend
    ? normalizedItem
    : recoverInterruptedPlan(normalizedItem, now);
  return {
    item: hydratedItem,
    shouldHydratePlan:
      hydratedItem.phase === "plan" &&
      !hydratedItem.errorMessage &&
      !hydratedItem.plan &&
      !hydratedItem.jobId,
    shouldPollProgress:
      hydratedItem.phase === "running" &&
      !hydratedItem.errorMessage &&
      Boolean(hydratedItem.jobId) &&
      (Boolean(hydratedItem.plan) || isPlanlessWorkflow(hydratedItem)),
    shouldRecoverFromBackend,
    shouldHydrateResults: hydratedItem.phase === "results",
  };
}

export function historyItemFromRecoveryEnvelope(
  recovery: FrontendHistoryRecoveryEnvelope,
): SearchHistoryItem {
  const recoveredPlan = normalizeRecoveredPlan(recovery.plan);
  const createdAt =
    String(recovery.createdAt || "").trim() ||
    String(recovery.updatedAt || "").trim() ||
    new Date().toISOString();
  const updatedAt =
    String(recovery.updatedAt || "").trim() ||
    String(recovery.createdAt || "").trim() ||
    new Date().toISOString();
  return normalizeHistoryItem({
    id: recovery.historyId,
    createdAt,
    updatedAt,
    queryText: recovery.queryText,
    summary: summarizeSearchQuery(recovery.queryText),
    phase:
      recovery.phase === "idle"
        ? recoveredPlan || recovery.jobId
          ? "plan"
          : "idle"
        : recovery.phase,
    errorMessage: recovery.errorMessage || extractPlanGenerationError(recovery.metadata),
    plan: recoveredPlan,
    reviewId: recovery.reviewId,
    jobId: recovery.jobId,
    revisionText: "",
    reviewDecision: cloneReviewDecision(recoveredPlan),
    reviewChecklistConfirmed:
      (recoveredPlan?.reviewGate?.confirmationItems.length || 0) === 0,
    requiresReview: Boolean(recoveredPlan?.reviewRequired),
    timelineSteps: [],
    selectedCandidateId: "",
    historyMetadata: normalizeHistoryMetadata(recovery.metadata),
  });
}

export function buildReusedCompletedFlow(
  activeFlow: SearchHistoryItem,
  nextDashboard: DashboardData | null = null,
): SearchHistoryItem {
  return {
    ...activeFlow,
    phase: "results",
    updatedAt: activeFlow.updatedAt || new Date().toISOString(),
    timelineSteps: activeFlow.plan
      ? buildReusedCompletedTimelineSteps({
          queryText: activeFlow.queryText,
          plan: activeFlow.plan,
          createdAt: activeFlow.createdAt,
          updatedAt: activeFlow.updatedAt || new Date().toISOString(),
          candidateCount: nextDashboard?.totalCandidates,
          manualReviewCount: nextDashboard?.manualReviewCount,
        })
      : activeFlow.timelineSteps,
    selectedCandidateId:
      activeFlow.selectedCandidateId || nextDashboard?.candidates[0]?.id || "",
  };
}

export function shouldUseReusedCompletedFlow(
  item: Pick<SearchHistoryItem, "historyMetadata"> | null | undefined,
): boolean {
  return isReusedCompletedHistory(item);
}
