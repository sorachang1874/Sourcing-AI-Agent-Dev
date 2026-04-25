import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ExcelWorkflowIntakePanel } from "../components/ExcelWorkflowIntakePanel";
import { SearchFlow } from "../components/SearchFlow";
import { useDashboardCandidateHydration } from "../hooks/useDashboardCandidateHydration";
import { readDemoSession, writeDemoSession } from "../lib/demoSession";
import { getDashboard, peekDashboardCache } from "../lib/api";
import {
  buildReusedCompletedFlow,
  cloneReviewDecision,
  historyItemFromRecoveryEnvelope,
  normalizeHistoryMetadata,
  prepareHistoryForHydration,
  shouldUseReusedCompletedFlow,
} from "../lib/historyRecovery";
import { summarizeSearchQuery } from "../lib/historySummary";
import {
  readSearchHistoryItem,
  startNewSearchEventName,
  upsertSearchHistoryItem,
} from "../lib/searchHistory";
import { sourcingBackendClient } from "../lib/sourcingBackend";
import {
  buildReusedCompletedTimelineSteps,
  buildTimelineSteps,
  createHistorySnapshot,
  hydrateHistoryWithResult,
} from "../lib/workflow";
import type {
  DashboardData,
  DemoPlan,
  RunStatusData,
  SearchHistoryItem,
  SearchTimelineStep,
  WorkflowPhase,
} from "../types";

const ENABLE_EXCEL_INTAKE_WORKFLOW = import.meta.env.VITE_ENABLE_EXCEL_INTAKE_WORKFLOW !== "false";

const promptExamples = [
  "帮我找Google做多模态方向和Pre-train方向的人",
  "我想要OpenAI做Reasoning方向的人",
  "帮我找Anthropic做Pre-training的人",
  "帮我找Reflection AI的Post-train方向的人",
];

interface ExcelBatchLaunchGroupView {
  company: string;
  historyId: string;
  jobId: string;
  queryText: string;
  rowCount: number;
  sourceCompanies: string[];
  status: string;
  currentMessage: string;
}

interface ExcelBatchLaunchView {
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
  groups: ExcelBatchLaunchGroupView[];
}

const emptyFlow: SearchHistoryItem = {
  id: "",
  createdAt: "",
  updatedAt: "",
  queryText: "",
  summary: "",
  phase: "idle",
  errorMessage: "",
  plan: null,
  reviewId: "",
  jobId: "",
  revisionText: "",
  reviewDecision: {
    confirmedCompanyScope: [],
    targetCompanyLinkedinUrl: "",
    extraSourceFamilies: [],
  },
  reviewChecklistConfirmed: false,
  requiresReview: false,
  timelineSteps: [],
  selectedCandidateId: "",
  historyMetadata: {},
};

export function SearchPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const historyId = searchParams.get("history") || "";
  const routeJobId = searchParams.get("job") || "";
  const [flow, setFlow] = useState<SearchHistoryItem>(emptyFlow);
  const flowRef = useRef<SearchHistoryItem>(emptyFlow);
  const [queryText, setQueryText] = useState("");
  const [dashboard, setDashboard] = useState<DashboardData | null>(() => peekDashboardCache(routeJobId));
  const [runStatus, setRunStatus] = useState<RunStatusData | null>(null);
  const [isGeneratingPlan, setIsGeneratingPlan] = useState(false);
  const [isApplyingRevision, setIsApplyingRevision] = useState(false);
  const [isConfirmingPlan, setIsConfirmingPlan] = useState(false);
  const [isContinuingStage2, setIsContinuingStage2] = useState(false);
  const [isStartingExcelWorkflow, setIsStartingExcelWorkflow] = useState(false);
  const [isLoadingResults, setIsLoadingResults] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const timerRef = useRef<number | null>(null);
  const planHydrationTimerRef = useRef<number | null>(null);
  const requestEpochRef = useRef(0);
  const dashboardWarmupAttemptedJobIdsRef = useRef<Set<string>>(new Set());
  const {
    isHydratingCandidates,
    candidateHydrationError,
  } = useDashboardCandidateHydration({
    jobId: flow.jobId || routeJobId,
    dashboard,
    onDashboardChange: setDashboard,
  });

  const commitFlow = (nextFlow: SearchHistoryItem) => {
    flowRef.current = nextFlow;
    setFlow(nextFlow);
  };

  const syncSearchRoute = (nextHistoryId: string, nextJobId = "", replace = false) => {
    const nextParams: Record<string, string> = {};
    if (nextHistoryId) {
      nextParams.history = nextHistoryId;
    }
    if (nextJobId) {
      nextParams.job = nextJobId;
    }
    setSearchParams(nextParams, { replace });
  };

  const updateFlow = (updater: (current: SearchHistoryItem) => SearchHistoryItem): SearchHistoryItem => {
    const nextFlow = updater(flowRef.current);
    commitFlow(nextFlow);
    return nextFlow;
  };

  const resetComposer = () => {
    nextRequestEpoch();
    stopTimelineAnimation();
    stopPlanHydrationPolling();
    dashboardWarmupAttemptedJobIdsRef.current.clear();
    commitFlow(emptyFlow);
    setQueryText("");
    setDashboard(null);
    setRunStatus(null);
    setIsStartingExcelWorkflow(false);
    setIsLoadingResults(false);
    setErrorMessage("");
    writeDemoSession({
      queryText: "",
      plan: null,
      reviewApproved: false,
      lastVisitedStage: "search",
      activeHistoryId: "",
      phase: "idle",
      revisionText: "",
      timelineSteps: [],
      selectedCandidateId: "",
    });
    setSearchParams({}, { replace: true });
  };

  const persistFlow = (nextFlow: SearchHistoryItem, nextDashboard: DashboardData | null = dashboard) => {
    commitFlow(nextFlow);
    if (nextFlow.id) {
      upsertSearchHistoryItem(nextFlow);
    }
    writeDemoSession({
      queryText: nextFlow.queryText,
      plan: nextFlow.plan,
      reviewApproved: nextFlow.phase === "running" || nextFlow.phase === "results",
      lastVisitedStage: "search",
      activeHistoryId: nextFlow.id,
      phase: nextFlow.phase,
      revisionText: nextFlow.revisionText,
      timelineSteps: nextFlow.timelineSteps,
      selectedCandidateId: nextFlow.selectedCandidateId,
    });
  };

  const stopTimelineAnimation = () => {
    if (timerRef.current) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  };

  const stopPlanHydrationPolling = () => {
    if (planHydrationTimerRef.current) {
      window.clearTimeout(planHydrationTimerRef.current);
      planHydrationTimerRef.current = null;
    }
  };

  const nextRequestEpoch = () => {
    requestEpochRef.current += 1;
    return requestEpochRef.current;
  };

  const isRequestEpochActive = (requestEpoch: number) => requestEpochRef.current === requestEpoch;

  const hasRenderableDashboard = (nextDashboard: DashboardData | null | undefined): boolean =>
    Boolean(nextDashboard && Math.max(nextDashboard.totalCandidates || 0, nextDashboard.candidates.length) > 0);

  const isStage1PreviewReady = (nextRunStatus: RunStatusData): boolean =>
    nextRunStatus.timeline.some((step) => step.stage === "stage_1_preview" && step.status === "completed");

  const waitForRenderableDashboard = async (
    jobId: string,
    requestEpoch: number,
    options?: {
      maxAttempts?: number;
      delayMs?: number;
    },
  ): Promise<DashboardData | null> => {
    const maxAttempts = Math.max(options?.maxAttempts || 30, 1);
    const delayMs = Math.max(options?.delayMs || 1500, 250);
    let lastDashboard: DashboardData | null = null;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const nextDashboard = await getDashboard(jobId, { forceRefresh: attempt > 0 });
      lastDashboard = nextDashboard;
      if (!isRequestEpochActive(requestEpoch)) {
        return nextDashboard;
      }
      if (hasRenderableDashboard(nextDashboard)) {
        return nextDashboard;
      }
      if (attempt + 1 >= maxAttempts) {
        break;
      }
      await new Promise<void>((resolve) => {
        window.setTimeout(resolve, delayMs);
      });
    }
    return lastDashboard;
  };

  const warmDashboardForPreviewReadyJob = (
    jobId: string,
    nextRunStatus: RunStatusData,
    requestEpoch: number,
  ) => {
    if (!jobId) {
      return;
    }
    const stage1PreviewReady = isStage1PreviewReady(nextRunStatus);
    if (!stage1PreviewReady) {
      return;
    }
    const cachedDashboard = peekDashboardCache(jobId);
    if (cachedDashboard) {
      if (hasRenderableDashboard(cachedDashboard)) {
        setDashboard(cachedDashboard);
        return;
      }
    }
    if (dashboardWarmupAttemptedJobIdsRef.current.has(jobId)) {
      return;
    }
    dashboardWarmupAttemptedJobIdsRef.current.add(jobId);
    void getDashboard(jobId)
      .then((nextDashboard) => {
        if (!isRequestEpochActive(requestEpoch)) {
          return;
        }
        const renderable = hasRenderableDashboard(nextDashboard);
        if (renderable) {
          setDashboard(nextDashboard);
        }
        setIsLoadingResults(!renderable);
        const currentFlow = flowRef.current;
        if (!renderable || (currentFlow.jobId || "") !== jobId || currentFlow.phase !== "running") {
          return;
        }
        persistFlow(
          {
            ...currentFlow,
            selectedCandidateId: currentFlow.selectedCandidateId || nextDashboard.candidates[0]?.id || "",
          },
          nextDashboard,
        );
        if (Math.max(nextDashboard.totalCandidates || 0, nextDashboard.candidates.length) <= 0) {
          dashboardWarmupAttemptedJobIdsRef.current.delete(jobId);
        }
      })
      .catch(() => {
        if (!isRequestEpochActive(requestEpoch)) {
          return;
        }
        setIsLoadingResults(true);
        dashboardWarmupAttemptedJobIdsRef.current.delete(jobId);
      });
  };

  const startProgressPolling = (
    jobId: string,
    activeFlow: SearchHistoryItem,
    requestEpoch = requestEpochRef.current,
  ) => {
    stopPlanHydrationPolling();
    const poll = async () => {
      try {
        const nextRunStatus = await sourcingBackendClient.getWorkflowProgress(jobId);
        if (!isRequestEpochActive(requestEpoch)) {
          return;
        }
        setRunStatus(nextRunStatus);
        if (isStage1PreviewReady(nextRunStatus) && !peekDashboardCache(jobId)) {
          setIsLoadingResults(true);
        }
        warmDashboardForPreviewReadyJob(jobId, nextRunStatus, requestEpoch);
        const nextSteps = buildTimelineSteps(nextRunStatus, activeFlow.queryText, activeFlow.plan || flowRef.current.plan);
        const nextError =
          nextRunStatus.status === "failed"
            ? nextRunStatus.timeline[nextRunStatus.timeline.length - 1]?.detail || "工作流未能完成，请检查后端采集日志。"
            : "";

        if (nextRunStatus.status === "completed") {
          stopTimelineAnimation();
          const cachedDashboard = peekDashboardCache(jobId);
          if (cachedDashboard && hasRenderableDashboard(cachedDashboard)) {
            setDashboard(cachedDashboard);
          }
          const readyCachedDashboard = hasRenderableDashboard(cachedDashboard);
          setIsLoadingResults(true);
          try {
            const nextDashboard = readyCachedDashboard
              ? await sourcingBackendClient.getWorkflowResults(jobId)
              : await waitForRenderableDashboard(jobId, requestEpoch);
            if (!isRequestEpochActive(requestEpoch)) {
              return;
            }
            if (!nextDashboard || !hasRenderableDashboard(nextDashboard)) {
              const message = "候选人看板仍在准备中，请稍后刷新。";
              setErrorMessage(message);
              setDashboard(null);
              setIsLoadingResults(false);
              persistFlow(
                {
                  ...activeFlow,
                  phase: "results",
                  updatedAt: new Date().toISOString(),
                  errorMessage: message,
                  timelineSteps: nextSteps,
                  selectedCandidateId: "",
                },
                null,
              );
              return;
            }
            setDashboard(nextDashboard);
            setErrorMessage("");
            setIsLoadingResults(false);
            persistFlow(
              {
                ...activeFlow,
                phase: "results",
                updatedAt: new Date().toISOString(),
                timelineSteps: nextSteps,
                selectedCandidateId: nextDashboard.candidates[0]?.id || "",
              },
              nextDashboard,
            );
          } catch (error) {
            if (!isRequestEpochActive(requestEpoch)) {
              return;
            }
            const message = error instanceof Error ? error.message : "结果看板加载失败。";
            setErrorMessage(message);
            setDashboard(null);
            setIsLoadingResults(false);
            persistFlow(
              {
                ...activeFlow,
                phase: "results",
                timelineSteps: nextSteps,
                selectedCandidateId: "",
              },
              null,
            );
          }
          return;
        }

        const runningFlow: SearchHistoryItem = {
          ...activeFlow,
          phase: "running",
          errorMessage: "",
          timelineSteps: nextSteps,
        };
        if (!isRequestEpochActive(requestEpoch)) {
          return;
        }
        setErrorMessage(nextError);

        if (nextRunStatus.status === "failed") {
          persistFlow(
            {
              ...activeFlow,
              phase: "results",
              errorMessage: nextError,
              timelineSteps: nextSteps,
            },
            null,
          );
          stopTimelineAnimation();
          return;
        }

        persistFlow(runningFlow, null);

        if (nextRunStatus.status === "blocked" && nextRunStatus.awaitingUserAction === "continue_stage2") {
          stopTimelineAnimation();
          return;
        }

        timerRef.current = window.setTimeout(() => {
          void poll();
        }, nextRunStatus.status === "blocked" ? 3500 : 3000);
      } catch {
        if (!isRequestEpochActive(requestEpoch)) {
          return;
        }
        timerRef.current = window.setTimeout(() => {
          void poll();
        }, 3500);
      }
    };

    void poll();
  };

  const startPlanHydrationPolling = (
    targetHistoryId: string,
    requestEpoch: number,
    attempt = 0,
  ) => {
    if (!targetHistoryId) {
      return;
    }
    stopPlanHydrationPolling();
    const delayMs = attempt <= 0 ? 0 : attempt <= 3 ? 700 : 1400;
    planHydrationTimerRef.current = window.setTimeout(() => {
      void (async () => {
        try {
          const recovered = await sourcingBackendClient.recoverHistory(targetHistoryId);
          if (!isRequestEpochActive(requestEpoch)) {
            return;
          }
          const recoveredFlow = historyItemFromRecoveryEnvelope(recovered);
          persistFlow(recoveredFlow, null);
          syncSearchRoute(recoveredFlow.id, recoveredFlow.jobId, true);
          if (recoveredFlow.errorMessage) {
            setErrorMessage(recoveredFlow.errorMessage);
            stopPlanHydrationPolling();
            return;
          }
          if (recoveredFlow.plan || recoveredFlow.jobId || recoveredFlow.phase === "running" || recoveredFlow.phase === "results") {
            stopPlanHydrationPolling();
            await hydrateFromHistory(recoveredFlow, requestEpoch);
            return;
          }
          startPlanHydrationPolling(targetHistoryId, requestEpoch, attempt + 1);
        } catch (error) {
          if (!isRequestEpochActive(requestEpoch)) {
            return;
          }
          if (attempt >= 30) {
            const message = error instanceof Error ? error.message : "检索方案生成未完成，请重新提交搜索。";
            setErrorMessage(message);
            persistFlow(
              {
                ...flowRef.current,
                errorMessage: message,
              },
              null,
            );
            stopPlanHydrationPolling();
            return;
          }
          startPlanHydrationPolling(targetHistoryId, requestEpoch, attempt + 1);
        }
      })();
    }, delayMs);
  };

  const hydrateFromHistory = async (item: SearchHistoryItem, requestEpoch: number) => {
    const {
      item: recoveredItem,
      shouldHydratePlan,
      shouldHydrateResults,
      shouldPollProgress,
      shouldRecoverFromBackend,
    } = prepareHistoryForHydration(item);
    if (!isRequestEpochActive(requestEpoch)) {
      return;
    }
    commitFlow(recoveredItem);
    setQueryText(recoveredItem.queryText);
    setErrorMessage(recoveredItem.errorMessage || "");
    const cachedRecoveredDashboard = peekDashboardCache(recoveredItem.jobId);
    setDashboard(cachedRecoveredDashboard);
    setRunStatus(null);
    setIsStartingExcelWorkflow(false);
    setIsLoadingResults(shouldHydrateResults && !cachedRecoveredDashboard);
    if (recoveredItem.id !== item.id || recoveredItem.errorMessage !== item.errorMessage) {
      persistFlow(recoveredItem, null);
    }
    if (shouldHydratePlan) {
      startPlanHydrationPolling(recoveredItem.id, requestEpoch);
      return;
    }
    if (shouldPollProgress) {
      startProgressPolling(recoveredItem.jobId, recoveredItem, requestEpoch);
      return;
    }
    if (shouldRecoverFromBackend) {
      await recoverHistoryFromBackend(recoveredItem.id, requestEpoch);
      return;
    }
    if (!shouldHydrateResults) {
      setDashboard(null);
      setRunStatus(null);
      setIsLoadingResults(false);
      return;
    }
    if (!recoveredItem.jobId) {
      setDashboard(null);
      setIsLoadingResults(false);
      setErrorMessage("历史记录缺少 job_id，无法从后端恢复真实结果，请重新搜索。");
      return;
    }
    const cachedDashboard = peekDashboardCache(recoveredItem.jobId);
    const timelinePlan = recoveredItem.plan || flowRef.current.plan;
    let refreshedTimelineSteps = recoveredItem.timelineSteps;
    const reusedCompletedHistory = shouldUseReusedCompletedFlow(recoveredItem);
    if (reusedCompletedHistory && timelinePlan) {
      refreshedTimelineSteps = buildReusedCompletedTimelineSteps({
        queryText: recoveredItem.queryText,
        plan: timelinePlan,
        createdAt: recoveredItem.createdAt,
        updatedAt: recoveredItem.updatedAt || recoveredItem.createdAt,
        candidateCount: cachedDashboard?.totalCandidates,
        manualReviewCount: cachedDashboard?.manualReviewCount,
      });
      persistFlow(
        {
          ...recoveredItem,
          phase: "results",
          timelineSteps: refreshedTimelineSteps,
        },
        cachedDashboard,
      );
    } else {
      void sourcingBackendClient.getWorkflowProgress(recoveredItem.jobId)
        .then((restoredRunStatus) => {
          if (!isRequestEpochActive(requestEpoch)) {
            return;
          }
          setRunStatus(restoredRunStatus);
          const restoredError =
            restoredRunStatus.status === "failed"
              ? restoredRunStatus.timeline[restoredRunStatus.timeline.length - 1]?.detail ||
                recoveredItem.errorMessage ||
                "工作流未能完成，请检查后端采集日志。"
              : "";
          refreshedTimelineSteps = buildTimelineSteps(
            restoredRunStatus,
            recoveredItem.queryText,
            timelinePlan,
          );
          if (refreshedTimelineSteps.length > 0 || restoredRunStatus.status === "failed") {
            persistFlow(
              {
                ...recoveredItem,
                phase: restoredRunStatus.status === "failed" ? "results" : recoveredItem.phase,
                errorMessage: restoredError,
                timelineSteps: refreshedTimelineSteps,
              },
              peekDashboardCache(recoveredItem.jobId),
            );
          }
        })
        .catch(() => {
          if (!isRequestEpochActive(requestEpoch)) {
            return;
          }
          setRunStatus(null);
        });
    }
    try {
      const restoredDashboard = await getDashboard(recoveredItem.jobId);
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      setDashboard(restoredDashboard);
      setErrorMessage("");
      setIsLoadingResults(false);
      if (reusedCompletedHistory) {
        persistFlow(buildReusedCompletedFlow(recoveredItem, restoredDashboard), restoredDashboard);
        return;
      }
      const hydrated = {
        ...hydrateHistoryWithResult(recoveredItem, restoredDashboard),
        timelineSteps: refreshedTimelineSteps,
      };
      persistFlow(hydrated, restoredDashboard);
    } catch (error) {
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      setIsLoadingResults(false);
      const dashboardError = error instanceof Error ? error.message : "历史结果恢复失败，请稍后重试。";
      setDashboard(cachedDashboard);
      setErrorMessage((currentError) => currentError || dashboardError);
      if (dashboardError !== recoveredItem.errorMessage) {
        persistFlow(
          {
            ...recoveredItem,
            errorMessage: dashboardError,
            timelineSteps: refreshedTimelineSteps,
          },
          cachedDashboard,
        );
      }
    }
  };

  const recoverHistoryFromBackend = async (missingHistoryId: string, requestEpoch: number) => {
    try {
      const recovered = await sourcingBackendClient.recoverHistory(missingHistoryId);
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const recoveredFlow = historyItemFromRecoveryEnvelope(recovered);
      persistFlow(recoveredFlow, null);
      syncSearchRoute(recoveredFlow.id, recoveredFlow.jobId, true);
      await hydrateFromHistory(recoveredFlow, requestEpoch);
    } catch (error) {
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const message = error instanceof Error ? error.message : "历史记录恢复失败，请重新发起搜索。";
      commitFlow(emptyFlow);
      setDashboard(null);
      setRunStatus(null);
      setQueryText("");
      setErrorMessage(message);
    }
  };

  const recoverPlanHistoryAfterRequestFailure = async (
    historyIdToRecover: string,
    requestEpoch: number,
  ): Promise<boolean> => {
    if (!historyIdToRecover) {
      return false;
    }
    try {
      const recovered = await sourcingBackendClient.recoverHistory(historyIdToRecover);
      if (!isRequestEpochActive(requestEpoch)) {
        return true;
      }
      const recoveredFlow = historyItemFromRecoveryEnvelope(recovered);
      if (
        !recoveredFlow.plan &&
        !recoveredFlow.reviewId &&
        !recoveredFlow.jobId &&
        Object.keys(normalizeHistoryMetadata(recoveredFlow.historyMetadata)).length === 0
      ) {
        return false;
      }
      persistFlow(recoveredFlow, null);
      syncSearchRoute(recoveredFlow.id, recoveredFlow.jobId, true);
      await hydrateFromHistory(recoveredFlow, requestEpoch);
      return true;
    } catch {
      return false;
    }
  };

  useEffect(() => {
    stopTimelineAnimation();
    stopPlanHydrationPolling();
    dashboardWarmupAttemptedJobIdsRef.current.clear();
    const routeMatchesCurrentFlow = historyId
      ? flowRef.current.id === historyId && (flowRef.current.jobId || "") === routeJobId
      : !flowRef.current.id && !flowRef.current.jobId;
    const requestEpoch = routeMatchesCurrentFlow ? requestEpochRef.current : nextRequestEpoch();
    const cachedRouteDashboard = peekDashboardCache(routeJobId);
    setDashboard(cachedRouteDashboard);
    setRunStatus(null);
    setIsStartingExcelWorkflow(false);
    setIsLoadingResults(false);
    setErrorMessage("");
    if (!historyId) {
      const session = readDemoSession();
      if (session.activeHistoryId) {
        const activeHistory = readSearchHistoryItem(session.activeHistoryId);
        if (activeHistory) {
          setDashboard(peekDashboardCache(activeHistory.jobId));
          void hydrateFromHistory(activeHistory, requestEpoch);
          syncSearchRoute(activeHistory.id, activeHistory.jobId, true);
          return;
        }
        void recoverHistoryFromBackend(session.activeHistoryId, requestEpoch);
        return;
      }
      commitFlow(emptyFlow);
      setQueryText(session.queryText || "");
      setDashboard(null);
      return;
    }
    const historyItem = readSearchHistoryItem(historyId);
    if (!historyItem) {
      commitFlow(emptyFlow);
      setQueryText("");
      void recoverHistoryFromBackend(historyId, requestEpoch);
      return;
    }
    setDashboard(peekDashboardCache(historyItem.jobId));
    if (historyItem.jobId && routeJobId !== historyItem.jobId) {
      syncSearchRoute(historyItem.id, historyItem.jobId, true);
    }
    void hydrateFromHistory(historyItem, requestEpoch);
    return () => {
      stopTimelineAnimation();
      stopPlanHydrationPolling();
    };
  }, [historyId, routeJobId, setSearchParams]);

  useEffect(() => () => {
    stopTimelineAnimation();
    stopPlanHydrationPolling();
  }, []);

  useEffect(() => {
    const handleStartNewSearch = () => resetComposer();
    window.addEventListener(startNewSearchEventName(), handleStartNewSearch);
    return () => window.removeEventListener(startNewSearchEventName(), handleStartNewSearch);
  }, [setSearchParams]);

  const submitSearch = async (submittedQuery: string) => {
    const nextQuery = submittedQuery.trim();
    if (!nextQuery) {
      return;
    }
    const requestEpoch = nextRequestEpoch();
    stopTimelineAnimation();
    stopPlanHydrationPolling();
    dashboardWarmupAttemptedJobIdsRef.current.clear();
    setDashboard(null);
    setRunStatus(null);
    setIsLoadingResults(false);
    setErrorMessage("");
    setIsGeneratingPlan(true);

    const historyItem = createHistorySnapshot(nextQuery);
    const pendingFlow: SearchHistoryItem = {
      ...historyItem,
      phase: "plan",
      errorMessage: "",
    };
    syncSearchRoute(historyItem.id);
    persistFlow(pendingFlow, null);
    setQueryText(nextQuery);

    try {
      const { plan: planned, reviewId, historyId: nextHistoryId, status, raw } =
        await sourcingBackendClient.planNaturalLanguageSearch(nextQuery, historyItem.id);
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const resolvedHistoryId = nextHistoryId || historyItem.id;
      if (!planned) {
        if (status === "pending") {
          const pendingHydrationFlow: SearchHistoryItem = {
            ...pendingFlow,
            id: resolvedHistoryId,
            historyMetadata:
              raw && typeof raw === "object" && !Array.isArray(raw)
                ? normalizeHistoryMetadata((raw as Record<string, unknown>).metadata)
                : {},
          };
          persistFlow(pendingHydrationFlow, null);
          syncSearchRoute(resolvedHistoryId, "", true);
          startPlanHydrationPolling(resolvedHistoryId, requestEpoch);
          return;
        }
        throw new Error("后端没有返回有效检索计划。");
      }
      const readyFlow: SearchHistoryItem = {
        ...pendingFlow,
        id: resolvedHistoryId,
        summary: summarizeSearchQuery(nextQuery),
        plan: planned,
        reviewId,
        requiresReview: planned.reviewRequired,
        reviewDecision: cloneReviewDecision(planned),
        reviewChecklistConfirmed: (planned.reviewGate?.confirmationItems.length || 0) === 0,
        errorMessage: "",
      };
      persistFlow(readyFlow, null);
    } catch (error) {
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const recovered = await recoverPlanHistoryAfterRequestFailure(historyItem.id, requestEpoch);
      if (recovered) {
        setErrorMessage("");
        setRunStatus(null);
        setIsLoadingResults(false);
        return;
      }
      const message = error instanceof Error ? error.message : "生成检索方案失败，请稍后重试。";
      setErrorMessage(message);
      setRunStatus(null);
      setIsLoadingResults(false);
      persistFlow(
        {
          ...pendingFlow,
          errorMessage: message,
        },
        null,
      );
    } finally {
      setIsGeneratingPlan(false);
    }
  };

  const startExcelWorkflow = async (payload: {
    file: File;
    filename: string;
  }): Promise<ExcelBatchLaunchView> => {
    setErrorMessage("");
    setIsStartingExcelWorkflow(true);

    try {
      const launched = await sourcingBackendClient.startExcelIntakeWorkflow({
        file: payload.file,
        filename: payload.filename,
      });
      const groups: ExcelBatchLaunchGroupView[] = launched.groups.map((group) => {
        const status = String(group.runStatus?.status || group.status || "queued");
        const currentMessage = String(group.runStatus?.currentMessage || "");
        const historyMetadata = normalizeHistoryMetadata({
          source: "excel_intake_workflow",
          workflow_kind: "excel_intake",
          workflow_status: status,
          batch_id: launched.batchId,
        });
        const historySnapshot = createHistorySnapshot(group.queryText);
        const historyItem: SearchHistoryItem = {
          ...historySnapshot,
          id: group.historyId || historySnapshot.id,
          queryText: group.queryText,
          summary: `Excel 导入 · ${group.targetCompany}`,
          phase: status === "completed" ? "results" : "running",
          errorMessage: "",
          plan: null,
          reviewId: "",
          jobId: group.jobId,
          revisionText: "",
          reviewDecision: {
            confirmedCompanyScope: [],
            targetCompanyLinkedinUrl: "",
            extraSourceFamilies: [],
          },
          reviewChecklistConfirmed: true,
          requiresReview: false,
          timelineSteps: group.runStatus ? buildTimelineSteps(group.runStatus, group.queryText, null) : [],
          selectedCandidateId: "",
          historyMetadata,
        };
        upsertSearchHistoryItem(historyItem);
        return {
          company: group.targetCompany,
          historyId: historyItem.id,
          jobId: group.jobId,
          queryText: group.queryText,
          rowCount: group.rowCount,
          sourceCompanies: group.sourceCompanies,
          status,
          currentMessage,
        };
      });
      return {
        batchId: launched.batchId,
        inputFilename: launched.inputFilename,
        totalRowCount: launched.totalRowCount,
        createdJobCount: launched.createdJobCount,
        groupCount: launched.groupCount,
        unassignedRowCount: launched.unassignedRowCount,
        unassignedRows: launched.unassignedRows,
        groups,
      };
    } catch (error) {
      throw error instanceof Error ? error : new Error("Excel intake workflow 启动失败。");
    } finally {
      setIsStartingExcelWorkflow(false);
    }
  };

  const applyRevision = async () => {
    const currentFlow = flowRef.current;
    if (!currentFlow.plan || !currentFlow.queryText) {
      return;
    }
    const requestEpoch = requestEpochRef.current;
    setIsApplyingRevision(true);
    stopPlanHydrationPolling();
    setErrorMessage("");
    setIsLoadingResults(false);
    try {
      const { plan: planned, reviewId, historyId: nextHistoryId, status, raw } =
        await sourcingBackendClient.planNaturalLanguageSearch(
        [currentFlow.queryText, currentFlow.revisionText].filter(Boolean).join(" "),
        currentFlow.id,
      );
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const resolvedHistoryId = nextHistoryId || currentFlow.id;
      if (!planned) {
        if (status === "pending") {
          const pendingRevisionFlow: SearchHistoryItem = {
            ...currentFlow,
            id: resolvedHistoryId,
            plan: null,
            reviewId: "",
            requiresReview: false,
            reviewChecklistConfirmed: false,
            errorMessage: "",
            historyMetadata:
              raw && typeof raw === "object" && !Array.isArray(raw)
                ? normalizeHistoryMetadata((raw as Record<string, unknown>).metadata)
                : currentFlow.historyMetadata,
          };
          persistFlow(pendingRevisionFlow, dashboard);
          syncSearchRoute(resolvedHistoryId, "", true);
          startPlanHydrationPolling(resolvedHistoryId, requestEpoch);
          return;
        }
        throw new Error("后端没有返回有效修订计划。");
      }
      const nextFlow: SearchHistoryItem = {
        ...currentFlow,
        id: resolvedHistoryId,
        plan: planned,
        reviewId: reviewId || currentFlow.reviewId,
        requiresReview: planned.reviewRequired,
        reviewDecision: cloneReviewDecision(planned),
        reviewChecklistConfirmed: (planned.reviewGate?.confirmationItems.length || 0) === 0,
        errorMessage: "",
      };
      persistFlow(nextFlow, dashboard);
    } catch (error) {
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const recovered = await recoverPlanHistoryAfterRequestFailure(currentFlow.id, requestEpoch);
      if (recovered) {
        setErrorMessage("");
        setRunStatus(null);
        setIsLoadingResults(false);
        return;
      }
      const message = error instanceof Error ? error.message : "修改方案失败，请稍后重试。";
      setErrorMessage(message);
      setRunStatus(null);
      setIsLoadingResults(false);
      persistFlow(
        {
          ...currentFlow,
          errorMessage: message,
        },
        dashboard,
      );
    } finally {
      setIsApplyingRevision(false);
    }
  };

  const confirmPlan = async () => {
    const currentFlow = flowRef.current;
    if (!currentFlow.plan) {
      return;
    }
    const requestEpoch = nextRequestEpoch();
    const requiresReviewAcknowledgement =
      (currentFlow.plan.reviewGate?.confirmationItems.length || 0) > 0;
    const reviewChecklistConfirmed =
      currentFlow.reviewChecklistConfirmed
      || requiresReviewAcknowledgement;
    setIsConfirmingPlan(true);
    stopTimelineAnimation();
    stopPlanHydrationPolling();
    setIsLoadingResults(false);
    setErrorMessage("");

    try {
      if (requiresReviewAcknowledgement && reviewChecklistConfirmed && !currentFlow.reviewChecklistConfirmed) {
        updateFlow((current) => ({
          ...current,
          reviewChecklistConfirmed: true,
        }));
      }
      let reviewId = currentFlow.reviewId;
      if (reviewId) {
        await sourcingBackendClient.approvePlan(reviewId, currentFlow.plan, currentFlow.reviewDecision);
      }
      const { jobId, runStatus, raw } = await sourcingBackendClient.startWorkflowFromReviewedPlan(reviewId, currentFlow.id);
      if (!jobId) {
        throw new Error("后端没有返回有效的 job_id，工作流未能启动。");
      }
      const historyMetadata =
        raw && typeof raw === "object" && !Array.isArray(raw)
          ? normalizeHistoryMetadata({
              source: String((raw as Record<string, unknown>).source || "start_workflow"),
              workflow_status: String((raw as Record<string, unknown>).workflow_status || ""),
              dispatch:
                (raw as Record<string, unknown>).dispatch &&
                typeof (raw as Record<string, unknown>).dispatch === "object" &&
                !Array.isArray((raw as Record<string, unknown>).dispatch)
                  ? ((raw as Record<string, unknown>).dispatch as Record<string, unknown>)
                  : {},
            })
          : {};
      const reusedCompletedLaunch = shouldUseReusedCompletedFlow({ historyMetadata });
      if (reusedCompletedLaunch) {
        setRunStatus(null);
        const cachedDashboard = peekDashboardCache(jobId);
        setDashboard(cachedDashboard);
        setIsLoadingResults(!hasRenderableDashboard(cachedDashboard));
        const reusedFlow = buildReusedCompletedFlow(
          {
            ...currentFlow,
            phase: "results",
            errorMessage: "",
            reviewId,
            jobId,
            updatedAt: new Date().toISOString(),
            historyMetadata,
          },
          cachedDashboard,
        );
        persistFlow(reusedFlow, cachedDashboard);
        syncSearchRoute(reusedFlow.id, reusedFlow.jobId, true);
        void getDashboard(jobId)
          .then((nextDashboard) => {
            if (!isRequestEpochActive(requestEpoch)) {
              return;
            }
            setDashboard(nextDashboard);
            setErrorMessage("");
            setIsLoadingResults(false);
            persistFlow(buildReusedCompletedFlow(reusedFlow, nextDashboard), nextDashboard);
          })
          .catch((error) => {
            if (!isRequestEpochActive(requestEpoch)) {
              return;
            }
            const message = error instanceof Error ? error.message : "结果看板加载失败。";
            setErrorMessage(message);
            setDashboard(null);
            setIsLoadingResults(false);
            persistFlow(
              {
                ...reusedFlow,
                errorMessage: message,
              },
              null,
            );
          });
        return;
      }
      const initialSteps = buildTimelineSteps(runStatus, currentFlow.queryText, currentFlow.plan);
      setRunStatus(runStatus);
      const nextFlow: SearchHistoryItem = {
        ...currentFlow,
        phase: "running",
        errorMessage: "",
        reviewId,
        jobId,
        updatedAt: new Date().toISOString(),
        historyMetadata,
        timelineSteps: initialSteps,
        selectedCandidateId: "",
      };
      setDashboard(null);
      persistFlow(nextFlow, null);
      syncSearchRoute(nextFlow.id, nextFlow.jobId, true);
      startProgressPolling(jobId, nextFlow, requestEpoch);
    } catch (error) {
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const message = error instanceof Error ? error.message : "启动工作流失败，请检查后端连接。";
      setErrorMessage(message);
      setRunStatus(null);
      setIsLoadingResults(false);
      persistFlow(
        {
          ...currentFlow,
          errorMessage: message,
        },
        dashboard,
      );
    } finally {
      setIsConfirmingPlan(false);
    }
  };

  const syncSelectedCandidate = (candidateId: string) => {
    if (!candidateId) {
      return;
    }
    const currentFlow = flowRef.current;
    if (!currentFlow.id) {
      return;
    }
    persistFlow(
      {
        ...currentFlow,
        selectedCandidateId: candidateId,
      },
      dashboard,
    );
  };

  const phase: WorkflowPhase = flow.phase || "idle";
  const plan: DemoPlan | null = flow.plan;
  const timelineSteps: SearchTimelineStep[] = flow.timelineSteps || [];
  const isExcelIntakeHistory = String(flow.historyMetadata?.workflow_kind || "").trim().toLowerCase() === "excel_intake";

  const refreshDashboardForCurrentJob = async () => {
    const currentFlow = flowRef.current;
    if (!currentFlow.jobId) {
      return;
    }
    const requestEpoch = requestEpochRef.current;
    try {
      const nextDashboard = await getDashboard(currentFlow.jobId, { forceRefresh: true });
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      setDashboard(nextDashboard);
      persistFlow(
        {
          ...currentFlow,
          selectedCandidateId:
            currentFlow.selectedCandidateId || nextDashboard.candidates[0]?.id || "",
        },
        nextDashboard,
      );
    } catch {
      // Keep the previous dashboard on screen; manual review actions should not blank the view.
    }
  };

  const continueStage2 = async () => {
    const currentFlow = flowRef.current;
    if (!currentFlow.jobId) {
      return;
    }
    const requestEpoch = nextRequestEpoch();
    setIsContinuingStage2(true);
    setErrorMessage("");
    try {
      const nextRunStatus = await sourcingBackendClient.continueStage2(currentFlow.jobId);
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      setRunStatus(nextRunStatus);
      startProgressPolling(currentFlow.jobId, currentFlow, requestEpoch);
    } catch (error) {
      if (!isRequestEpochActive(requestEpoch)) {
        return;
      }
      const message = error instanceof Error ? error.message : "继续执行 Stage 2 失败，请稍后重试。";
      setErrorMessage(message);
    } finally {
      setIsContinuingStage2(false);
    }
  };

  const pollExcelBatchGroupProgress = async (jobId: string) => {
    const nextRunStatus = await sourcingBackendClient.getWorkflowProgress(jobId);
    return {
      status: String(nextRunStatus.status || ""),
      currentMessage: String(nextRunStatus.currentMessage || ""),
    };
  };

  const openExcelBatchHistory = (nextHistoryId: string, nextJobId: string) => {
    syncSearchRoute(nextHistoryId, nextJobId, false);
  };

  return (
    <section className="page">
      <SearchFlow
        phase={phase}
        errorMessage={errorMessage}
        currentMessage={runStatus?.currentMessage || ""}
        awaitingUserAction={runStatus?.awaitingUserAction || ""}
        runStatus={runStatus}
        composerValue={queryText}
        revisionText={flow.revisionText}
        reviewDecision={flow.reviewDecision}
        reviewChecklistConfirmed={flow.reviewChecklistConfirmed}
        promptExamples={promptExamples}
        plan={plan}
        timelineSteps={timelineSteps}
        dashboard={dashboard}
        historyId={flow.id || historyId}
        jobId={flow.jobId}
        selectedCandidateId={flow.selectedCandidateId}
        isHydratingCandidates={isHydratingCandidates}
        candidateHydrationError={candidateHydrationError}
        isLoadingResults={isLoadingResults}
        idleSupplementalContent={
          ENABLE_EXCEL_INTAKE_WORKFLOW ? (
            <ExcelWorkflowIntakePanel
              isSubmitting={isStartingExcelWorkflow}
              onLaunch={startExcelWorkflow}
              onPollGroupProgress={pollExcelBatchGroupProgress}
              onOpenHistory={openExcelBatchHistory}
              onImportTargetCandidates={(jobId, groupHistoryId) =>
                sourcingBackendClient.importTargetCandidatesFromJob(jobId, groupHistoryId)
              }
              onExportTargetCandidates={(jobId, groupHistoryId) =>
                sourcingBackendClient.exportTargetCandidatesForJob(jobId, groupHistoryId)
              }
            />
          ) : null
        }
        planPlaceholderBody={
          isExcelIntakeHistory ? "Excel 批量导入工作流不适用检索方案，结果会直接进入执行过程与候选人看板。" : undefined
        }
        isGeneratingPlan={isGeneratingPlan}
        isApplyingRevision={isApplyingRevision}
        isConfirmingPlan={isConfirmingPlan}
        isContinuingStage2={isContinuingStage2}
        onQueryChange={setQueryText}
        onSubmitSearch={submitSearch}
        onPickPrompt={setQueryText}
        onRevisionChange={(value) => {
          const nextFlow = updateFlow((current) => ({
            ...current,
            revisionText: value,
          }));
          writeDemoSession({
            revisionText: value,
          });
        }}
        onReviewDecisionChange={(patch) => {
          updateFlow((current) => ({
            ...current,
            reviewDecision: {
              ...current.reviewDecision,
              ...patch,
            },
          }));
        }}
        onReviewChecklistChange={(value) => {
          updateFlow((current) => ({
            ...current,
            reviewChecklistConfirmed: value,
          }));
        }}
        onApplyRevision={applyRevision}
        onConfirmPlan={confirmPlan}
        onContinueStage2={continueStage2}
        onSelectedCandidateChange={syncSelectedCandidate}
        onRefreshDashboard={() => {
          void refreshDashboardForCurrentJob();
        }}
      />
    </section>
  );
}
