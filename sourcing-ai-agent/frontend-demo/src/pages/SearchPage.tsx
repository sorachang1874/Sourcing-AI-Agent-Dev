import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { OnePageDrawer } from "../components/OnePageDrawer";
import { SearchFlow } from "../components/SearchFlow";
import { readDemoSession, writeDemoSession } from "../lib/demoSession";
import {
  clearRuntimeApiBaseUrl,
  getCandidateDetail,
  getConfiguredApiBaseUrl,
  getDashboard,
  isApiBaseUrlRuntimeConfigurable,
  saveRuntimeApiBaseUrl,
} from "../lib/api";
import {
  readSearchHistoryItem,
  startNewSearchEventName,
  summarizeSearchQuery,
  upsertSearchHistoryItem,
} from "../lib/searchHistory";
import { sourcingBackendClient } from "../lib/sourcingBackend";
import {
  buildTimelineSteps,
  createHistorySnapshot,
  hydrateHistoryWithResult,
} from "../lib/workflow";
import type { Candidate, CandidateDetail, DashboardData, DemoPlan, SearchHistoryItem, SearchTimelineStep, WorkflowPhase } from "../types";

const promptExamples = [
  "帮我找 Google DeepMind 做多模态视频生成的华人研究员",
  "找 OpenAI 里做 agent 产品化、最好有创业经历的 applied AI engineer",
  "想看 Anthropic 做 model optimization 的华人工程师，优先在职",
  "找 Thinking Machines Lab 偏 research engineer 的候选人，排除纯 academia",
];

const emptyFlow: SearchHistoryItem = {
  id: "",
  createdAt: "",
  queryText: "",
  summary: "",
  phase: "idle",
  errorMessage: "",
  plan: null,
  reviewId: "",
  jobId: "",
  revisionText: "",
  requiresReview: false,
  timelineSteps: [],
  selectedCandidateId: "",
};

export function SearchPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const historyId = searchParams.get("history") || "";
  const [flow, setFlow] = useState<SearchHistoryItem>(emptyFlow);
  const [queryText, setQueryText] = useState("");
  const [apiBaseUrlInput, setApiBaseUrlInput] = useState(() => getConfiguredApiBaseUrl());
  const [configuredApiBaseUrl, setConfiguredApiBaseUrl] = useState(() => getConfiguredApiBaseUrl());
  const [apiConfigMessage, setApiConfigMessage] = useState("");
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [isGeneratingPlan, setIsGeneratingPlan] = useState(false);
  const [isApplyingRevision, setIsApplyingRevision] = useState(false);
  const [isConfirmingPlan, setIsConfirmingPlan] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerLoading, setDrawerLoading] = useState(false);
  const [drawerCandidate, setDrawerCandidate] = useState<CandidateDetail | null>(null);
  const timerRef = useRef<number | null>(null);
  const apiBaseUrlRuntimeConfigurable = isApiBaseUrlRuntimeConfigurable();

  const resetComposer = () => {
    stopTimelineAnimation();
    setFlow(emptyFlow);
    setQueryText("");
    setDashboard(null);
    setDrawerOpen(false);
    setDrawerLoading(false);
    setDrawerCandidate(null);
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
      dashboard: null,
    });
    setSearchParams({}, { replace: true });
  };

  const persistFlow = (nextFlow: SearchHistoryItem, nextDashboard: DashboardData | null = dashboard) => {
    setFlow(nextFlow);
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
      dashboard: nextDashboard,
    });
  };

  const recoverInterruptedPlan = (item: SearchHistoryItem): SearchHistoryItem => {
    if (item.plan || item.jobId || item.phase !== "plan" || item.errorMessage) {
      return item;
    }
    const createdAt = new Date(item.createdAt).getTime();
    const expired = Number.isFinite(createdAt) && Date.now() - createdAt > 20_000;
    if (!expired) {
      return item;
    }
    return {
      ...item,
      errorMessage: "检索方案生成未完成，请重新提交搜索。",
    };
  };

  const stopTimelineAnimation = () => {
    if (timerRef.current) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  };

  const handleSaveApiBaseUrl = () => {
    const saved = saveRuntimeApiBaseUrl(apiBaseUrlInput);
    setConfiguredApiBaseUrl(saved);
    setApiBaseUrlInput(saved);
    setApiConfigMessage(saved ? "后端地址已保存，后续搜索会直接走这个 API。" : "已清空后端地址。");
    if (saved) {
      setErrorMessage("");
    }
  };

  const handleClearApiBaseUrl = () => {
    clearRuntimeApiBaseUrl();
    setConfiguredApiBaseUrl(getConfiguredApiBaseUrl());
    setApiBaseUrlInput("");
    setApiConfigMessage("已清空运行时后端地址配置。");
  };

  const startProgressPolling = (jobId: string, activeFlow: SearchHistoryItem) => {
    const poll = async () => {
      try {
        const runStatus = await sourcingBackendClient.getWorkflowProgress(jobId);
        const timelinePlan = activeFlow.plan || flow.plan;
        if (!timelinePlan) {
          throw new Error("工作流缺少检索计划上下文，无法继续渲染时间线。");
        }
        const nextSteps = buildTimelineSteps(runStatus, activeFlow.queryText, timelinePlan);
        const nextError =
          runStatus.status === "failed"
            ? runStatus.timeline[runStatus.timeline.length - 1]?.detail || "工作流未能完成，请检查后端采集日志。"
            : "";

        if (runStatus.status === "completed") {
          stopTimelineAnimation();
          try {
            const nextDashboard = await sourcingBackendClient.getWorkflowResults(jobId);
            setDashboard(nextDashboard);
            setErrorMessage("");
            persistFlow(
              {
                ...activeFlow,
                phase: "results",
                timelineSteps: nextSteps,
                selectedCandidateId: nextDashboard.candidates[0]?.id || "",
              },
              nextDashboard,
            );
          } catch (error) {
            const message = error instanceof Error ? error.message : "结果看板加载失败。";
            setErrorMessage(message);
            setDashboard(null);
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
          timelineSteps: nextSteps,
        };
        setErrorMessage(nextError);
        persistFlow(runningFlow, null);

        if (runStatus.status === "failed") {
          stopTimelineAnimation();
          return;
        }

        timerRef.current = window.setTimeout(() => {
          void poll();
        }, runStatus.status === "blocked" ? 3500 : 3000);
      } catch {
        timerRef.current = window.setTimeout(() => {
          void poll();
        }, 3500);
      }
    };

    void poll();
  };

  const hydrateFromHistory = async (item: SearchHistoryItem) => {
    const recoveredItem = recoverInterruptedPlan(item);
    setFlow(recoveredItem);
    setQueryText(recoveredItem.queryText);
    setErrorMessage(recoveredItem.errorMessage || "");
    if (recoveredItem.id !== item.id || recoveredItem.errorMessage !== item.errorMessage) {
      persistFlow(recoveredItem, null);
    }
    if (recoveredItem.phase === "running" && recoveredItem.jobId && recoveredItem.plan) {
      startProgressPolling(recoveredItem.jobId, recoveredItem);
      return;
    }
    if (recoveredItem.phase !== "results") {
      setDashboard(null);
      return;
    }
    if (!recoveredItem.jobId) {
      setDashboard(null);
      setErrorMessage("历史记录缺少 job_id，无法从后端恢复真实结果，请重新搜索。");
      return;
    }
    try {
      const restoredDashboard = await getDashboard(recoveredItem.jobId);
      const hydrated = hydrateHistoryWithResult(recoveredItem, restoredDashboard);
      setDashboard(restoredDashboard);
      persistFlow(hydrated, restoredDashboard);
    } catch {
      setDashboard(null);
    }
  };

  useEffect(() => {
    stopTimelineAnimation();
    if (!historyId) {
      const session = readDemoSession();
      if (session.activeHistoryId) {
        const activeHistory = readSearchHistoryItem(session.activeHistoryId);
        if (activeHistory) {
          void hydrateFromHistory(activeHistory);
          setSearchParams({ history: activeHistory.id }, { replace: true });
          return;
        }
      }
      setFlow(emptyFlow);
      setQueryText(session.queryText || "");
      setDashboard(session.dashboard || null);
      return;
    }
    const historyItem = readSearchHistoryItem(historyId);
    if (!historyItem) {
      setFlow(emptyFlow);
      setDashboard(null);
      setQueryText("");
      return;
    }
    void hydrateFromHistory(historyItem);
    return () => stopTimelineAnimation();
  }, [historyId, setSearchParams]);

  useEffect(() => () => stopTimelineAnimation(), []);

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
    stopTimelineAnimation();
    setDrawerOpen(false);
    setDrawerCandidate(null);
    setDashboard(null);
    setErrorMessage("");
    setIsGeneratingPlan(true);

    const historyItem = createHistorySnapshot(nextQuery);
    const pendingFlow: SearchHistoryItem = {
      ...historyItem,
      phase: "plan",
      errorMessage: "",
    };
    setSearchParams({ history: historyItem.id });
    persistFlow(pendingFlow, null);
    setQueryText(nextQuery);

    try {
      const { plan: planned, reviewId } = await sourcingBackendClient.planNaturalLanguageSearch(nextQuery);
      if (!planned) {
        throw new Error("后端没有返回有效检索计划。");
      }
      const readyFlow: SearchHistoryItem = {
        ...pendingFlow,
        summary: summarizeSearchQuery(nextQuery),
        plan: planned,
        reviewId,
        requiresReview: planned.reviewRequired,
        errorMessage: "",
      };
      persistFlow(readyFlow, null);
    } catch (error) {
      const message = error instanceof Error ? error.message : "生成检索方案失败，请稍后重试。";
      setErrorMessage(message);
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

  const applyRevision = async () => {
    if (!flow.plan || !flow.queryText) {
      return;
    }
    setIsApplyingRevision(true);
    setErrorMessage("");
    try {
      const { plan: planned, reviewId } = await sourcingBackendClient.planNaturalLanguageSearch(
        [flow.queryText, flow.revisionText].filter(Boolean).join(" "),
      );
      if (!planned) {
        throw new Error("后端没有返回有效修订计划。");
      }
      const nextFlow: SearchHistoryItem = {
        ...flow,
        plan: planned,
        reviewId: reviewId || flow.reviewId,
        requiresReview: planned.reviewRequired,
        errorMessage: "",
      };
      persistFlow(nextFlow, dashboard);
    } catch (error) {
      const message = error instanceof Error ? error.message : "修改方案失败，请稍后重试。";
      setErrorMessage(message);
      persistFlow(
        {
          ...flow,
          errorMessage: message,
        },
        dashboard,
      );
    } finally {
      setIsApplyingRevision(false);
    }
  };

  const confirmPlan = async () => {
    if (!flow.plan) {
      return;
    }
    setIsConfirmingPlan(true);
    stopTimelineAnimation();
    setErrorMessage("");

    try {
      let reviewId = flow.reviewId;
      if (reviewId) {
        await sourcingBackendClient.approvePlan(reviewId);
      }
      const { jobId, runStatus } = await sourcingBackendClient.startWorkflowFromReviewedPlan(reviewId);
      if (!jobId) {
        throw new Error("后端没有返回有效的 job_id，工作流未能启动。");
      }
      const initialSteps = buildTimelineSteps(runStatus, flow.queryText, flow.plan);
      const nextFlow: SearchHistoryItem = {
        ...flow,
        phase: "running",
        errorMessage: "",
        reviewId,
        jobId,
        timelineSteps: initialSteps,
        selectedCandidateId: "",
      };
      setDashboard(null);
      persistFlow(nextFlow, null);
      startProgressPolling(jobId, nextFlow);
    } catch (error) {
      const message = error instanceof Error ? error.message : "启动工作流失败，请检查后端连接。";
      setErrorMessage(message);
      persistFlow(
        {
          ...flow,
          errorMessage: message,
        },
        dashboard,
      );
    } finally {
      setIsConfirmingPlan(false);
    }
  };

  const openOnePage = async (candidate: Candidate) => {
    if (!flow.jobId) {
      setErrorMessage("当前结果没有关联 job_id，无法加载候选人详情，请重新执行检索。");
      return;
    }
    setDrawerOpen(true);
    setDrawerLoading(true);
    setDrawerCandidate(null);
    try {
      const detail = await getCandidateDetail(candidate.id, flow.jobId);
      if (detail) {
        setDrawerCandidate(detail);
        const nextFlow: SearchHistoryItem = {
          ...flow,
          selectedCandidateId: detail.id,
        };
        persistFlow(nextFlow, dashboard);
      }
    } finally {
      setDrawerLoading(false);
    }
  };

  const phase: WorkflowPhase = flow.phase || "idle";
  const plan: DemoPlan | null = flow.plan;
  const timelineSteps: SearchTimelineStep[] = flow.timelineSteps || [];

  return (
    <section className="page">
      {apiBaseUrlRuntimeConfigurable ? (
        <section className="conversation-card backend-config-card">
          <div className="section-heading">
            <span className="section-step">Backend</span>
            <h3>连接后端 API</h3>
          </div>
          <p className="muted backend-config-copy">
            当前后端：
            {" "}
            {configuredApiBaseUrl || "未配置"}
          </p>
          <label className="field-label" htmlFor="backend-api-base-url">
            API Base URL
          </label>
          <input
            id="backend-api-base-url"
            className="backend-config-input"
            type="url"
            value={apiBaseUrlInput}
            placeholder="https://your-backend.example.com"
            onChange={(event) => setApiBaseUrlInput(event.target.value)}
          />
          <div className="action-row">
            <button type="button" className="primary-button" onClick={handleSaveApiBaseUrl}>
              保存后端地址
            </button>
            <button type="button" className="ghost-button" onClick={handleClearApiBaseUrl}>
              清空配置
            </button>
          </div>
          {apiConfigMessage ? <p className="muted backend-config-copy">{apiConfigMessage}</p> : null}
        </section>
      ) : null}

      <SearchFlow
        phase={phase}
        errorMessage={errorMessage}
        displayQuery={flow.queryText || queryText}
        composerValue={queryText}
        revisionText={flow.revisionText}
        promptExamples={promptExamples}
        plan={plan}
        timelineSteps={timelineSteps}
        dashboard={dashboard}
        isGeneratingPlan={isGeneratingPlan}
        isApplyingRevision={isApplyingRevision}
        isConfirmingPlan={isConfirmingPlan}
        onQueryChange={setQueryText}
        onSubmitSearch={submitSearch}
        onRevisionChange={(value) => {
          const nextFlow = {
            ...flow,
            revisionText: value,
          };
          setFlow(nextFlow);
          writeDemoSession({
            revisionText: value,
          });
        }}
        onApplyRevision={applyRevision}
        onConfirmPlan={confirmPlan}
        onOpenOnePage={openOnePage}
      />

      <OnePageDrawer
        candidate={drawerCandidate}
        isOpen={drawerOpen}
        isLoading={drawerLoading}
        onClose={() => setDrawerOpen(false)}
      />
    </section>
  );
}
