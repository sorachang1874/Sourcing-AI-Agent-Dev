import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ResultsBoardPanel } from "../components/ResultsBoardPanel";
import { useDashboardCandidateHydration } from "../hooks/useDashboardCandidateHydration";
import { useCandidateReviewState } from "../hooks/useCandidateReviewState";
import { dashboardHasRenderableCandidates, getDashboard, peekDashboardCache } from "../lib/api";
import { writeDemoSession } from "../lib/demoSession";
import { resolveWorkflowPageContext } from "../lib/workflowContext";
import type { DashboardData } from "../types";

const emptyDashboard: DashboardData = {
  title: "候选人结果看板",
  snapshotId: "--",
  queryLabel: "",
  targetCompany: "",
  intentKeywords: [],
  resultMode: "ranked_results",
  resultModeLabel: "检索排序结果",
  rankedCandidateCount: 0,
  assetPopulationCount: 0,
  totalCandidates: 0,
  totalEvidence: 0,
  manualReviewCount: 0,
  layers: [
    { id: "layer_0", label: "Layer 0", count: 0 },
    { id: "layer_1", label: "Layer 1", count: 0 },
    { id: "layer_2", label: "Layer 2", count: 0 },
    { id: "layer_3", label: "Layer 3", count: 0 },
  ],
  groups: ["All"],
  candidates: [],
};

export function ResultsPage() {
  const [searchParams] = useSearchParams();
  const context = resolveWorkflowPageContext(searchParams);
  const [dashboard, setDashboard] = useState<DashboardData>(() => peekDashboardCache(context.jobId) || emptyDashboard);
  const { reviewStatusMap, refresh } = useCandidateReviewState(context.jobId, dashboard.candidates);
  const [candidateHydrationWindow, setCandidateHydrationWindow] = useState({
    requiredCandidateCount: 96,
    backgroundCandidateCount: 168,
  });
  const [isLoading, setIsLoading] = useState(
    () => !dashboardHasRenderableCandidates(peekDashboardCache(context.jobId)),
  );
  const [errorMessage, setErrorMessage] = useState("");
  const {
    isHydratingCandidates,
    candidateHydrationError,
  } = useDashboardCandidateHydration({
    jobId: context.jobId,
    dashboard: context.jobId ? dashboard : null,
    onDashboardChange: setDashboard,
    requiredCandidateCount: candidateHydrationWindow.requiredCandidateCount,
    backgroundCandidateCount: candidateHydrationWindow.backgroundCandidateCount,
  });

  useEffect(() => {
    let isMounted = true;
    if (!context.jobId) {
      setDashboard(emptyDashboard);
      setErrorMessage("当前没有可恢复的 workflow job，请先在搜索页完成一次真实执行。");
      setIsLoading(false);
      return () => {
        isMounted = false;
      };
    }
    const cachedDashboard = peekDashboardCache(context.jobId);
    if (cachedDashboard) {
      setDashboard(cachedDashboard);
      setIsLoading(!dashboardHasRenderableCandidates(cachedDashboard));
    } else {
      setIsLoading(true);
    }
    setErrorMessage("");
    void getDashboard(context.jobId, {
      forceRefresh: !dashboardHasRenderableCandidates(cachedDashboard),
    })
      .then((payload) => {
        if (!isMounted) {
          return;
        }
        setDashboard(payload);
        writeDemoSession({ lastVisitedStage: "results" });
      })
      .catch((error) => {
        if (!isMounted) {
          return;
        }
        setDashboard(cachedDashboard || emptyDashboard);
        setErrorMessage(error instanceof Error ? error.message : "结果加载失败。");
      })
      .finally(() => {
        if (isMounted) {
          setIsLoading(false);
        }
      });
    return () => {
      isMounted = false;
    };
  }, [context.jobId]);

  if (isLoading) {
    return (
      <section className="page">
        <header className="page-header split-header">
          <div>
            <p className="eyebrow">候选人看板</p>
            <h2>候选人结果看板</h2>
          </div>
        </header>
        <section className="panel">
          <div className="results-skeleton">
            <div className="skeleton-line short" />
            <div className="skeleton-line" />
            <div className="skeleton-line" />
          </div>
        </section>
      </section>
    );
  }

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">候选人看板</p>
          <h2>{dashboard.targetCompany ? `${dashboard.targetCompany} 候选人看板` : "候选人结果看板"}</h2>
        </div>
      </header>

      {errorMessage ? (
        <section className="warning-card error-card">
          <strong>结果加载失败</strong>
          <p>{errorMessage}</p>
        </section>
      ) : null}

      <ResultsBoardPanel
        key={[context.historyId || "no-history", context.jobId || "no-job", dashboard.snapshotId || "no-snapshot"].join(":")}
        dashboard={dashboard}
        historyId={context.historyId}
        jobId={context.jobId}
        initialCandidateId={context.candidateId}
        isHydratingCandidates={isHydratingCandidates}
        candidateHydrationError={candidateHydrationError}
        totalCandidateCount={dashboard.totalCandidates}
        reviewStatusMap={reviewStatusMap}
        onHydrationWindowChange={setCandidateHydrationWindow}
        onReviewStateChanged={() => {
          void refresh();
        }}
      />
    </section>
  );
}
