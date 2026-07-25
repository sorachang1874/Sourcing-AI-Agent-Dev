import { useEffect, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import { LocalAssetTabs } from "../components/LocalAssetTabs";
import { ResultsBoardPanel } from "../components/ResultsBoardPanel";
import { useDashboardCandidateHydration } from "../hooks/useDashboardCandidateHydration";
import { useCandidateReviewState } from "../hooks/useCandidateReviewState";
import {
  dashboardHasRenderableCandidates,
  getDashboard,
  getProjectionDashboard,
  getRunProjectionId,
  peekProjectionDashboardCache,
} from "../lib/api";
import { writeDemoSession } from "../lib/demoSession";
import { collectionDisplayName } from "../lib/localAssetPresentation";
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
  const navigate = useNavigate();
  const routeParams = useParams();
  const context = resolveWorkflowPageContext(searchParams);
  const collectionId = (searchParams.get("collection") || "").trim();
  const routeProjectionId = (routeParams.projectionId || "").trim();
  const effectiveProjectionId = routeProjectionId || context.projectionId;
  const [dashboard, setDashboard] = useState<DashboardData>(
    () => peekProjectionDashboardCache(effectiveProjectionId) || emptyDashboard,
  );
  const { reviewStatusMap, refresh } = useCandidateReviewState(context.jobId, dashboard.candidates);
  const [candidateHydrationWindow, setCandidateHydrationWindow] = useState({
    requiredCandidateCount: 96,
    backgroundCandidateCount: 168,
  });
  const [isLoading, setIsLoading] = useState(
    () => !dashboardHasRenderableCandidates(peekProjectionDashboardCache(effectiveProjectionId)),
  );
  const [errorMessage, setErrorMessage] = useState("");
  const {
    isHydratingCandidates,
    candidateHydrationError,
  } = useDashboardCandidateHydration({
    jobId: context.jobId,
    projectionId: effectiveProjectionId,
    dashboard: context.jobId || effectiveProjectionId ? dashboard : null,
    onDashboardChange: setDashboard,
    requiredCandidateCount: candidateHydrationWindow.requiredCandidateCount,
    backgroundCandidateCount: candidateHydrationWindow.backgroundCandidateCount,
  });
  const displayCompanyName = collectionId
    ? collectionDisplayName({ collectionId, displayName: dashboard.targetCompany })
    : dashboard.targetCompany;

  useEffect(() => {
    let isMounted = true;
    if (!context.jobId && !effectiveProjectionId) {
      setDashboard(emptyDashboard);
      setErrorMessage("当前没有可恢复的 projection，请先从执行页或本地资产入口进入结果页。");
      setIsLoading(false);
      return () => {
        isMounted = false;
      };
    }
    const cachedDashboard = peekProjectionDashboardCache(effectiveProjectionId);
    if (cachedDashboard) {
      setDashboard(cachedDashboard);
      setIsLoading(!dashboardHasRenderableCandidates(cachedDashboard));
    } else {
      setIsLoading(true);
    }
    setErrorMessage("");
    const loadDashboard = async () => {
      const projectionId = effectiveProjectionId || await getRunProjectionId(context.jobId);
      if (!effectiveProjectionId) {
        const params = new URLSearchParams();
        if (context.historyId) {
          params.set("history", context.historyId);
        }
        if (context.jobId) {
          params.set("job", context.jobId);
        }
        if (context.candidateId) {
          params.set("candidate", context.candidateId);
        }
        navigate(`/projections/${encodeURIComponent(projectionId)}${params.toString() ? `?${params.toString()}` : ""}`, {
          replace: true,
        });
      }
      return effectiveProjectionId
        ? getProjectionDashboard(projectionId, {
            forceRefresh: !dashboardHasRenderableCandidates(cachedDashboard),
            runId: context.jobId,
          })
        : getProjectionDashboard(projectionId, {
            forceRefresh: true,
            runId: context.jobId,
          });
    };
    void loadDashboard()
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
  }, [context.candidateId, context.historyId, context.jobId, effectiveProjectionId, navigate]);

  if (isLoading) {
    return (
      <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">候选人看板</p>
          <h2>候选人结果看板</h2>
        </div>
      </header>
      {collectionId ? <LocalAssetTabs active="board" collectionId={collectionId} projectionId={effectiveProjectionId} /> : null}
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
          <h2>{displayCompanyName ? `${displayCompanyName} 候选人看板` : "候选人结果看板"}</h2>
        </div>
      </header>
      {collectionId ? (
        <LocalAssetTabs
          active="board"
          collectionId={collectionId}
          companyName={displayCompanyName}
          projectionId={effectiveProjectionId || dashboard.projectionId || ""}
          jobId={context.jobId}
          historyId={context.historyId}
          candidateId={context.candidateId}
        />
      ) : null}

      {errorMessage ? (
        <section className="warning-card error-card">
          <strong>结果加载失败</strong>
          <p>{errorMessage}</p>
        </section>
      ) : null}

      <ResultsBoardPanel
        key={[context.historyId || "no-history", context.jobId || "no-job", effectiveProjectionId || "no-projection"].join(":")}
        dashboard={dashboard}
        historyId={context.historyId}
        jobId={context.jobId}
        projectionId={effectiveProjectionId || dashboard.projectionId || ""}
        collectionId={collectionId}
        initialCandidateId={context.candidateId}
        isHydratingCandidates={isHydratingCandidates}
        candidateHydrationError={candidateHydrationError}
        reviewStatusMap={reviewStatusMap}
        onHydrationWindowChange={setCandidateHydrationWindow}
        onReviewStateChanged={() => {
          void refresh();
        }}
      />
    </section>
  );
}
