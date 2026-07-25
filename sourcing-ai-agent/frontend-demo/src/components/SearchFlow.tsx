import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { ExecutionTimeline } from "./ExecutionTimeline";
import { ManualReviewQueuePanel } from "./ManualReviewQueuePanel";
import { PlanCard } from "./PlanCard";
import { ResultsBoardPanel } from "./ResultsBoardPanel";
import { SearchComposer } from "./SearchComposer";
import { TargetCandidatesPanel } from "./TargetCandidatesPanel";
import { useCandidateReviewState } from "../hooks/useCandidateReviewState";
import {
  dashboardCandidateBoardBootstrapping,
  dashboardExpectedCandidateCount,
  dashboardHasRenderableCandidates,
} from "../lib/dashboardHydration";
import type {
  CandidateReviewStatus,
  CohortLocationSelection,
  CohortSelection,
  CohortSelectionOptions,
  DashboardData,
  DemoPlan,
  PlanReviewDecision,
  RunStatusData,
  SearchTimelineStep,
  WorkflowPhase,
} from "../types";

type SearchFlowStep = "plan" | "timeline" | "results" | "review" | "targets";

interface SearchFlowProps {
  phase: WorkflowPhase;
  errorMessage: string;
  currentMessage: string;
  awaitingUserAction: string;
  runStatus: RunStatusData | null;
  composerValue: string;
  revisionText: string;
  reviewDecision: PlanReviewDecision;
  reviewChecklistConfirmed: boolean;
  promptExamples: string[];
  cohortSelection: CohortSelection | null;
  cohortOptions: CohortSelectionOptions | null;
  isLoadingCohortOptions: boolean;
  cohortOptionsError: string;
  cohortLocations?: CohortLocationSelection | null;
  plan: DemoPlan | null;
  timelineSteps: SearchTimelineStep[];
  dashboard: DashboardData | null;
  historyId?: string;
  jobId?: string;
  selectedCandidateId?: string;
  isHydratingCandidates?: boolean;
  candidateHydrationError?: string;
  isLoadingResults?: boolean;
  idleSupplementalContent?: ReactNode;
  planPlaceholderBody?: string;
  isGeneratingPlan: boolean;
  isApplyingRevision: boolean;
  isConfirmingPlan: boolean;
  isContinuingStage2: boolean;
  onQueryChange: (value: string) => void;
  onCohortSelectionChange: (value: CohortSelection | null) => void;
  onCohortLocationChange?: (value: CohortLocationSelection) => void;
  onRetryCohortOptions: () => void;
  onSubmitSearch: (value: string) => void;
  onPickPrompt?: (value: string) => void;
  onRevisionChange: (value: string) => void;
  onReviewDecisionChange: (patch: Partial<PlanReviewDecision>) => void;
  onReviewChecklistChange: (value: boolean) => void;
  onApplyRevision: () => void;
  onConfirmPlan: () => void;
  onContinueStage2: () => void;
  onSelectedCandidateChange: (candidateId: string) => void;
  onRefreshDashboard?: () => void;
}

function defaultActiveStep(
  phase: WorkflowPhase,
  hasRenderableDashboard: boolean,
): SearchFlowStep {
  if (phase === "results") {
    return hasRenderableDashboard ? "results" : "timeline";
  }
  if (phase === "running" && hasRenderableDashboard) {
    return "results";
  }
  if (phase === "running") {
    return "timeline";
  }
  return "plan";
}

function StepPlaceholder({
  title,
  body,
}: {
  title: string;
  body: string;
}) {
  return (
    <section className="conversation-card">
      <div className="section-heading">
        <h3>{title}</h3>
      </div>
      <div className="empty-state">
        <p>{body}</p>
      </div>
    </section>
  );
}

const emptyReviewMap: Record<string, CandidateReviewStatus> = {};

export function SearchFlow({
  phase,
  errorMessage,
  currentMessage,
  awaitingUserAction,
  runStatus,
  composerValue,
  revisionText,
  reviewDecision,
  reviewChecklistConfirmed,
  promptExamples,
  cohortSelection,
  cohortOptions,
  isLoadingCohortOptions,
  cohortOptionsError,
  cohortLocations = null,
  plan,
  timelineSteps,
  dashboard,
  historyId = "",
  jobId = "",
  selectedCandidateId = "",
  isHydratingCandidates = false,
  candidateHydrationError = "",
  isLoadingResults = false,
  idleSupplementalContent,
  planPlaceholderBody = "当前还没有可展示的检索方案。",
  isGeneratingPlan,
  isApplyingRevision,
  isConfirmingPlan,
  isContinuingStage2,
  onQueryChange,
  onCohortSelectionChange,
  onCohortLocationChange,
  onRetryCohortOptions,
  onSubmitSearch,
  onPickPrompt,
  onRevisionChange,
  onReviewDecisionChange,
  onReviewChecklistChange,
  onApplyRevision,
  onConfirmPlan,
  onContinueStage2,
  onSelectedCandidateChange,
  onRefreshDashboard,
}: SearchFlowProps) {
  const hasRenderableDashboard = dashboardHasRenderableCandidates(dashboard);
  const [activeStep, setActiveStep] = useState<SearchFlowStep>(() =>
    defaultActiveStep(phase, hasRenderableDashboard),
  );
  const [reviewFocusCandidateId, setReviewFocusCandidateId] = useState("");
  const [resultPanelContextWithUserState, setResultPanelContextWithUserState] = useState("");
  const resultsTabAutoOpenedRef = useRef(false);
  const hasRenderableDashboardRef = useRef(hasRenderableDashboard);
  const candidateBoardBootstrapping = dashboardCandidateBoardBootstrapping(dashboard, candidateHydrationError);
  const {
    effectiveReviewCount,
    reviewStatusMap,
    refresh: refreshReviewState,
  } = useCandidateReviewState(jobId, dashboard?.candidates || []);
  const timelineCandidateCount = dashboardExpectedCandidateCount(dashboard);
  const timelineCandidateCountOverride = timelineCandidateCount > 0 ? timelineCandidateCount : undefined;
  const timelineManualReviewCountOverride = timelineCandidateCount > 0 ? (
    effectiveReviewCount > 0
      ? effectiveReviewCount
      : typeof dashboard?.manualReviewCount === "number"
        ? dashboard.manualReviewCount
        : undefined
  ) : undefined;

  const workflowKey = useMemo(
    () =>
      [
        historyId || "no-history",
        plan?.planId || "no-plan",
        jobId || "no-job",
      ].join(":"),
    [historyId, jobId, plan?.planId],
  );
  const panelContextKey = useMemo(
    () => [historyId || "no-history", jobId || "no-job"].join(":"),
    [historyId, jobId],
  );
  const shouldRenderResultsPanel = Boolean(dashboard) && (
    !candidateBoardBootstrapping ||
    resultPanelContextWithUserState === panelContextKey
  );

  useEffect(() => {
    hasRenderableDashboardRef.current = hasRenderableDashboard;
  }, [hasRenderableDashboard]);

  useEffect(() => {
    setActiveStep(defaultActiveStep(phase, hasRenderableDashboardRef.current));
  }, [workflowKey]);

  // Keep the auto-open memory workflow-scoped. Phase flips within the same
  // job should not re-arm the results tab.
  useEffect(() => {
    resultsTabAutoOpenedRef.current = false;
  }, [workflowKey]);

  useEffect(() => {
    setReviewFocusCandidateId(selectedCandidateId || "");
  }, [selectedCandidateId]);

  useEffect(() => {
    if (hasRenderableDashboard) {
      setResultPanelContextWithUserState(panelContextKey);
    }
  }, [hasRenderableDashboard, panelContextKey]);

  useEffect(() => {
    if (!dashboard) {
      setResultPanelContextWithUserState("");
    }
  }, [dashboard]);

  useEffect(() => {
    if (phase === "running" && awaitingUserAction === "continue_stage2") {
      if (activeStep !== "timeline") {
        setActiveStep("timeline");
      }
      return;
    }

    const shouldAutoOpenResults = hasRenderableDashboard && (phase === "results" || phase === "running");
    if (!shouldAutoOpenResults || resultsTabAutoOpenedRef.current) {
      return;
    }
    resultsTabAutoOpenedRef.current = true;
    if (activeStep !== "results") {
      setActiveStep("results");
    }
  }, [activeStep, awaitingUserAction, hasRenderableDashboard, phase]);

  if (phase === "idle") {
    return (
      <div className="search-flow search-flow-empty">
        {errorMessage ? (
          <section className="warning-card error-card">
            <strong>执行失败</strong>
            <p>{errorMessage}</p>
          </section>
        ) : null}
        <SearchComposer
          value={composerValue}
          isSubmitting={isGeneratingPlan}
          promptExamples={promptExamples}
          afterPrompts={idleSupplementalContent}
          cohortSelection={cohortSelection}
          cohortOptions={cohortOptions}
          isLoadingCohortOptions={isLoadingCohortOptions}
          cohortOptionsError={cohortOptionsError}
          cohortLocations={cohortLocations}
          onChange={onQueryChange}
          onCohortSelectionChange={onCohortSelectionChange}
          onCohortLocationChange={onCohortLocationChange}
          onRetryCohortOptions={onRetryCohortOptions}
          onSubmit={onSubmitSearch}
          onPickPrompt={onPickPrompt}
        />
      </div>
    );
  }

  return (
    <div className="search-flow">
      {errorMessage ? (
        <section className="warning-card error-card">
          <strong>执行失败</strong>
          <p>{errorMessage}</p>
        </section>
      ) : null}

      <section className="workflow-step-shell">
        <div className="workflow-step-tabs" role="tablist" aria-label="Workflow steps">
          <button
            type="button"
            className={`workflow-step-tab${activeStep === "plan" ? " active" : ""}`}
            role="tab"
            aria-selected={activeStep === "plan"}
            data-testid="workflow-step-tab-plan"
            onClick={() => setActiveStep("plan")}
          >
            <strong>检索方案</strong>
          </button>
          <button
            type="button"
            className={`workflow-step-tab${activeStep === "timeline" ? " active" : ""}`}
            role="tab"
            aria-selected={activeStep === "timeline"}
            data-testid="workflow-step-tab-timeline"
            onClick={() => setActiveStep("timeline")}
          >
            <strong>执行过程</strong>
          </button>
          <button
            type="button"
            className={`workflow-step-tab${activeStep === "results" ? " active" : ""}`}
            role="tab"
            aria-selected={activeStep === "results"}
            data-testid="workflow-step-tab-results"
            onClick={() => setActiveStep("results")}
          >
            <strong>候选人看板</strong>
          </button>
          <button
            type="button"
            className={`workflow-step-tab${activeStep === "review" ? " active" : ""}`}
            role="tab"
            aria-selected={activeStep === "review"}
            data-testid="workflow-step-tab-review"
            onClick={() => setActiveStep("review")}
          >
            <strong>人工审核</strong>
          </button>
          <button
            type="button"
            className={`workflow-step-tab${activeStep === "targets" ? " active" : ""}`}
            role="tab"
            aria-selected={activeStep === "targets"}
            data-testid="workflow-step-tab-targets"
            onClick={() => setActiveStep("targets")}
          >
            <strong>目标候选人</strong>
          </button>
        </div>

        <div className="workflow-step-panel">
          {activeStep === "plan" ? (
            plan ? (
              <PlanCard
                plan={plan}
                revisionText={revisionText}
                reviewDecision={reviewDecision}
                cohortOptions={cohortOptions}
                isLoadingCohortOptions={isLoadingCohortOptions}
                cohortOptionsError={cohortOptionsError}
                reviewChecklistConfirmed={reviewChecklistConfirmed}
                isApplyingRevision={isApplyingRevision}
                isConfirming={isConfirmingPlan}
                onRevisionChange={onRevisionChange}
                onReviewDecisionChange={onReviewDecisionChange}
                onRetryCohortOptions={onRetryCohortOptions}
                onReviewChecklistChange={onReviewChecklistChange}
                onApplyRevision={onApplyRevision}
                onConfirm={onConfirmPlan}
              />
            ) : (phase === "plan" || isGeneratingPlan) && !errorMessage ? (
              <section className="conversation-card plan-loading-card">
                <div className="section-heading">
                  <h3>检索方案生成中</h3>
                </div>
                <div className="plan-loading-body">
                  <span className="timeline-pulse plan-loading-pulse" />
                  <p>正在根据你的搜索需求生成结构化检索方案，并准备后续执行步骤。</p>
                </div>
              </section>
            ) : (
              <StepPlaceholder title="检索方案" body={planPlaceholderBody} />
            )
          ) : null}

          {activeStep === "timeline" ? (
            <>
              {runStatus ? (
                <section className="conversation-card run-status-card">
                  <div className="section-heading">
                    <h3>{runStatus.currentStage || "Workflow"}</h3>
                  </div>
                  {currentMessage ? <p className="muted run-status-copy">{currentMessage}</p> : null}
                  <div className="run-status-metrics" data-testid="run-status-metrics">
                    {runStatus.metrics.map((metric) => (
                      <div key={metric.label} className="run-status-metric-card">
                        <span className="field-label">{metric.label}</span>
                        <strong>{metric.value}</strong>
                      </div>
                    ))}
                  </div>
                </section>
              ) : null}

              {timelineSteps.length > 0 ? (
                <ExecutionTimeline
                  key={`timeline:${panelContextKey}`}
                  steps={timelineSteps}
                  candidateCountOverride={timelineCandidateCountOverride}
                  manualReviewCountOverride={timelineManualReviewCountOverride}
                />
              ) : (
                <StepPlaceholder title="执行过程" body="当前还没有可展示的执行时间线。" />
              )}

              {phase === "running" && awaitingUserAction === "continue_stage2" ? (
                <section className="conversation-card warning-card">
                  <div className="section-heading">
                    <h3>等待继续执行</h3>
                  </div>
                  <p>
                    {currentMessage || "Stage 1 preview 已完成，当前等待继续进入 Stage 2 分析与最终结果生成。"}
                  </p>
                  <div className="action-row">
                    <button
                      type="button"
                      className="primary-button"
                      onClick={onContinueStage2}
                      disabled={isContinuingStage2}
                    >
                      {isContinuingStage2 ? "继续中..." : "继续执行 Stage 2"}
                    </button>
                  </div>
                </section>
              ) : null}
            </>
          ) : null}

          {activeStep === "results" ? (
            shouldRenderResultsPanel && dashboard ? (
              <ResultsBoardPanel
                key={`results:${panelContextKey}`}
                dashboard={dashboard}
                projectionId={dashboard.projectionId || dashboard.resultViewLifecycle?.servingProjectionId || ""}
                historyId={historyId}
                jobId={jobId}
                initialCandidateId={selectedCandidateId}
                isHydratingCandidates={isHydratingCandidates}
                candidateHydrationError={candidateHydrationError}
                reviewStatusMap={reviewStatusMap || emptyReviewMap}
                onSelectedCandidateChange={onSelectedCandidateChange}
                onOpenManualReview={(candidateId) => {
                  onSelectedCandidateChange(candidateId);
                  setReviewFocusCandidateId(candidateId);
                  setActiveStep("review");
                }}
                onReviewStateChanged={() => {
                  void refreshReviewState();
                  onRefreshDashboard?.();
                }}
              />
            ) : isLoadingResults || candidateBoardBootstrapping ? (
              <section className="conversation-card plan-loading-card" data-testid="results-loading-card">
                <div className="section-heading">
                  <h3>候选人看板加载中</h3>
                </div>
                <div className="plan-loading-body">
                  <span className="timeline-pulse plan-loading-pulse" />
                  <p>最终结果已生成，正在装载候选人看板与首批候选人。</p>
                </div>
              </section>
            ) : (
              <StepPlaceholder
                title="候选人看板"
                body="当前还没有可展示的候选结果，工作流完成后会在这里呈现。"
              />
            )
          ) : null}

          {activeStep === "review" ? (
            <ManualReviewQueuePanel
              key={`review:${panelContextKey}`}
              jobId={jobId}
              historyId={historyId}
              preferredCandidateId={reviewFocusCandidateId || selectedCandidateId}
              supplementalCandidates={dashboard?.candidates}
              onReviewMutated={() => {
                void refreshReviewState();
                onRefreshDashboard?.();
              }}
            />
          ) : null}

          {activeStep === "targets" ? <TargetCandidatesPanel /> : null}
        </div>
      </section>
    </div>
  );
}
