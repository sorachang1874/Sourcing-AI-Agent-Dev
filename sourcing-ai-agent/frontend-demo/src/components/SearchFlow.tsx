import { CandidateBoard } from "./CandidateBoard";
import { ExecutionTimeline } from "./ExecutionTimeline";
import { PlanCard } from "./PlanCard";
import { SearchComposer } from "./SearchComposer";
import type { Candidate, DashboardData, DemoPlan, SearchTimelineStep, WorkflowPhase } from "../types";

interface SearchFlowProps {
  phase: WorkflowPhase;
  errorMessage: string;
  displayQuery: string;
  composerValue: string;
  revisionText: string;
  promptExamples: string[];
  plan: DemoPlan | null;
  timelineSteps: SearchTimelineStep[];
  dashboard: DashboardData | null;
  isGeneratingPlan: boolean;
  isApplyingRevision: boolean;
  isConfirmingPlan: boolean;
  onQueryChange: (value: string) => void;
  onSubmitSearch: (value: string) => void;
  onRevisionChange: (value: string) => void;
  onApplyRevision: () => void;
  onConfirmPlan: () => void;
  onOpenOnePage: (candidate: Candidate) => void;
}

export function SearchFlow({
  phase,
  errorMessage,
  displayQuery,
  composerValue,
  revisionText,
  promptExamples,
  plan,
  timelineSteps,
  dashboard,
  isGeneratingPlan,
  isApplyingRevision,
  isConfirmingPlan,
  onQueryChange,
  onSubmitSearch,
  onRevisionChange,
  onApplyRevision,
  onConfirmPlan,
  onOpenOnePage,
}: SearchFlowProps) {
  if (phase === "idle") {
    return (
      <div className="search-flow search-flow-empty">
        <SearchComposer
          value={composerValue}
          isSubmitting={isGeneratingPlan}
          promptExamples={promptExamples}
          onChange={onQueryChange}
          onSubmit={onSubmitSearch}
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

      <section className="conversation-card user-query-card">
        <span className="section-step">搜索需求</span>
        <p>{displayQuery}</p>
      </section>

      {plan ? (
        <PlanCard
          plan={plan}
          revisionText={revisionText}
          isApplyingRevision={isApplyingRevision}
          isConfirming={isConfirmingPlan}
          onRevisionChange={onRevisionChange}
          onApplyRevision={onApplyRevision}
          onConfirm={onConfirmPlan}
        />
      ) : (phase === "plan" || isGeneratingPlan) && !errorMessage ? (
        <section className="conversation-card plan-loading-card">
          <div className="section-heading">
            <span className="section-step">Step 1</span>
            <h3>检索方案生成中</h3>
          </div>
          <div className="plan-loading-body">
            <span className="timeline-pulse plan-loading-pulse" />
            <p>正在根据你的搜索需求生成结构化检索方案，并准备后续执行步骤。</p>
          </div>
        </section>
      ) : null}

      {phase === "running" && timelineSteps.length > 0 ? <ExecutionTimeline steps={timelineSteps} /> : null}

      {phase === "results" && timelineSteps.length > 0 ? <ExecutionTimeline steps={timelineSteps} /> : null}
      {phase === "results" && dashboard ? <CandidateBoard dashboard={dashboard} onOpenOnePage={onOpenOnePage} /> : null}
    </div>
  );
}
