import { useMemo, useState } from "react";
import { CandidateCard } from "./CandidateCard";
import type { Candidate, DashboardData } from "../types";
import { tierForCandidate } from "../lib/workflow";

interface CandidateBoardProps {
  dashboard: DashboardData;
  onOpenOnePage: (candidate: Candidate) => void;
}

type CandidateTab = "tier1" | "tier2" | "all";

const tabs: Array<{ id: CandidateTab; label: string }> = [
  { id: "tier1", label: "完全符合（Tier 1）" },
  { id: "tier2", label: "一般符合（Tier 2）" },
  { id: "all", label: "全部候选人" },
];

export function CandidateBoard({ dashboard, onOpenOnePage }: CandidateBoardProps) {
  const [activeTab, setActiveTab] = useState<CandidateTab>("tier1");

  const visibleCandidates = useMemo(() => {
    if (activeTab === "all") {
      return dashboard.candidates;
    }
    return dashboard.candidates.filter((candidate) => tierForCandidate(candidate) === activeTab);
  }, [activeTab, dashboard.candidates]);

  return (
    <section className="conversation-card candidate-board">
      <div className="section-heading section-heading-spread">
        <div>
          <span className="section-step">Step 3</span>
          <h3>候选人结果看板</h3>
        </div>
        <div className="board-stats">
          <span>{dashboard.totalCandidates} 位候选人</span>
          <span>{dashboard.totalEvidence} 条证据</span>
        </div>
      </div>

      <div className="board-tabs">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            className={`board-tab${activeTab === tab.id ? " active" : ""}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="candidate-board-grid">
        {visibleCandidates.map((candidate) => (
          <CandidateCard key={candidate.id} candidate={candidate} onOpenOnePage={onOpenOnePage} />
        ))}
      </div>

      {visibleCandidates.length === 0 ? (
        <div className="empty-state">
          <p>当前层级还没有候选人。</p>
          <span>可以切换到“全部候选人”查看完整结果池。</span>
        </div>
      ) : null}
    </section>
  );
}
