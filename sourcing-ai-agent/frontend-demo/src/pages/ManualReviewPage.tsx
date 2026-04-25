import { useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { ManualReviewQueuePanel } from "../components/ManualReviewQueuePanel";
import { SupplementIntakePanel } from "../components/SupplementIntakePanel";
import { writeDemoSession } from "../lib/demoSession";
import { resolveWorkflowPageContext } from "../lib/workflowContext";

export function ManualReviewPage() {
  const [searchParams] = useSearchParams();
  const context = resolveWorkflowPageContext(searchParams);

  useEffect(() => {
    writeDemoSession({ lastVisitedStage: "manual-review" });
  }, []);

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">人工审核队列</p>
          <h2>待确认与待补充候选人</h2>
        </div>
      </header>

      <SupplementIntakePanel />

      <ManualReviewQueuePanel
        key={[context.historyId || "no-history", context.jobId || "no-job", context.candidateId || "no-candidate"].join(":")}
        jobId={context.jobId}
        historyId={context.historyId}
        preferredCandidateId={context.candidateId}
      />
    </section>
  );
}
