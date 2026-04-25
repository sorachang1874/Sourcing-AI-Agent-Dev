import { useEffect, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { OnePageDrawer } from "../components/OnePageDrawer";
import { getCandidateDetail } from "../lib/api";
import { buildWorkflowRoute, resolveWorkflowPageContext } from "../lib/workflowContext";
import type { CandidateDetail } from "../types";

export function CandidatePage() {
  const { candidateId = "" } = useParams();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const context = resolveWorkflowPageContext(searchParams);
  const [candidate, setCandidate] = useState<CandidateDetail | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    let isMounted = true;
    if (!context.jobId || !candidateId) {
      setCandidate(null);
      setErrorMessage("当前候选详情缺少 workflow 上下文，请从结果页重新进入。");
      setIsLoading(false);
      return () => {
        isMounted = false;
      };
    }
    setIsLoading(true);
    setErrorMessage("");
    void getCandidateDetail(candidateId, context.jobId)
      .then((payload) => {
        if (!isMounted) {
          return;
        }
        if (!payload) {
          setCandidate(null);
          setErrorMessage("候选详情不存在或不属于当前 workflow。");
          return;
        }
        setCandidate(payload);
      })
      .catch((error) => {
        if (!isMounted) {
          return;
        }
        setCandidate(null);
        setErrorMessage(error instanceof Error ? error.message : "候选详情加载失败。");
      })
      .finally(() => {
        if (isMounted) {
          setIsLoading(false);
        }
      });
    return () => {
      isMounted = false;
    };
  }, [candidateId, context.jobId]);

  const resultsRoute = buildWorkflowRoute("/results", {
    historyId: context.historyId,
    jobId: context.jobId,
    candidateId: candidate?.id || context.candidateId,
  });
  const closeDrawer = () => navigate(resultsRoute);

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">候选详情</p>
          <h2>{candidate?.name || "经历详情"}</h2>
        </div>
        <div className="stat-pill-row">
          <Link className="ghost-button" to={resultsRoute}>
            返回结果页
          </Link>
          <Link
            className="ghost-button"
            to={buildWorkflowRoute("/manual-review", {
              historyId: context.historyId,
              jobId: context.jobId,
              candidateId: candidate?.id || context.candidateId,
            })}
          >
            查看人工审核
          </Link>
        </div>
      </header>

      {errorMessage ? (
        <section className="warning-card error-card">
          <strong>候选详情加载失败</strong>
          <p>{errorMessage}</p>
        </section>
      ) : null}

      <OnePageDrawer candidate={candidate} isOpen isLoading={isLoading} onClose={closeDrawer} />
    </section>
  );
}
