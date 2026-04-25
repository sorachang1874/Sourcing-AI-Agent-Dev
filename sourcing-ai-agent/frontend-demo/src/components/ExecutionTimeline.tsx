import { useEffect, useMemo, useState } from "react";
import type { SearchTimelineStep } from "../types";

interface ExecutionTimelineProps {
  steps: SearchTimelineStep[];
  candidateCountOverride?: number;
  manualReviewCountOverride?: number;
}

function stepIcon(status: SearchTimelineStep["status"]): string {
  if (status === "completed") {
    return "OK";
  }
  if (status === "running") {
    return "..";
  }
  return "--";
}

function displayDetail(
  step: SearchTimelineStep,
  candidateCountOverride?: number,
  manualReviewCountOverride?: number,
): string {
  const sanitized = step.detail
    .replace(/\bscore\s+\d+(\.\d+)?\b/gi, "")
    .replace(/\bconfidence\s+\d+(\.\d+)?\b/gi, "")
    .replace(/\bweight\s+\d+(\.\d+)?\b/gi, "")
    .replace(/top match is[^。.!?]*/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
  if (
    /score\s+\d+(\.\d+)?/i.test(step.detail) ||
    /top match is/i.test(step.detail) ||
    /confidence|weight/i.test(step.detail)
  ) {
    const candidateCount = candidateCountOverride ?? 0;
    const manualReviewCount = manualReviewCountOverride ?? 0;
    return `当前返回 ${candidateCount} 位候选人，其中 ${manualReviewCount} 位需要人工审核。`;
  }
  if (candidateCountOverride === undefined && manualReviewCountOverride === undefined) {
    return sanitized;
  }
  if (/当前返回\s+\d+\s+位候选人/.test(sanitized) || /待审核\s+\d+\s+位|待审核\s+\d+\s+条/.test(sanitized)) {
    const candidateCount = candidateCountOverride ?? 0;
    const manualReviewCount = manualReviewCountOverride ?? 0;
    return `当前返回 ${candidateCount} 位候选人，其中 ${manualReviewCount} 位需要人工审核。`;
  }
  return sanitized;
}

export function ExecutionTimeline({ steps, candidateCountOverride, manualReviewCountOverride }: ExecutionTimelineProps) {
  const defaultOpenId = useMemo(
    () => steps.find((step) => step.status === "running")?.id || steps[0]?.id || "",
    [steps],
  );
  const [openStepId, setOpenStepId] = useState(defaultOpenId);

  useEffect(() => {
    setOpenStepId((current) => {
      if (current && steps.some((step) => step.id === current)) {
        return current;
      }
      return defaultOpenId;
    });
  }, [defaultOpenId, steps]);

  return (
    <section className="conversation-card">
      <div className="section-heading">
        <h3>执行过程</h3>
      </div>
      <div className="timeline-shell">
        {steps.map((step) => (
          <button
            key={step.id}
            type="button"
            className={`timeline-step timeline-${step.status}${openStepId === step.id ? " open" : ""}`}
            onClick={() => setOpenStepId((current) => (current === step.id ? "" : step.id))}
          >
            <div className="timeline-marker">
              <span>{stepIcon(step.status)}</span>
              {step.status === "running" ? <span className="timeline-pulse" /> : null}
            </div>
            <div className="timeline-content">
              <div className="timeline-head">
                <div className="timeline-copy">
                  <strong>{step.title}</strong>
                  <span>{step.timestamp}</span>
                </div>
                {step.duration ? <em>{step.duration}</em> : null}
              </div>
              <p>{displayDetail(step, candidateCountOverride, manualReviewCountOverride)}</p>
              {openStepId === step.id && step.sources.length > 0 ? (
                <div className="timeline-tags">
                  {step.sources.map((source) => (
                    <span key={`${step.id}-${source.label}`} className="source-tag">
                      {source.label}
                      {source.count ? ` +${source.count}` : ""}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
          </button>
        ))}
      </div>
    </section>
  );
}
