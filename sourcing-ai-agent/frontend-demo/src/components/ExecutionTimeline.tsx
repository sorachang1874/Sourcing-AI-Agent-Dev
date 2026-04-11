import { useEffect, useMemo, useState } from "react";
import type { SearchTimelineStep } from "../types";

interface ExecutionTimelineProps {
  steps: SearchTimelineStep[];
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

export function ExecutionTimeline({ steps }: ExecutionTimelineProps) {
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
        <span className="section-step">Step 2</span>
        <h3>Agent 执行时间线</h3>
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
              <p>{step.detail}</p>
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
