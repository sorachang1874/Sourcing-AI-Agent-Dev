import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { StatusBadge } from "../components/Badges";
import { getRunStatus } from "../lib/api";
import { writeDemoSession } from "../lib/demoSession";
import { formatWorkflowTimestamp } from "../lib/time";
import type { RunStatusData } from "../types";

function formatEventTime(startedAt: string, completedAt: string): string {
  return formatWorkflowTimestamp(completedAt || startedAt);
}

const emptyRunStatus: RunStatusData = {
  jobId: "--",
  status: "queued",
  currentStage: "Loading",
  startedAt: "--",
  metrics: [],
  timeline: [],
  workers: [],
};

export function RunStatusPage() {
  const [runStatus, setRunStatus] = useState<RunStatusData | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let isMounted = true;
    setIsLoading(true);
    void getRunStatus()
      .then((payload) => {
        if (isMounted) {
          setRunStatus(payload);
          writeDemoSession({ lastVisitedStage: "run" });
        }
      })
      .finally(() => {
        if (isMounted) {
          setIsLoading(false);
        }
      });
    return () => {
      isMounted = false;
    };
  }, []);

  if (isLoading) {
    return (
      <section className="page">
        <header className="page-header split-header">
          <div>
            <p className="eyebrow">执行过程</p>
            <h2>执行采集与候选富化</h2>
          </div>
          <div className="stat-pill-row">
            <StatusBadge label="正在加载本地资产..." />
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

  const status = runStatus || emptyRunStatus;

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">执行过程</p>
          <h2>执行采集与候选富化</h2>
          <p className="muted">
            这个页面把后端 workflow 转成可读的执行时间线，让 Demo 在结果出来之前也能展示系统正在做什么。
          </p>
        </div>
        <div className="stat-pill-row">
          <StatusBadge label={status.jobId} />
          <StatusBadge label={status.status} />
          <StatusBadge label={`Stage ${status.currentStage}`} />
        </div>
      </header>

      <div className="content-grid three-column">
        <section className="panel">
          <div className="panel-header">
            <h3>运行指标</h3>
          </div>
          <div className="metric-grid">
            {status.metrics.map((metric) => (
              <div key={metric.label} className="metric-card">
                <span className="muted">{metric.label}</span>
                <strong>{metric.value}</strong>
              </div>
            ))}
          </div>
          <div className="divider" />
          <p className="muted">启动时间：{formatWorkflowTimestamp(status.startedAt)}</p>
          <div className="action-row">
            <Link className="primary-button" to="/results">
              查看结果
            </Link>
            <Link className="ghost-button" to="/review">
              返回审核
            </Link>
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <h3>执行时间线</h3>
          </div>
          <div className="timeline">
            {status.timeline.map((event) => (
              <div key={event.id} className="timeline-item">
                <div className="timeline-dot" />
                <div>
                  <div className="timeline-row">
                    <strong>{event.title || event.stage}</strong>
                    <span className="muted">{formatEventTime(event.startedAt, event.completedAt)}</span>
                  </div>
                  <p className="muted">{event.detail}</p>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <h3>Workers / Lanes</h3>
          </div>
          <div className="stack">
            {status.workers.map((worker) => (
              <div key={worker.id} className="worker-card">
                <div className="timeline-row">
                  <strong>{worker.lane}</strong>
                  <StatusBadge label={worker.status} />
                </div>
                <p className="muted">{worker.id}</p>
                <p className="muted">{worker.budget}</p>
              </div>
            ))}
          </div>
        </section>
      </div>
    </section>
  );
}
