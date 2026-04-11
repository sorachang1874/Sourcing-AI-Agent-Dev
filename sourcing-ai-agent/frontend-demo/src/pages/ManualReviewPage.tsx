import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { StatusBadge } from "../components/Badges";
import { getManualReviewItems } from "../lib/api";
import { writeDemoSession } from "../lib/demoSession";
import type { ManualReviewItem } from "../types";

export function ManualReviewPage() {
  const [items, setItems] = useState<ManualReviewItem[]>([]);
  const [selectedId, setSelectedId] = useState<string>("");
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let isMounted = true;
    setIsLoading(true);
    void getManualReviewItems()
      .then((payload) => {
        if (!isMounted) {
          return;
        }
        setItems(payload);
        setSelectedId(payload[0]?.id ?? "");
        writeDemoSession({ lastVisitedStage: "manual-review" });
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
            <p className="eyebrow">人工审核队列</p>
            <h2>处理低置信度身份与缺失 profile 证据</h2>
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

  const selectedItem = items.find((item) => item.id === selectedId) ?? items[0];

  return (
    <section className="page">
      <header className="page-header split-header">
        <div>
          <p className="eyebrow">人工审核队列</p>
          <h2>处理低置信度身份与缺失 profile 证据</h2>
          <p className="muted">
            这是 sourcing workflow 中的人在回路步骤。它会展示那些还不能自动确认、需要分析师明确判断的候选人。
          </p>
        </div>
        <div className="stat-pill-row">
          <StatusBadge label={`${items.length} 条待处理`} />
          <StatusBadge label="已接入本地 backlog" />
          <Link className="ghost-button" to="/results">
            返回结果页
          </Link>
        </div>
      </header>

      <div className="dashboard-layout">
        <section className="panel">
          <div className="panel-header">
            <h3>审核队列</h3>
          </div>
          <div className="stack">
            {items.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`review-item${selectedId === item.id ? " selected" : ""}`}
                onClick={() => setSelectedId(item.id)}
              >
                <div className="timeline-row">
                  <strong>{item.candidateName}</strong>
                  <StatusBadge label={item.status} />
                </div>
                <p className="muted">{item.reviewType}</p>
                <p className="muted">{item.summary}</p>
              </button>
            ))}
          </div>
        </section>

        <section className="panel candidate-panel">
          {selectedItem && (
            <>
              <div className="panel-header">
                <h3>{selectedItem.candidateName}</h3>
                <StatusBadge label={selectedItem.reviewType} />
              </div>
              <p className="muted">{selectedItem.summary}</p>
              <div className="divider" />
              <h4>建议动作</h4>
              <p className="muted">{selectedItem.recommendedAction}</p>
              <h4>证据</h4>
              <div className="tag-row">
                {selectedItem.evidenceLabels.map((label) => (
                  <span key={label} className="tag">
                    {label}
                  </span>
                ))}
              </div>
              <div className="divider" />
              {selectedItem.candidateId && (
                <p className="muted">候选人 ID：{selectedItem.candidateId}</p>
              )}
              <div className="divider" />
              <div className="action-row">
                <button className="primary-button" type="button">
                  标记为已解决
                </button>
                <button className="ghost-button" type="button">
                  继续研究
                </button>
                {selectedItem.candidateId && (
                  <Link className="ghost-button" to={`/candidate/${selectedItem.candidateId}`}>
                    打开候选详情
                  </Link>
                )}
              </div>
            </>
          )}
        </section>

        <aside className="panel grouping-panel">
          <div className="panel-header">
            <h3>审核说明</h3>
          </div>
          <ul className="flat-list compact">
            <li>这个队列主要处理 unresolved lead、缺少 profile URL 和身份确认场景。</li>
            <li>后续审核动作应映射到后端的 `manual_review_resolution`。</li>
            <li>当前 Demo 已展示决策面板和证据 framing。</li>
          </ul>
        </aside>
      </div>
    </section>
  );
}
