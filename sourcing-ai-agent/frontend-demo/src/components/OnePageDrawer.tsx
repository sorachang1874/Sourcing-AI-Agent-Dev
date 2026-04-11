import { useEffect, useMemo, useState } from "react";
import type { CandidateDetail } from "../types";
import { Avatar } from "./Avatar";
import { buildOutreachDrafts } from "../lib/workflow";

interface OnePageDrawerProps {
  candidate: CandidateDetail | null;
  isOpen: boolean;
  isLoading: boolean;
  onClose: () => void;
}

type MessageTab = "linkedin" | "email";

export function OnePageDrawer({ candidate, isOpen, isLoading, onClose }: OnePageDrawerProps) {
  const [activeTab, setActiveTab] = useState<MessageTab>("linkedin");
  const drafts = useMemo(() => (candidate ? buildOutreachDrafts(candidate) : { linkedin: "", email: "" }), [candidate]);
  const [messages, setMessages] = useState(drafts);
  const [copiedTab, setCopiedTab] = useState<MessageTab | "">("");

  useEffect(() => {
    setMessages(drafts);
  }, [drafts]);

  if (!isOpen) {
    return null;
  }

  const activeMessage = activeTab === "linkedin" ? messages.linkedin : messages.email;

  return (
    <div className="drawer-backdrop" onClick={onClose} role="presentation">
      <aside className="one-page-drawer" onClick={(event) => event.stopPropagation()}>
        <div className="drawer-head">
          <div>
            <span className="section-step">One Page</span>
            <h3>{candidate?.name || "候选人详情"}</h3>
          </div>
          <button type="button" className="ghost-button small-button" onClick={onClose}>
            关闭
          </button>
        </div>

        {isLoading || !candidate ? (
          <div className="drawer-loading">
            <p>正在加载候选人资料...</p>
          </div>
        ) : (
          <div className="drawer-body">
            <section className="drawer-card">
              <div className="drawer-profile">
                <Avatar name={candidate.name} src={candidate.avatarUrl} size="large" />
                <div>
                  <h4>{candidate.name}</h4>
                  <p>{candidate.headline}</p>
                  <span>
                    {candidate.currentCompany} · {candidate.location}
                  </span>
                </div>
              </div>
              <div className="candidate-tag-row">
                {candidate.focusAreas.slice(0, 6).map((tag) => (
                  <span key={`${candidate.id}-${tag}`} className="candidate-tag">
                    {tag}
                  </span>
                ))}
              </div>
              <div className="drawer-meta-grid">
                <div>
                  <strong>教育背景</strong>
                  <p>{candidate.education.join(" / ") || "待补充"}</p>
                </div>
                <div>
                  <strong>技能标签</strong>
                  <p>{candidate.focusAreas.join(" / ") || "待补充"}</p>
                </div>
              </div>
            </section>

            <section className="drawer-card">
              <h4>工作经历时间线</h4>
              <div className="drawer-timeline">
                {candidate.experience.map((item) => (
                  <div key={item} className="drawer-timeline-item">
                    <span className="timeline-dot-mini" />
                    <p>{item}</p>
                  </div>
                ))}
              </div>
            </section>

            <section className="drawer-card">
              <h4>代表性论文 / 项目</h4>
              <div className="drawer-links">
                {candidate.evidence.map((item) => (
                  <a key={`${candidate.id}-${item.label}`} href={item.url} target="_blank" rel="noreferrer" className="evidence-link">
                    <strong>{item.label}</strong>
                    <span>{item.excerpt}</span>
                  </a>
                ))}
              </div>
            </section>

            <section className="drawer-card">
              <div className="section-heading section-heading-spread">
                <div>
                  <span className="section-step">沟通话术</span>
                  <h4>个性化 Outreach</h4>
                </div>
                <div className="message-tabs">
                  <button
                    type="button"
                    className={`message-tab${activeTab === "linkedin" ? " active" : ""}`}
                    onClick={() => setActiveTab("linkedin")}
                  >
                    LinkedIn Message
                  </button>
                  <button
                    type="button"
                    className={`message-tab${activeTab === "email" ? " active" : ""}`}
                    onClick={() => setActiveTab("email")}
                  >
                    Email
                  </button>
                </div>
              </div>
              <textarea
                className="message-editor"
                value={activeMessage}
                onChange={(event) =>
                  setMessages((current) => ({
                    ...current,
                    [activeTab]: event.target.value,
                  }))
                }
                rows={activeTab === "email" ? 10 : 7}
              />
              <div className="action-row">
                <button
                  type="button"
                  className="primary-button small-button"
                  onClick={() => {
                    void navigator.clipboard.writeText(activeMessage);
                    setCopiedTab(activeTab);
                    window.setTimeout(() => setCopiedTab(""), 1500);
                  }}
                >
                  {copiedTab === activeTab ? "已复制" : "复制"}
                </button>
              </div>
            </section>
          </div>
        )}
      </aside>
    </div>
  );
}
