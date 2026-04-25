import { useMemo } from "react";
import type { CandidateDetail } from "../types";
import { Avatar } from "./Avatar";
import { getFormattedEducationExperience, getFormattedWorkExperience } from "../lib/profileFormatting";

interface OnePageDrawerProps {
  candidate: CandidateDetail | null;
  isOpen: boolean;
  isLoading: boolean;
  onClose: () => void;
}

export function OnePageDrawer({ candidate, isOpen, isLoading, onClose }: OnePageDrawerProps) {
  const workExperience = useMemo(() => (candidate ? getFormattedWorkExperience(candidate, 8) : []), [candidate]);
  const educationExperience = useMemo(() => (candidate ? getFormattedEducationExperience(candidate, 6) : []), [candidate]);

  if (!isOpen) {
    return null;
  }

  return (
    <div className="drawer-backdrop" onClick={onClose} role="presentation">
      <aside className="one-page-drawer" onClick={(event) => event.stopPropagation()}>
        <div className="drawer-head">
          <div>
            <span className="section-step">Profile</span>
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
                  <strong>技能标签</strong>
                  <p>{candidate.focusAreas.join(" / ") || "待补充"}</p>
                </div>
                <div>
                  <strong>LinkedIn</strong>
                  {candidate.linkedinUrl ? (
                    <a href={candidate.linkedinUrl} target="_blank" rel="noreferrer" className="evidence-link">
                      <strong>打开原始资料</strong>
                      <span>{candidate.linkedinUrl}</span>
                    </a>
                  ) : (
                    <p>待补充</p>
                  )}
                </div>
              </div>
            </section>

            <section className="drawer-card">
              <h4>工作经历</h4>
              <div className="drawer-timeline">
                {workExperience.length > 0 ? workExperience.map((item) => (
                  <div key={item} className="drawer-timeline-item">
                    <span className="timeline-dot-mini" />
                    <p>{item}</p>
                  </div>
                )) : <p className="muted">暂无可结构化提取的工作经历。</p>}
              </div>
            </section>

            <section className="drawer-card">
              <h4>教育经历</h4>
              <div className="drawer-timeline">
                {educationExperience.length > 0 ? educationExperience.map((item) => (
                  <div key={item} className="drawer-timeline-item">
                    <span className="timeline-dot-mini" />
                    <p>{item}</p>
                  </div>
                )) : <p className="muted">暂无可结构化提取的教育经历。</p>}
              </div>
            </section>
          </div>
        )}
      </aside>
    </div>
  );
}
