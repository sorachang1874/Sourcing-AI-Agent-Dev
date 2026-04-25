import { Avatar } from "./Avatar";
import type { Candidate } from "../types";
import { resolveCandidateLinkedinUrl } from "../lib/candidatePresentation";
import { getFormattedEducationExperience, getFormattedWorkExperience } from "../lib/profileFormatting";

interface CandidateCardProps {
  candidate: Candidate;
  onOpenOnePage: (candidate: Candidate) => void;
}

function buildExternalLinks(candidate: Candidate): Array<{ label: string; url: string }> {
  const links: Array<{ label: string; url: string }> = [];
  const linkedinUrl = resolveCandidateLinkedinUrl(candidate);
  if (linkedinUrl) {
    links.push({ label: "LinkedIn", url: linkedinUrl });
  }
  const github = candidate.evidence.find((item) => item.type === "github")?.url;
  if (github) {
    links.push({ label: "GitHub", url: github });
  }
  const scholar = candidate.evidence.find((item) => item.type === "publication")?.url;
  if (scholar) {
    links.push({ label: "Scholar", url: scholar });
  }
  return links.slice(0, 3);
}

function candidateTags(candidate: Candidate): string[] {
  const tags = [...candidate.focusAreas, candidate.team];
  const chineseSignals = [
    candidate.headline,
    candidate.notesSnippet || "",
    ...candidate.matchReasons,
    ...candidate.experience,
    ...candidate.education,
  ].join(" ");
  if (/华人|Chinese/i.test(chineseSignals)) {
    tags.push("华人");
  }
  return Array.from(new Set(tags)).slice(0, 3);
}

export function CandidateCard({ candidate, onOpenOnePage }: CandidateCardProps) {
  const links = buildExternalLinks(candidate);
  const linkedinUrl = resolveCandidateLinkedinUrl(candidate);
  const workExperience = getFormattedWorkExperience(candidate, 2);
  const educationExperience = getFormattedEducationExperience(candidate, 1);

  return (
    <article className="candidate-card-v2">
      <div className="candidate-card-top">
        <Avatar name={candidate.name} src={candidate.avatarUrl} size="small" />
        <div className="candidate-card-copy">
          <h4>{candidate.name}</h4>
          <p>{candidate.headline}</p>
          <span>{candidate.currentCompany || "当前公司待确认"}</span>
        </div>
      </div>

      <div className="candidate-tag-row">
        {candidateTags(candidate).map((tag) => (
          <span key={`${candidate.id}-${tag}`} className="candidate-tag">
            {tag}
          </span>
        ))}
      </div>

      <div className="experience-preview-block">
        <div className="candidate-experience-section">
          <strong>工作经历</strong>
          {workExperience.length > 0 ? (
            <ul className="flat-list compact experience-preview-list">
              {workExperience.map((line) => (
                <li key={`${candidate.id}-work-${line}`}>{line}</li>
              ))}
            </ul>
          ) : (
            <p className="candidate-summary">暂无可结构化提取的工作经历。</p>
          )}
        </div>
        <div className="candidate-experience-section">
          <strong>教育经历</strong>
          {educationExperience.length > 0 ? (
            <ul className="flat-list compact experience-preview-list">
              {educationExperience.map((line) => (
                <li key={`${candidate.id}-edu-${line}`}>{line}</li>
              ))}
            </ul>
          ) : (
            <p className="candidate-summary">暂无可结构化提取的教育经历。</p>
          )}
        </div>
      </div>

      <div className="candidate-link-row">
        {links.map((link) => (
          <a key={`${candidate.id}-${link.label}`} href={link.url} target="_blank" rel="noreferrer" className="link-chip">
            {link.label}
          </a>
        ))}
      </div>

      <div className="candidate-footer">
        {linkedinUrl ? (
          <a
            href={linkedinUrl}
            target="_blank"
            rel="noreferrer"
            className="ghost-button small-button"
          >
            打开 LinkedIn
          </a>
        ) : null}
        <button type="button" className="primary-button small-button" onClick={() => onOpenOnePage(candidate)}>
          查看历史记录
        </button>
      </div>
    </article>
  );
}
