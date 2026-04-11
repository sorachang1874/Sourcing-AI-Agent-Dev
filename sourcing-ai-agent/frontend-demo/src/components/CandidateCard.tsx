import { Avatar } from "./Avatar";
import type { Candidate } from "../types";
import { scoreCandidate } from "../lib/workflow";

interface CandidateCardProps {
  candidate: Candidate;
  onOpenOnePage: (candidate: Candidate) => void;
}

function buildExternalLinks(candidate: Candidate): Array<{ label: string; url: string }> {
  const links: Array<{ label: string; url: string }> = [];
  if (candidate.linkedinUrl) {
    links.push({ label: "LinkedIn", url: candidate.linkedinUrl });
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
  if (/华人|Chinese/i.test(candidate.summary)) {
    tags.push("华人");
  }
  return Array.from(new Set(tags)).slice(0, 3);
}

export function CandidateCard({ candidate, onOpenOnePage }: CandidateCardProps) {
  const links = buildExternalLinks(candidate);
  const score = scoreCandidate(candidate);

  return (
    <article className="candidate-card-v2">
      <div className="candidate-card-top">
        <Avatar name={candidate.name} src={candidate.avatarUrl} size="small" />
        <div className="candidate-card-copy">
          <h4>{candidate.name}</h4>
          <p>{candidate.headline}</p>
          <span>{candidate.currentCompany || "当前公司待确认"}</span>
        </div>
        <div className="score-pill">{score}%</div>
      </div>

      <div className="candidate-tag-row">
        {candidateTags(candidate).map((tag) => (
          <span key={`${candidate.id}-${tag}`} className="candidate-tag">
            {tag}
          </span>
        ))}
      </div>

      <p className="candidate-summary">{candidate.summary}</p>

      <div className="candidate-link-row">
        {links.map((link) => (
          <a key={`${candidate.id}-${link.label}`} href={link.url} target="_blank" rel="noreferrer" className="link-chip">
            {link.label}
          </a>
        ))}
      </div>

      <div className="candidate-footer">
        <span className="candidate-confidence">{candidate.confidence.replace("_", " ")}</span>
        <button type="button" className="primary-button small-button" onClick={() => onOpenOnePage(candidate)}>
          查看 One Page
        </button>
      </div>
    </article>
  );
}
