import type { CandidateConfidence } from "../types";

export function ConfidenceBadge({ confidence }: { confidence: CandidateConfidence }) {
  return <span className={`badge confidence-${confidence}`}>{confidence.replace("_", " ")}</span>;
}

export function StatusBadge({ label }: { label: string }) {
  return <span className="badge status-badge">{label}</span>;
}
