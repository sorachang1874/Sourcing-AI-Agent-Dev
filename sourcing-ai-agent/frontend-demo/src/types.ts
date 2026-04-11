export type PlanStatus = "draft" | "pending_review" | "approved";

export type CandidateConfidence = "high" | "medium" | "lead_only";
export type WorkflowPhase = "idle" | "plan" | "running" | "results";
export type TimelineStepStatus = "completed" | "running" | "pending";

export interface DemoPlan {
  planId: string;
  rawUserRequest: string;
  targetCompany: string;
  targetPopulation: string;
  projectScope: string;
  keywords: string[];
  acquisitionStrategy: string;
  searchStrategy: string[];
  estimatedCostLevel: "low" | "medium" | "high";
  reviewRequired: boolean;
  status: PlanStatus;
}

export interface TimelineSourceTag {
  label: string;
  count?: number;
}

export interface SearchTimelineStep {
  id: string;
  title: string;
  detail: string;
  timestamp: string;
  status: TimelineStepStatus;
  duration?: string;
  sources: TimelineSourceTag[];
}

export interface CandidateEvidence {
  label: string;
  type: "linkedin" | "publication" | "homepage" | "github" | "cv";
  url: string;
  excerpt: string;
}

export interface CandidateExternalLink {
  label: string;
  url: string;
  type: "email" | "github" | "twitter" | "scholar" | "website";
}

export interface Candidate {
  id: string;
  name: string;
  headline: string;
  avatarUrl: string;
  team: string;
  employmentStatus: "current" | "former" | "lead";
  confidence: CandidateConfidence;
  summary: string;
  currentCompany?: string;
  location?: string;
  linkedinUrl?: string;
  sourceDataset?: string;
  notesSnippet?: string;
  focusAreas: string[];
  matchReasons: string[];
  education: string[];
  experience: string[];
  evidence: CandidateEvidence[];
  externalLinks?: CandidateExternalLink[];
}

export interface CandidateDetail extends Candidate {
  currentCompany: string;
  location: string;
  aliases: string[];
  sourceSummary: string[];
  documents: {
    title: string;
    body: string;
  }[];
}

export interface FunnelLayer {
  id: string;
  label: string;
  count: number;
}

export interface DashboardData {
  title: string;
  snapshotId: string;
  queryLabel: string;
  totalCandidates: number;
  totalEvidence: number;
  manualReviewCount: number;
  layers: FunnelLayer[];
  groups: string[];
  candidates: Candidate[];
}

export interface RunMetric {
  label: string;
  value: string;
}

export interface RunEvent {
  id: string;
  stage: string;
  title: string;
  detail: string;
  status: string;
  startedAt: string;
  completedAt: string;
  sourceTags: TimelineSourceTag[];
}

export interface RunWorker {
  id: string;
  lane: string;
  status: string;
  budget: string;
}

export interface RunStatusData {
  jobId: string;
  status: "queued" | "running" | "completed" | "blocked" | "failed";
  currentStage: string;
  startedAt: string;
  metrics: RunMetric[];
  timeline: RunEvent[];
  workers: RunWorker[];
}

export interface ManualReviewItem {
  id: string;
  candidateId: string;
  candidateName: string;
  reviewType: string;
  status: "pending" | "resolved" | "rejected";
  summary: string;
  recommendedAction: string;
  evidenceLabels: string[];
}

export interface SearchHistoryItem {
  id: string;
  createdAt: string;
  queryText: string;
  summary: string;
  phase: WorkflowPhase;
  errorMessage: string;
  plan: DemoPlan | null;
  reviewId: string;
  jobId: string;
  revisionText: string;
  requiresReview: boolean;
  timelineSteps: SearchTimelineStep[];
  selectedCandidateId: string;
}
