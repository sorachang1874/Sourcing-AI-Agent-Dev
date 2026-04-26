export type PlanStatus = "draft" | "pending_review" | "approved";

export type CandidateConfidence = "high" | "medium" | "lead_only";
export type WorkflowPhase = "idle" | "plan" | "running" | "results";
export type TimelineStepStatus = "completed" | "running" | "pending";
export type CandidateReviewStatus =
  | "no_review_needed"
  | "needs_review"
  | "needs_profile_completion"
  | "low_profile_richness"
  | "verified_keep"
  | "verified_exclude";
export type TargetCandidateFollowUpStatus =
  | "pending_outreach"
  | "contacted_waiting"
  | "rejected"
  | "accepted"
  | "interview_completed";
export type PlanReviewEditableField =
  | "company_scope"
  | "target_company_linkedin_url"
  | "extra_source_families"
  | "precision_recall_bias"
  | "acquisition_strategy_override"
  | "use_company_employees_lane"
  | "keyword_priority_only"
  | "former_keyword_queries_only"
  | "provider_people_search_query_strategy"
  | "provider_people_search_max_queries"
  | "large_org_keyword_probe_mode"
  | "force_fresh_run"
  | "reuse_existing_roster"
  | "run_former_search_seed";

export interface PlanReviewGate {
  status: string;
  requiredBeforeExecution: boolean;
  riskLevel: string;
  reasons: string[];
  confirmationItems: string[];
  editableFields: PlanReviewEditableField[];
  suggestedActions: string[];
  scopeHints: string[];
  executionModeHints: string[];
}

export interface PlanReviewDecision {
  confirmedCompanyScope: string[];
  targetCompanyLinkedinUrl?: string;
  extraSourceFamilies: string[];
  precisionRecallBias?: string;
  acquisitionStrategyOverride?: string;
  useCompanyEmployeesLane?: boolean;
  keywordPriorityOnly?: boolean;
  formerKeywordQueriesOnly?: boolean;
  providerPeopleSearchQueryStrategy?: string;
  providerPeopleSearchMaxQueries?: number | null;
  largeOrgKeywordProbeMode?: boolean;
  forceFreshRun?: boolean;
  reuseExistingRoster?: boolean;
  runFormerSearchSeed?: boolean;
}

export interface TargetCompanyIdentityPreview {
  requestedName?: string;
  canonicalName?: string;
  companyKey?: string;
  linkedinSlug?: string;
  linkedinCompanyUrl?: string;
  domain?: string;
  resolver?: string;
  confidence?: string;
  localAssetAvailable?: boolean;
}

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
  organizationScaleBand?: string;
  defaultAcquisitionMode?: string;
  plannerMode?: string;
  dispatchStrategy?: string;
  currentLaneBehavior?: string;
  formerLaneBehavior?: string;
  baselineSnapshotId?: string;
  requiresDeltaAcquisition?: boolean;
  executionNotes?: string[];
  targetCompanyIdentity?: TargetCompanyIdentityPreview;
  reviewGate?: PlanReviewGate;
  reviewDecisionDefaults?: PlanReviewDecision;
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

export interface CandidateEmailMetadata {
  source?: string;
  status?: string;
  qualityScore?: number;
  foundInLinkedInProfile?: boolean;
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
  rank?: number;
  score?: number;
  outreachLayer: number;
  outreachLayerKey?: string;
  matchedKeywords: string[];
  currentCompany?: string;
  location?: string;
  roleBucket?: string;
  functionIds?: string[];
  linkedinUrl?: string;
  sourceDataset?: string;
  notesSnippet?: string;
  focusAreas: string[];
  matchReasons: string[];
  education: string[];
  experience: string[];
  evidence: CandidateEvidence[];
  externalLinks?: CandidateExternalLink[];
  primaryEmail?: string;
  primaryEmailMetadata?: CandidateEmailMetadata;
  needsProfileCompletion?: boolean;
  hasProfileDetail?: boolean;
  profileCaptureKind?: string;
  lowProfileRichness?: boolean;
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
  targetCompany: string;
  intentKeywords: string[];
  resultMode: "ranked_results" | "asset_population";
  resultModeLabel: string;
  rankedCandidateCount: number;
  assetPopulationCount: number;
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
  currentMessage?: string;
  awaitingUserAction?: string;
  metrics: RunMetric[];
  timeline: RunEvent[];
  workers: RunWorker[];
}

export interface ManualReviewItem {
  id: string;
  reviewItemId?: number;
  candidateId: string;
  candidateName: string;
  candidate?: Candidate | null;
  reviewType: string;
  status: "open" | "resolved" | "dismissed" | "escalated";
  summary: string;
  recommendedAction: string;
  evidenceLabels: string[];
  notes?: string;
  synthesisSummary?: string;
}

export interface SupplementOperationResult {
  status: string;
  reason?: string;
  target_company?: string;
  company_key?: string;
  snapshot_id?: string;
  summary_path?: string;
  artifact_result?: Record<string, unknown>;
  package_sync?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface ExcelIntakeReviewOption {
  match_score?: number;
  match_reason?: string;
  candidate?: Record<string, unknown>;
  candidate_id?: string;
  full_name?: string;
  profile_url?: string;
  headline?: string;
  current_company?: string;
  [key: string]: unknown;
}

export interface ExcelIntakeRowResult {
  row_key: string;
  status: string;
  name?: string;
  company?: string;
  title?: string;
  linkedin_url?: string;
  email?: string;
  reason?: string;
  match_reason?: string;
  selected_candidate_id?: string;
  selected_profile_url?: string;
  matched_candidate?: Record<string, unknown>;
  manual_review_candidates?: ExcelIntakeReviewOption[];
  search_result?: Record<string, unknown>;
  fetch_errors?: string[];
  [key: string]: unknown;
}

export interface ExcelIntakeAttachmentConfig {
  attach_to_snapshot: boolean;
  target_company?: string;
  snapshot_id?: string;
  build_artifacts?: boolean;
}

export interface ExcelIntakeResponse {
  status: string;
  intake_id: string;
  continuation_id?: string;
  decision_count?: number;
  workbook?: Record<string, unknown>;
  schema_inference?: Record<string, unknown>;
  summary?: Record<string, number>;
  attachment?: ExcelIntakeAttachmentConfig;
  attachment_summary?: Record<string, unknown>;
  results: ExcelIntakeRowResult[];
  artifact_paths?: Record<string, string>;
  artifact_path?: string;
}

export interface ExcelIntakeWorkflowLaunchResponse {
  status: string;
  jobId: string;
  historyId: string;
  queryText: string;
  workflowKind?: string;
  raw?: unknown;
}

export interface SearchHistoryItem {
  id: string;
  createdAt: string;
  updatedAt?: string;
  queryText: string;
  summary: string;
  phase: WorkflowPhase;
  errorMessage: string;
  plan: DemoPlan | null;
  reviewId: string;
  jobId: string;
  revisionText: string;
  reviewDecision: PlanReviewDecision;
  reviewChecklistConfirmed: boolean;
  requiresReview: boolean;
  timelineSteps: SearchTimelineStep[];
  selectedCandidateId: string;
  historyMetadata?: Record<string, unknown>;
}

export interface CandidateReviewRecord {
  id: string;
  jobId: string;
  historyId?: string;
  candidateId: string;
  candidateName: string;
  headline?: string;
  currentCompany?: string;
  avatarUrl?: string;
  linkedinUrl?: string;
  primaryEmail?: string;
  primaryEmailMetadata?: CandidateEmailMetadata;
  status: CandidateReviewStatus;
  comment?: string;
  source: "manual_add" | "backend_override" | "manual_review";
  addedAt: string;
  updatedAt: string;
}

export interface TargetCandidateRecord {
  id: string;
  candidateId: string;
  historyId?: string;
  jobId?: string;
  candidateName: string;
  headline?: string;
  currentCompany?: string;
  avatarUrl?: string;
  linkedinUrl?: string;
  primaryEmail?: string;
  primaryEmailMetadata?: CandidateEmailMetadata;
  followUpStatus: TargetCandidateFollowUpStatus;
  qualityScore: number | null;
  comment: string;
  addedAt: string;
  updatedAt: string;
}

export type TargetCandidatePublicWebStatus =
  | "queued"
  | "search_submitted"
  | "searching"
  | "entry_links_ready"
  | "fetching"
  | "analyzing"
  | "completed"
  | "completed_with_errors"
  | "needs_review"
  | "failed"
  | "cancelled"
  | "unknown";

export interface TargetCandidatePublicWebBatch {
  batchId: string;
  status: TargetCandidatePublicWebStatus;
  requestedRecordIds: string[];
  runIds: string[];
  sourceFamilies: string[];
  summary: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface TargetCandidatePublicWebRun {
  runId: string;
  batchId: string;
  recordId: string;
  candidateId: string;
  candidateName: string;
  currentCompany: string;
  linkedinUrl: string;
  status: TargetCandidatePublicWebStatus;
  phase: string;
  sourceFamilies: string[];
  summary: Record<string, unknown>;
  queryManifest: Record<string, unknown>[];
  searchCheckpoint: Record<string, unknown>;
  analysisCheckpoint: Record<string, unknown>;
  artifactRoot: string;
  lastError: string;
  startedAt: string;
  completedAt: string;
  updatedAt: string;
}

export interface TargetCandidatePublicWebSignal {
  signalId: string;
  runId: string;
  signalKind: string;
  signalType: string;
  emailType: string;
  value: string;
  normalizedValue: string;
  url: string;
  sourceUrl: string;
  sourceDomain: string;
  sourceFamily: string;
  sourceTitle: string;
  confidenceLabel: string;
  confidenceScore: number | null;
  identityMatchLabel: string;
  identityMatchScore: number | null;
  publishable: boolean;
  promotionStatus: string;
  promotionId?: string;
  promotionAction?: string;
  promotedField?: string;
  promotedValue?: string;
  previousValue?: string;
  promotedBy?: string;
  promotedAt?: string;
  promotionNote?: string;
  suppressionReason: string;
  evidenceExcerpt: string;
  linkShapeWarnings: string[];
  cleanProfileLink: boolean;
  artifactRefs: Record<string, unknown>;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface TargetCandidatePublicWebEvidenceLink {
  sourceUrl: string;
  sourceDomain: string;
  sourceFamily: string;
  sourceTitle: string;
  signalIds: string[];
  signalKinds: string[];
  signalTypes: string[];
  identityMatchLabels: string[];
  maxConfidenceScore: number | null;
}

export interface TargetCandidatePublicWebPromotion {
  promotionId: string;
  signalId: string;
  runId: string;
  recordId: string;
  signalKind: string;
  signalType: string;
  emailType: string;
  newValue: string;
  previousValue: string;
  sourceUrl: string;
  sourceDomain: string;
  confidenceLabel: string;
  identityMatchLabel: string;
  action: string;
  promotionStatus: string;
  operator: string;
  note: string;
  createdAt: string;
  updatedAt: string;
}

export interface TargetCandidatePublicWebDetail {
  status: string;
  recordId: string;
  targetCandidate: Record<string, unknown> | null;
  latestRun: Record<string, unknown> | null;
  personAsset: Record<string, unknown> | null;
  signals: TargetCandidatePublicWebSignal[];
  emailCandidates: TargetCandidatePublicWebSignal[];
  profileLinks: TargetCandidatePublicWebSignal[];
  groupedSignals: Record<string, unknown>;
  evidenceLinks: TargetCandidatePublicWebEvidenceLink[];
  promotions: TargetCandidatePublicWebPromotion[];
  promotionSummary: Record<string, unknown>;
  rawAssetPolicy: Record<string, unknown>;
}

export interface TargetCandidatePublicWebSearchState {
  status: string;
  batches: TargetCandidatePublicWebBatch[];
  runs: TargetCandidatePublicWebRun[];
}

export interface TargetCandidatePublicWebStartResult extends TargetCandidatePublicWebSearchState {
  batch: TargetCandidatePublicWebBatch | null;
  summary: Record<string, unknown>;
  workerSummary: Record<string, unknown>;
  job: Record<string, unknown>;
}
