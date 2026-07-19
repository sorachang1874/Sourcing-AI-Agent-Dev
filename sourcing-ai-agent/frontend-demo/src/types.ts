export type PlanStatus = "draft" | "pending_review" | "approved";

export interface CohortSelectionOption {
  id: string;
  label: string;
  order: number;
}

export interface CohortSelectionOptions {
  schemaVersion: string;
  registryVersion: string;
  registryDigest: string;
  roleBuckets: CohortSelectionOption[];
  employmentStatuses: CohortSelectionOption[];
  roleMatchOptions: CohortSelectionOption[];
  defaultRoleMatch: string;
}

/** Exact public API wire object; keys intentionally remain snake_case. */
export interface CohortSelection {
  schema_version: string;
  role_bucket_ids: string[];
  employment_statuses: string[];
  role_match: string;
  source: "user_explicit";
}

/**
 * Location targeting selection. The wire fields `target_locations` /
 * `exclude_target_locations` ride ALONGSIDE the closed cohort_selection.v1
 * object at request top level; they are never part of the cohort object.
 * Free-text provider location names; no client-side enum (backend-validated).
 *
 * Presence-aware tri-state mirroring the backend contract
 * (`domain._normalize_location_list`, FT0 §7.2):
 * - `undefined` (absent): the server default applies (`["United States"]` for
 *   explicit-Cohort target locations; no exclusions otherwise).
 * - `[]` (explicit empty): opt-out — the request suppresses location
 *   filtering and every location default.
 * - present values: the user's explicit ordered list.
 * A present `null` is invalid on the wire and rejected by the parser.
 */
export interface CohortLocationSelection {
  targetLocations?: string[];
  excludeTargetLocations?: string[];
}

export type CandidateConfidence = "high" | "medium" | "lead_only";
export type WorkflowPhase = "idle" | "plan" | "running" | "results";
export type TimelineStepStatus = "completed" | "running" | "pending" | "failed" | "cancelled";
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
  | "run_former_search_seed"
  | "target_locations"
  | "exclude_target_locations";

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
  cohortSelection?: CohortSelection;
  targetLocations?: string[];
  excludeTargetLocations?: string[];
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

export interface ProviderExecutionLanePreview {
  laneId: string;
  employmentStatus: string;
  provider: string;
  operation: string;
  queryTexts: string[];
  companyFilters: Record<string, string[]>;
  providerFacingQuery: boolean;
  displayLabel: string;
  reason: string;
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
  providerExecutionLanes?: ProviderExecutionLanePreview[];
  reviewGate?: PlanReviewGate;
  reviewDecisionDefaults?: PlanReviewDecision;
  cohortSelection?: CohortSelection;
  targetLocations?: string[];
  excludeTargetLocations?: string[];
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

export interface CandidateSourceMatch {
  field?: string;
  matched_on?: string;
  source_type?: string;
  source_query?: string;
  matched_keywords?: string[];
  [key: string]: unknown;
}

export interface Candidate {
  id: string;
  candidateIdentityKey?: string;
  personIdentityKey?: string;
  profileUrlKey?: string;
  name: string;
  headline: string;
  avatarUrl: string;
  team: string;
  employmentStatus: "current" | "former" | "lead";
  confidence: CandidateConfidence;
  summary: string;
  rank?: number;
  score?: number;
  outreachLayer: number | null;
  outreachLayerKey?: string;
  matchedKeywords: string[];
  sourceMatches?: CandidateSourceMatch[];
  currentCompany?: string;
  location?: string;
  roleBucket?: string;
  functionIds?: string[];
  /** Server-computed multi-valued function facet ids (FT0 §5.2); never re-derived locally. */
  functionBucketIds?: string[];
  /** Provenance of `functionBucketIds`, pinned by FT0 §5.2. */
  functionBucketSource?: "lane_membership" | "registry_evidence" | "legacy_inference";
  /**
   * Server-owned employment membership truth (FT0 §6): the verbatim
   * `metadata.cohort_employment_statuses` set for Cohort-produced candidates.
   * A dual-status candidate carries BOTH values here while the top-level
   * `employmentStatus` stays a display-only convenience projection.
   */
  cohortEmploymentStatuses?: ("current" | "former")[];
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

export interface CandidateFacetSummary {
  schemaVersion?: number;
  candidateCount?: number;
  layers?: FunnelLayer[];
  recall?: FunnelLayer[];
  employment?: FunnelLayer[];
  locations?: FunnelLayer[];
  functions?: FunnelLayer[];
}

export interface BoardRuntimeSyncNoteLine {
  id: string;
  text: string;
}

export interface DashboardData {
  projectionId?: string;
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
  profileFetchProgress?: ProfileFetchProgress;
  linkedinStage1Progress?: LinkedinStage1Progress;
  resultViewLifecycle?: ResultViewLifecycle;
  boardRuntimeState?: BoardRuntimeState;
  executionPhaseContract?: ExecutionPhaseContract;
  effectiveExecutionSemantics?: EffectiveExecutionSemantics;
  candidateFacetSummary?: CandidateFacetSummary;
  candidateFacetSummaryScope?: string;
}

export interface EffectiveExecutionSemantics {
  effectiveAcquisitionMode: string;
  defaultResultsMode: string;
  executionStrategyLabel: string;
  fullLocalAssetReuse: boolean;
  requiresDeltaAcquisition: boolean;
  assetPopulationSupported: boolean;
}

export interface ProfileFetchProgress {
  totalUrlCount: number;
  fetchedUrlCount: number;
  queuedUrlCount: number;
  failedRetryableUrlCount: number;
  unrecoverableUrlCount: number;
  missingRegistryUrlCount: number;
  deferredUrlCount: number;
  pendingUrlCount: number;
  statusCounts: Record<string, number>;
}

export interface LinkedinStage1Progress {
  currentSearchReturnedCount: number;
  formerSearchReturnedCount: number;
  allSearchReturnedCount: number;
  dedupedCandidateCount: number;
  dedupedProfileUrlCount: number;
  profileFetchRequiredCount: number;
  profileFetchedCount: number;
  profileQueuedCount: number;
  profileFailedRetryableCount: number;
  profileUnrecoverableCount: number;
  profilePendingCount: number;
  statusCounts: Record<string, number>;
}

export interface ResultViewLifecycle {
  state: string;
  baselineSnapshotId: string;
  currentSnapshotId: string;
  servedSnapshotId: string;
  baselineCandidateCount: number;
  servedCandidateCount: number;
  expectedCandidateCount: number;
  deltaProfileProgressApplicable: boolean;
  deltaProfileProgressReason: string;
  deltaProfileRequiredCount: number;
  deltaProfileFetchedCount: number;
  deltaProfileMaterializedCount?: number;
  deltaProfileBoardVisibleCount?: number;
  deltaProfilePendingCount: number;
  deltaProfileQueuedCount: number;
  deltaProfileRetryableCount: number;
  servingProjectionId?: string;
  servingProjectionPhase?: string;
  backgroundSnapshotMaterializationStatus: string;
  outreachLayeringStatus: string;
}

export interface BoardRuntimeState {
  schemaVersion: number;
  jobId: string;
  resultMode: "ranked_results" | "asset_population";
  phase: string;
  publicationStatus: "unavailable" | "pending" | "partial" | "complete" | string;
  expectedCandidateCount: number;
  servedCandidateCount: number;
  publishedCandidateCount: number;
  displayReadyCandidateCount: number;
  previewCandidateCount: number;
  profileDetailCandidateCount: number;
  explicitProfileCaptureCandidateCount: number | null;
  needsProfileCompletionCandidateCount: number | null;
  lowProfileRichnessCandidateCount: number | null;
  cardMaterializationQualityFieldsAvailable: boolean;
  rowHydrationTargetCount: number;
  candidateDiscoveryCount?: number;
  profileFetchRequiredCount?: number;
  profileFetchedCount?: number;
  baselineCandidateCount: number;
  deltaProfileRequiredCount: number;
  deltaProfileFetchedCount: number;
  deltaProfileMaterializedCount: number;
  deltaProfileBoardVisibleCount: number;
  deltaProfileDenominatorPromoted?: boolean;
  rowPublicationSequence: number;
  rowPublicationTier?: string;
  rowPublicationRevision?: string;
  rowPublicationWatermark: string;
  rowPublicationUpdatedAt: string;
  facetSummaryStatus: string;
  facetSummaryScope: string;
  facetSummaryCandidateCount: number;
  layeringStatus: string;
  filterContract?: {
    source: string;
    facetCountScope: string;
    rowFilterScope: string;
    backendFilteredPagingSupported: boolean;
  };
  syncStatusText: string;
  syncNoteLines?: BoardRuntimeSyncNoteLine[];
  candidateDiscoveryStatusText?: string;
  profileFetchStatusText: string;
  cardMaterializationStatusText: string;
  noteText: string;
}

export interface ExecutionPhaseContract {
  activePhaseId: string;
  activeStageId: string;
  activePhaseLabel: string;
  activePhaseDetail: string;
  publicWebStageApplicable: boolean;
  localAssetMaterializationApplicable: boolean;
  profileWorkPending: boolean;
  stageTitleOverrides: Record<string, string>;
  stageDetailOverrides: Record<string, string>;
}

export interface ExcelIntakeProgress {
  workflowKind: string;
  targetCompany: string;
  inputFilename: string;
  totalRowCount: number;
  matchedRowCount: number;
  targetCandidateCount: number;
  manualReviewRowCount: number;
  unresolvedRowCount: number;
  invalidRowCount: number;
  reviewRowCount: number;
  statusCounts: Record<string, number>;
  rowManifestAvailable: boolean;
  rowManifestTruncated: boolean;
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

export type WorkflowRunStatus = "queued" | "running" | "completed" | "blocked" | "failed" | "cancelled";

export interface RunStatusData {
  jobId: string;
  status: WorkflowRunStatus;
  currentStage: string;
  startedAt: string;
  currentMessage?: string;
  awaitingUserAction?: string;
  metrics: RunMetric[];
  linkedinStage1Progress?: LinkedinStage1Progress;
  resultViewLifecycle?: ResultViewLifecycle;
  boardRuntimeState?: BoardRuntimeState;
  executionPhaseContract?: ExecutionPhaseContract;
  excelIntakeProgress?: ExcelIntakeProgress;
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
  workspaceId: string;
  candidateId: string;
  candidateIdentityKey?: string;
  personIdentityKey?: string;
  sourceProjectionId?: string;
  sourceMembershipRevision?: string;
  sourceRunId?: string;
  sourceCollectionId?: string;
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
  | "documents_fetched"
  | "analyzing"
  | "adjudication_completed"
  | "analysis_completed"
  | "completed"
  | "completed_with_errors"
  | "needs_review"
  | "failed"
  | "cancelled"
  | "unknown";

export interface TargetCandidatePublicWebBatch {
  batchId: string;
  workspaceId: string;
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
  workspaceId: string;
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
  phaseCommands: Record<string, unknown>;
  phaseCommandDisplayLine: string;
  runControlState: Record<string, unknown>;
  runDisplayContract: Record<string, unknown>;
  artifactRoot: string;
  lastError: string;
  createdAt: string;
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
  promotionOverrideReason?: string;
  promotionOverrideValidationReason?: string;
  promotionRequiresManualOverride?: boolean;
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
  value: string;
  normalizedValue: string;
  url: string;
  newValue: string;
  previousValue: string;
  sourceUrl: string;
  sourceDomain: string;
  sourceFamily: string;
  sourceTitle: string;
  confidenceLabel: string;
  confidenceScore: number | null;
  identityMatchLabel: string;
  identityMatchScore: number | null;
  publishable: boolean;
  cleanProfileLink: boolean;
  linkShapeWarnings: string[];
  action: string;
  promotionStatus: string;
  operator: string;
  note: string;
  overrideReason: string;
  overrideValidationReason: string;
  requiresManualOverride: boolean;
  evidenceExcerpt: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface TargetCandidatePublicWebDetail {
  status: string;
  recordId: string;
  targetCandidate: Record<string, unknown> | null;
  latestRun: Record<string, unknown> | null;
  phaseCommands: Record<string, unknown>;
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

export interface TargetCandidateComposedProfile {
  schemaVersion: number;
  identity: Record<string, unknown>;
  contact: {
    primary_email?: string;
    selected_email?: string;
    selected_email_source?: string;
    method_count?: number;
    methods?: Record<string, unknown>[];
  };
  public_web: {
    status?: string;
    latest_run_id?: string;
    email_candidate_count?: number;
    profile_link_count?: number;
    clean_profile_link_count?: number;
    evidence_link_count?: number;
    promotion_count?: number;
    promoted_signal_count?: number;
  };
  review: {
    follow_up_status?: string;
    quality_score?: number | null;
    comment?: string;
    flags?: string[];
    needs_review?: boolean;
  };
  export_readiness: {
    ready?: boolean;
    exportable_signal_count?: number;
    promoted_exportable_signal_count?: number;
    ai_publishable_unconfirmed_signal_count?: number;
    reason?: string;
    default_public_web_export_mode?: string;
    expanded_public_web_export_mode?: string;
  };
  completeness: {
    score?: number;
    reasons?: string[];
  };
  evidence_sources: TargetCandidatePublicWebEvidenceLink[];
  raw_asset_policy: Record<string, unknown>;
}

export interface TargetCandidateProfileDetail {
  status: string;
  recordId: string;
  targetCandidate: Record<string, unknown> | null;
  profile: TargetCandidateComposedProfile | null;
  publicWebDetail: TargetCandidatePublicWebDetail | null;
  rawAssetPolicy: Record<string, unknown>;
}

export interface TargetCandidatePublicWebSearchState {
  status: string;
  batches: TargetCandidatePublicWebBatch[];
  runs: TargetCandidatePublicWebRun[];
  phaseCommandsByRunId: Record<string, unknown>;
}

export interface TargetCandidatePublicWebStartResult extends TargetCandidatePublicWebSearchState {
  batch: TargetCandidatePublicWebBatch | null;
  summary: Record<string, unknown>;
  workerSummary: Record<string, unknown>;
  job: Record<string, unknown>;
}

export interface TargetCandidatePublicWebActionResult extends TargetCandidatePublicWebSearchState {
  batch: TargetCandidatePublicWebBatch | null;
  summary: Record<string, unknown>;
  workerSummary: Record<string, unknown>;
  job: Record<string, unknown>;
  reason: string;
}
