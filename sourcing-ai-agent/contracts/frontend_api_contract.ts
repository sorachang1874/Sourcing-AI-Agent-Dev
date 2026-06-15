export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonObject
  | JsonValue[];

export interface JsonObject {
  [key: string]: JsonValue;
}

export interface IntentRewriteRule {
  rewrite_id?: string;
  summary_label?: string;
  policy_layer?: string;
  keywords?: string[];
  must_have_facets?: string[];
  must_have_primary_role_buckets?: string[];
  must_have_keywords?: string[];
  targeting_terms?: string[];
  matched_terms?: string[];
  request_patch?: JsonObject;
  trigger_sources?: JsonObject;
  additional_rewrites?: IntentRewriteRule[];
  notes?: string;
  [key: string]: JsonValue | undefined;
}

export interface IntentRewritePolicyCatalogEntry {
  rewrite_id?: string;
  summary_label?: string;
  policy_layer?: string;
  trigger_sources?: JsonObject;
  request_patch?: JsonObject;
  targeting_terms?: string[];
  notes?: string;
  [key: string]: JsonValue | undefined;
}

export interface IntentRewriteEntry {
  matched: boolean;
  summary: string;
  rewrite: IntentRewriteRule | Record<string, never>;
  [key: string]: JsonValue | undefined;
}

export interface IntentRewritePayload {
  request: IntentRewriteEntry;
  instruction?: IntentRewriteEntry;
  policy_catalog?: IntentRewritePolicyCatalogEntry[];
  [key: string]: JsonValue | undefined;
}

export interface IntentBrief {
  identified_request: string[];
  target_output: string[];
  default_execution_strategy: string[];
  review_focus: string[];
  [key: string]: JsonValue | undefined;
}

export interface PlanReviewGate {
  status?: string;
  required_before_execution?: boolean;
  risk_level?: string;
  reasons?: string[];
  confirmation_items?: string[];
  editable_fields?: string[];
  suggested_actions?: string[];
  execution_mode_hints?: JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface PlanReviewSessionSummary {
  review_id?: number;
  target_company?: string;
  status?: string;
  risk_level?: string;
  required_before_execution?: boolean;
  created_at?: string;
  updated_at?: string;
  raw_user_request?: string;
  [key: string]: JsonValue | undefined;
}

export interface SourcingPlanSummary {
  target_company?: string;
  intent_summary?: string;
  criteria_summary?: string;
  intent_brief?: IntentBrief;
  retrieval_plan?: JsonObject;
  acquisition_strategy?: JsonObject;
  acquisition_tasks?: JsonObject[];
  search_strategy?: JsonObject;
  open_questions?: string[];
  assumptions?: string[];
  [key: string]: JsonValue | undefined;
}

export interface PlanResponse {
  request: JsonObject;
  request_preview?: JsonObject;
  plan: SourcingPlanSummary;
  plan_review_gate: PlanReviewGate;
  plan_review_session: JsonObject;
  intent_rewrite: IntentRewritePayload;
  criteria_version_id?: number;
  criteria_compiler_run_id?: number;
  criteria_request_signature?: string;
  [key: string]: JsonValue | undefined;
}

export interface ReviewPayload {
  review_id?: number;
  action?: string;
  reviewer?: string;
  notes?: string;
  decision?: JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface InstructionCompiler {
  source?: string;
  provider?: string;
  allowed_fields?: string[];
  model_decision?: JsonObject;
  deterministic_decision?: JsonObject;
  model_patch?: JsonObject;
  deterministic_patch?: JsonObject;
  supplemented_keys?: string[];
  policy_inferred_keys?: string[];
  fallback_used?: boolean;
  request_intent_rewrite?: IntentRewriteRule | JsonObject;
  instruction_intent_rewrite?: IntentRewriteRule | JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface ReviewInstructionCompileResponse {
  status: "compiled" | "invalid" | "not_found";
  review_id?: number;
  reason?: string;
  review_payload?: ReviewPayload;
  instruction_compiler?: InstructionCompiler;
  intent_rewrite?: IntentRewritePayload;
  [key: string]: JsonValue | undefined;
}

export interface ReviewPlanApplyResponse {
  status: string;
  review?: JsonObject;
  instruction_compiler?: InstructionCompiler;
  intent_rewrite?: IntentRewritePayload;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowStartResponse {
  job_id?: string;
  status: string;
  stage?: string;
  plan?: SourcingPlanSummary;
  plan_review_session?: JsonObject;
  plan_review_gate?: PlanReviewGate;
  reason?: string;
  intent_rewrite?: IntentRewritePayload;
  dispatch?: QueryDispatchRecord;
  criteria_version_id?: number;
  criteria_compiler_run_id?: number;
  criteria_request_signature?: string;
  [key: string]: JsonValue | undefined;
}

export interface QueryDispatchRecord {
  dispatch_id?: number;
  target_company?: string;
  request_signature?: string;
  request_family_signature?: string;
  requester_id?: string;
  tenant_id?: string;
  idempotency_key?: string;
  strategy?: string;
  status?: string;
  source_job_id?: string;
  created_job_id?: string;
  payload?: JsonObject;
  created_at?: string;
  updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface QueryDispatchListResponse {
  query_dispatches: QueryDispatchRecord[];
  [key: string]: JsonValue | undefined;
}

export interface JobMilestone {
  stage: string;
  status: string;
  started_at?: string;
  completed_at?: string;
  latest_detail?: string;
  event_count?: number;
  elapsed_seconds?: number;
  [key: string]: JsonValue | undefined;
}

export interface WorkerLaneSummary {
  lane_id: string;
  worker_count: number;
  by_status: Record<string, number>;
  raw_by_status: Record<string, number>;
  last_updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkerSummary {
  by_status: Record<string, number>;
  raw_by_status: Record<string, number>;
  by_lane: WorkerLaneSummary[];
  [key: string]: JsonValue | undefined;
}

export interface RefreshMetricsSummary {
  pre_retrieval_refresh_count?: number;
  pre_retrieval_refresh_status?: string;
  inline_search_seed_worker_count?: number;
  inline_harvest_prefetch_worker_count?: number;
  pre_retrieval_refresh_snapshot_id?: string;
  background_reconcile_count?: number;
  background_search_seed_reconcile_count?: number;
  background_search_seed_reconcile_status?: string;
  background_search_seed_worker_count?: number;
  background_search_seed_added_entry_count?: number;
  background_harvest_prefetch_reconcile_count?: number;
  background_harvest_prefetch_reconcile_status?: string;
  background_harvest_prefetch_worker_count?: number;
  background_exploration_reconcile_count?: number;
  background_exploration_reconcile_status?: string;
  background_exploration_worker_count?: number;
  [key: string]: JsonValue | undefined;
}

export interface RuntimeRefreshMetricsSummary {
  pre_retrieval_refresh_job_count?: number;
  inline_search_seed_worker_count?: number;
  inline_harvest_prefetch_worker_count?: number;
  background_reconcile_job_count?: number;
  background_search_seed_reconcile_job_count?: number;
  background_harvest_prefetch_reconcile_job_count?: number;
  [key: string]: JsonValue | undefined;
}

export interface ServiceStatusPayload {
  status?: string;
  lock_status?: string;
  pid?: number;
  started_at?: string;
  updated_at?: string;
  heartbeat_at?: string;
  detail?: string;
  [key: string]: JsonValue | undefined;
}

export interface RecoveryControlSummary {
  status?: string;
  service_name?: string;
  service_ready?: boolean;
  service_status?: ServiceStatusPayload;
  [key: string]: JsonValue | undefined;
}

export interface JobRuntimeHealth {
  state?: string;
  classification?: string;
  detail?: string;
  blocked_task?: string;
  pending_worker_count?: number;
  active_worker_count?: number;
  [key: string]: JsonValue | undefined;
}

export interface RuntimeJobRecoveryItem {
  job_id?: string;
  status?: string;
  stage?: string;
  job_recovery?: RecoveryControlSummary;
  [key: string]: JsonValue | undefined;
}

export interface RuntimeStaleJobItem {
  job_id?: string;
  status?: string;
  stage?: string;
  updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface RuntimeStaleJobsSummary {
  acquiring?: RuntimeStaleJobItem[];
  queued?: RuntimeStaleJobItem[];
  [key: string]: JsonValue | undefined;
}

export interface RuntimeRecoverableWorkerItem {
  worker_id?: number;
  job_id?: string;
  lane_id?: string;
  status?: string;
  [key: string]: JsonValue | undefined;
}

export interface RuntimeRecoverableWorkersSummary {
  count?: number;
  sample?: RuntimeRecoverableWorkerItem[];
  [key: string]: JsonValue | undefined;
}

export interface RuntimeServicesSummary {
  shared_recovery?: ServiceStatusPayload;
  tracked_job_recovery_count?: number;
  [key: string]: JsonValue | undefined;
}

export interface RuntimeMetricsResponse {
  status: string;
  observed_at?: string;
  metrics: JsonObject;
  refresh_metrics?: RuntimeRefreshMetricsSummary;
  event_level_efficiency?: JsonObject;
  services?: RuntimeServicesSummary;
  [key: string]: JsonValue | undefined;
}

export interface RuntimeHealthServicesSummary {
  shared_recovery?: ServiceStatusPayload;
  job_recoveries?: RuntimeJobRecoveryItem[];
  [key: string]: JsonValue | undefined;
}

export interface RuntimeHealthResponse {
  status: string;
  observed_at?: string;
  providers?: JsonObject;
  services?: RuntimeHealthServicesSummary;
  metrics?: JsonObject;
  stalled_jobs?: Array<{
    job_id?: string;
    status?: string;
    stage?: string;
    updated_at?: string;
    runtime_health?: JobRuntimeHealth;
    [key: string]: JsonValue | undefined;
  }>;
  stale_jobs?: RuntimeStaleJobsSummary;
  recoverable_workers?: RuntimeRecoverableWorkersSummary;
  [key: string]: JsonValue | undefined;
}

export interface ProgressMetrics {
  refresh_metrics?: RefreshMetricsSummary;
  pre_retrieval_refresh?: JsonObject;
  background_reconcile?: JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface ProgressPayload {
  stage_order: string[];
  current_stage: string;
  completed_stages: string[];
  milestones: JobMilestone[];
  timing: JsonObject;
  latest_event?: JsonObject;
  worker_summary: WorkerSummary;
  latest_metrics?: ProgressMetrics;
  linkedin_stage_1_progress?: LinkedinStage1Progress;
  board_runtime_state?: BoardRuntimeState;
  result_view_lifecycle?: ResultViewLifecycle;
  execution_phase_contract?: ExecutionPhaseContract;
  counters: Record<string, number>;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowStageSummaryItem {
  stage?: string;
  status?: string;
  summary_path?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowStageSummariesPayload {
  directory?: string;
  stage_order: string[];
  summaries: Record<string, WorkflowStageSummaryItem>;
  [key: string]: JsonValue | undefined;
}

export interface JobProgressResponse {
  job_id: string;
  status: string;
  stage: string;
  started_at?: string;
  updated_at?: string;
  elapsed_seconds?: number;
  blocked_task?: string;
  current_message?: string;
  progress: ProgressPayload;
  linkedin_stage_1_progress?: LinkedinStage1Progress;
  board_runtime_state?: BoardRuntimeState;
  result_view_lifecycle?: ResultViewLifecycle;
  execution_phase_contract?: ExecutionPhaseContract;
  workflow_stage_summaries?: WorkflowStageSummariesPayload;
  [key: string]: JsonValue | undefined;
}

export interface SystemProgressWorkflowItem {
  job_id: string;
  target_company?: string;
  status?: string;
  stage?: string;
  updated_at?: string;
  runtime_health?: JobRuntimeHealth;
  counters?: Record<string, number>;
  latest_metrics?: ProgressMetrics;
  refresh_metrics?: RefreshMetricsSummary;
  pre_retrieval_refresh?: JsonObject;
  background_reconcile?: JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface CloudAssetOperationItem {
  ledger_id?: number;
  operation_type?: string;
  bundle_kind?: string;
  bundle_id?: string;
  sync_run_id?: string;
  status?: string;
  manifest_path?: string;
  target_runtime_dir?: string;
  target_db_path?: string;
  scoped_companies?: string[];
  scoped_snapshot_id?: string;
  summary?: JsonObject;
  metadata?: JsonObject;
  created_at?: string;
  updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface CompanyAssetProgress {
  target_company?: string;
  asset_view?: string;
  authoritative_registry?: JsonObject;
  execution_profile?: JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface ObjectSyncTransferProgressItem {
  bundle_id?: string;
  bundle_kind?: string;
  direction?: string;
  status?: string;
  updated_at?: string;
  completion_ratio?: number;
  requested_file_count?: number;
  completed_file_count?: number;
  remaining_file_count?: number;
  transfer_mode?: string;
  bundle_dir?: string;
  progress_path?: string;
  archive?: JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface SystemProgressResponse {
  status: string;
  observed_at?: string;
  runtime: RuntimeMetricsResponse;
  workflow_jobs: {
    count: number;
    items: SystemProgressWorkflowItem[];
    [key: string]: JsonValue | undefined;
  };
  profile_registry: JsonObject;
  object_sync: {
    tracked_bundle_count?: number;
    bundle_index_updated_at?: string;
    active_transfer_count?: number;
    status_counts?: Record<string, number>;
    recent_transfers?: ObjectSyncTransferProgressItem[];
    [key: string]: JsonValue | undefined;
  };
  cloud_asset_operations?: {
    count?: number;
    items?: CloudAssetOperationItem[];
    [key: string]: JsonValue | undefined;
  };
  company_asset?: CompanyAssetProgress;
  [key: string]: JsonValue | undefined;
}

export interface MatchResult {
  candidate_id?: string;
  display_name?: string;
  name_en?: string;
  name_zh?: string;
  category?: string;
  target_company?: string;
  organization?: string;
  employment_status?: string;
  role_bucket?: string;
  functional_facets?: string[];
  role?: string;
  team?: string;
  focus_areas?: string;
  education?: string;
  work_history?: string;
  notes?: string;
  linkedin_url?: string;
  score?: number;
  semantic_score?: number;
  confidence_label?: string;
  confidence_score?: number;
  confidence_reason?: string;
  rank?: number;
  matched_keywords?: string[];
  matched_fields?: JsonObject[];
  source_matches?: JsonObject[];
  explanation?: string;
  evidence?: JsonObject[];
  [key: string]: JsonValue | undefined;
}

export interface ProfileFetchProgress {
  total_url_count?: number;
  fetched_url_count?: number;
  queued_url_count?: number;
  failed_retryable_url_count?: number;
  unrecoverable_url_count?: number;
  missing_registry_url_count?: number;
  deferred_url_count?: number;
  pending_url_count?: number;
  status_counts?: Record<string, number>;
  [key: string]: JsonValue | undefined;
}

export interface LinkedinStage1Progress {
  current_search_returned_count?: number;
  former_search_returned_count?: number;
  all_search_returned_count?: number;
  deduped_candidate_count?: number;
  deduped_profile_url_count?: number;
  profile_fetch_required_count?: number;
  profile_fetched_count?: number;
  profile_queued_count?: number;
  profile_failed_retryable_count?: number;
  profile_unrecoverable_count?: number;
  profile_pending_count?: number;
  status_counts?: Record<string, number>;
  [key: string]: JsonValue | undefined;
}

export interface BoardRuntimeState {
  schema_version?: number;
  job_id?: string;
  result_mode?: string;
  phase?: string;
  publication_status?: string;
  expected_candidate_count?: number;
  served_candidate_count?: number;
  published_candidate_count?: number;
  display_ready_candidate_count?: number;
  preview_candidate_count?: number;
  profile_detail_candidate_count?: number;
  explicit_profile_capture_candidate_count?: number;
  needs_profile_completion_candidate_count?: number;
  low_profile_richness_candidate_count?: number;
  card_materialization_quality_fields_available?: boolean;
  row_hydration_target_count?: number;
  candidate_discovery_count?: number;
  profile_fetch_required_count?: number;
  profile_fetched_count?: number;
  baseline_candidate_count?: number;
  delta_profile_required_count?: number;
  delta_profile_fetched_count?: number;
  delta_profile_materialized_count?: number;
  delta_profile_board_visible_count?: number;
  delta_profile_denominator_promoted?: boolean;
  row_publication_sequence?: number;
  row_publication_tier?: string;
  row_publication_watermark?: string;
  row_publication_updated_at?: string;
  facet_summary_status?: string;
  facet_summary_scope?: string;
  facet_summary_candidate_count?: number;
  layering_status?: string;
  filter_contract?: {
    source?: string;
    facet_count_scope?: string;
    row_filter_scope?: string;
    backend_filtered_paging_supported?: boolean;
  };
  sync_status_text?: string;
  sync_note_lines?: Array<{
    id?: string;
    text?: string;
  }>;
  candidate_discovery_status_text?: string;
  profile_fetch_status_text?: string;
  card_materialization_status_text?: string;
  profile_fetch_status_detail?: string;
  card_materialization_status_detail?: string;
  note_text?: string;
  [key: string]: JsonValue | undefined;
}

export interface ResultViewLifecycle {
  state?: string;
  baseline_snapshot_id?: string;
  current_snapshot_id?: string;
  served_snapshot_id?: string;
  baseline_candidate_count?: number;
  served_candidate_count?: number;
  expected_candidate_count?: number;
  delta_profile_required_count?: number;
  delta_profile_fetched_count?: number;
  delta_profile_materialized_count?: number;
  delta_profile_board_visible_count?: number;
  delta_profile_pending_count?: number;
  delta_profile_queued_count?: number;
  delta_profile_retryable_count?: number;
  serving_projection_id?: string;
  serving_projection_phase?: string;
  background_snapshot_materialization_status?: string;
  outreach_layering_status?: string;
  [key: string]: JsonValue | undefined;
}

export interface ExecutionPhaseContract {
  active_phase_id?: string;
  active_stage_id?: string;
  active_phase_label?: string;
  active_phase_detail?: string;
  public_web_stage_applicable?: boolean;
  local_asset_materialization_applicable?: boolean;
  profile_work_pending?: boolean;
  stage_title_overrides?: Record<string, string>;
  stage_detail_overrides?: Record<string, string>;
  [key: string]: JsonValue | undefined;
}

export interface JobResultsResponse {
  job: JsonObject;
  events: JsonObject[];
  results: JsonObject[];
  asset_population?: JsonObject & {
    profile_fetch_progress?: ProfileFetchProgress;
  };
  profile_fetch_progress?: ProfileFetchProgress;
  linkedin_stage_1_progress?: LinkedinStage1Progress;
  result_view_lifecycle?: ResultViewLifecycle;
  execution_phase_contract?: ExecutionPhaseContract;
  manual_review_items: JsonObject[];
  agent_runtime_session: JsonObject;
  agent_trace_spans: JsonObject[];
  agent_workers: JsonObject[];
  intent_rewrite: IntentRewritePayload;
  request_preview?: JsonObject;
  workflow_stage_summaries?: WorkflowStageSummariesPayload;
  board_runtime_state?: BoardRuntimeState;
  [key: string]: JsonValue | undefined;
}

export interface RetrievalJobResponse {
  job_id: string;
  status: string;
  request: JsonObject;
  request_preview?: JsonObject;
  plan: SourcingPlanSummary | JsonObject;
  intent_rewrite: IntentRewritePayload;
  summary: JsonObject;
  matches: MatchResult[];
  manual_review_items: JsonObject[];
  criteria_patterns_applied?: JsonObject[];
  semantic_hits?: JsonObject[];
  confidence_policy?: JsonObject;
  confidence_policy_control?: JsonObject;
  runtime_policy?: JsonObject;
  artifact_path?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowExplainResponse {
  status: string;
  reason?: string;
  request?: JsonObject;
  request_preview?: JsonObject;
  intent_rewrite?: IntentRewritePayload;
  plan_review_gate?: JsonObject;
  plan_review_session?: JsonObject;
  organization_execution_profile?: JsonObject;
  asset_reuse_plan?: JsonObject;
  ingress_normalization?: JsonObject;
  planning?: JsonObject;
  dispatch_matching_normalization?: JsonObject;
  dispatch_preview?: JsonObject;
  lane_preview?: JsonObject;
  generation_watermarks?: JsonObject;
  cloud_asset_operations?: {
    count?: number;
    items?: CloudAssetOperationItem[];
    [key: string]: JsonValue | undefined;
  };
  timings_ms?: JsonObject;
  [key: string]: JsonValue | undefined;
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
  batch_id?: string;
  workspace_id?: string;
  status: TargetCandidatePublicWebStatus;
  requested_record_ids: string[];
  run_ids: string[];
  source_families: string[];
  summary: JsonObject;
  created_at?: string;
  updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface TargetCandidatePublicWebRun {
  run_id?: string;
  workspace_id?: string;
  batch_id?: string;
  record_id?: string;
  candidate_id?: string;
  candidate_name?: string;
  current_company?: string;
  linkedin_url?: string;
  status: TargetCandidatePublicWebStatus;
  phase?: string;
  source_families: string[];
  summary: JsonObject;
  query_manifest: JsonObject[];
  search_checkpoint: JsonObject;
  analysis_checkpoint: JsonObject;
  phase_commands?: JsonObject;
  phase_command_display_line?: string;
  run_control_state?: JsonObject;
  run_display_contract?: JsonObject;
  artifact_root?: string;
  last_error?: string;
  created_at?: string;
  started_at?: string;
  completed_at?: string;
  updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface TargetCandidatePublicWebSearchState {
  status: string;
  batches: TargetCandidatePublicWebBatch[];
  runs: TargetCandidatePublicWebRun[];
  [key: string]: JsonValue | undefined;
}

export interface TargetCandidatePublicWebStartResponse extends TargetCandidatePublicWebSearchState {
  batch?: TargetCandidatePublicWebBatch | null;
  summary: JsonObject;
  worker_summary: JsonObject;
  job: JsonObject;
}

export interface TargetCandidatePublicWebSignal {
  signal_id?: string;
  run_id?: string;
  asset_id?: string;
  person_identity_key?: string;
  record_id?: string;
  candidate_id?: string;
  signal_kind: "email_candidate" | "profile_link" | string;
  signal_type?: string;
  email_type?: string;
  value?: string;
  normalized_value?: string;
  url?: string;
  source_url?: string;
  source_domain?: string;
  source_family?: string;
  source_title?: string;
  confidence_label?: string;
  confidence_score?: number;
  identity_match_label?: string;
  identity_match_score?: number;
  publishable?: boolean;
  promotion_status?: string;
  promotion_id?: string;
  promotion_action?: string;
  promoted_field?: string;
  promoted_value?: string;
  previous_value?: string;
  promoted_by?: string;
  promoted_at?: string;
  promotion_note?: string;
  promotion_override_reason?: string;
  promotion_override_validation_reason?: string;
  promotion_requires_manual_override?: boolean;
  suppression_reason?: string;
  evidence_excerpt?: string;
  artifact_refs?: JsonObject;
  model_provider?: string;
  model_version?: string;
  link_shape_warnings?: string[];
  clean_profile_link?: boolean;
  metadata?: JsonObject;
  created_at?: string;
  updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface TargetCandidatePublicWebPromotion {
  promotion_id?: string;
  signal_id?: string;
  run_id?: string;
  record_id?: string;
  signal_kind?: string;
  signal_type?: string;
  email_type?: string;
  new_value?: string;
  previous_value?: string;
  source_url?: string;
  source_domain?: string;
  confidence_label?: string;
  identity_match_label?: string;
  action?: string;
  promotion_status?: string;
  operator?: string;
  note?: string;
  override_reason?: string;
  override_validation_reason?: string;
  requires_manual_override?: boolean;
  created_at?: string;
  updated_at?: string;
  [key: string]: JsonValue | undefined;
}

export interface TargetCandidatePublicWebEvidenceLink {
  source_url: string;
  source_domain?: string;
  source_family?: string;
  source_title?: string;
  signal_ids: string[];
  signal_kinds: string[];
  signal_types: string[];
  identity_match_labels: string[];
  max_confidence_score?: number;
  [key: string]: JsonValue | undefined;
}

export interface TargetCandidatePublicWebDetailResponse {
  status: string;
  record_id?: string;
  target_candidate?: JsonObject | null;
  latest_run?: JsonObject | null;
  person_asset?: JsonObject | null;
  signals: TargetCandidatePublicWebSignal[];
  email_candidates: TargetCandidatePublicWebSignal[];
  profile_links: TargetCandidatePublicWebSignal[];
  grouped_signals: JsonObject;
  evidence_links: TargetCandidatePublicWebEvidenceLink[];
  promotions: TargetCandidatePublicWebPromotion[];
  promotion_summary: JsonObject;
  raw_asset_policy: JsonObject;
  [key: string]: JsonValue | undefined;
}

export interface TargetCandidatePublicWebPromotionResponse {
  status: string;
  record_id?: string;
  signal?: TargetCandidatePublicWebSignal;
  promotion?: TargetCandidatePublicWebPromotion;
  target_candidate?: JsonObject;
  detail?: TargetCandidatePublicWebDetailResponse | null;
  reason?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandControlPolicy {
  schema_version?: string;
  command_type?: string;
  owner?: string;
  generic_control_contract?: string;
  running_control_category?: string;
  running_control_categories?: string[];
  running_control_maturity?: string;
  running_control_gap_status?: string;
  running_control_surface?: string;
  generic_cancel_statuses?: string[];
  generic_retry_statuses?: string[];
  generic_resume_statuses?: string[];
  running_cancel_supported?: boolean;
  running_cancel_statuses?: string[];
  running_cancel_owner?: string;
  running_cancel_delegate?: string;
  running_cancel_prerequisites?: string[];
  running_cancel_blocked_reason?: string;
  running_cancel_upgrade_requirements?: string[];
  running_cancel_contract?: string;
  unsupported_running_cancel_reason?: string;
  module_state_mutated_on_running_cancel?: boolean;
  running_resume_supported?: boolean;
  running_resume_statuses?: string[];
  running_resume_owner?: string;
  running_resume_delegate?: string;
  running_resume_prerequisites?: string[];
  running_resume_blocked_reason?: string;
  running_resume_upgrade_requirements?: string[];
  running_resume_contract?: string;
  unsupported_running_resume_reason?: string;
  module_state_mutated_on_running_resume?: boolean;
  control_source_of_truth?: string;
  agent_callable_surface?: string;
  fallback_status?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandControlState {
  schema_version?: string;
  command_type?: string;
  owner?: string;
  command_status?: string;
  can_cancel?: boolean;
  can_retry?: boolean;
  can_resume?: boolean;
  cancel_mode?: string;
  retry_mode?: string;
  resume_mode?: string;
  allowed_actions?: string[];
  disabled_reasons?: JsonObject;
  running_cancel_supported?: boolean;
  running_cancel_delegate?: string;
  running_cancel_prerequisites?: string[];
  running_resume_supported?: boolean;
  running_resume_delegate?: string;
  running_resume_prerequisites?: string[];
  module_state_mutated_on_cancel?: boolean;
  module_state_mutated_on_resume?: boolean;
  control_source_of_truth?: string;
  policy_source_of_truth?: string;
  fallback_status?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandActivitySpinePolicy {
  schema_version?: string;
  command_type?: string;
  owner?: string;
  requirement?: string;
  must_write_activity_run?: boolean;
  must_write_activity_attempt?: boolean;
  must_write_entity_delta?: boolean;
  downstream_activity_required?: boolean;
  agent_callable?: boolean;
  activity_table?: string;
  attempt_table?: string;
  entity_delta_table?: string;
  source_of_truth?: string;
  agent_callable_surface?: string;
  fallback_status?: string;
  migration_status?: string;
  deletion_condition?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandDisplayContract {
  schema_version?: string;
  command_type?: string;
  owner?: string;
  display_label?: string;
  display_category?: string;
  description?: string;
  source_of_truth?: string;
  fallback_status?: string;
  [key: string]: JsonValue | undefined;
}

export interface OperationActionDisplayContract {
  schema_version?: string;
  action_type?: string;
  owner_module?: string;
  operation_type?: string;
  display_label?: string;
  display_category?: string;
  description?: string;
  source_of_truth?: string;
  fallback_status?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandContract {
  command_type: string;
  owner?: string;
  agent_exposure_status?: string;
  agent_exposure_gate?: string;
  stage_id?: string;
  readiness_effect?: string;
  display_contract?: WorkflowCommandDisplayContract;
  control_policy?: WorkflowCommandControlPolicy;
  activity_spine_policy?: WorkflowCommandActivitySpinePolicy;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandControlSummary {
  source_of_truth?: string;
  fallback_status?: string;
  command_count?: number;
  running_control_maturity_counts?: Record<string, number>;
  running_control_gap_status_counts?: Record<string, number>;
  running_control_category_counts?: Record<string, number>;
  has_fail_closed_running_controls?: boolean;
  has_owner_specific_running_controls?: boolean;
  default_workflow_command_type?: string;
  default_running_control_maturity?: string;
  default_running_control_gap_status?: string;
  agent_ui_guidance?: string;
  [key: string]: JsonValue | undefined;
}

export interface OperationActionRegistryEntry {
  owner_module?: string;
  operation_type?: string;
  approval_policy?: string;
  budget_required?: boolean;
  description?: string;
  display_contract?: OperationActionDisplayContract;
  allowed_workflow_command_types?: string[];
  default_workflow_command_type?: string;
  workflow_command_exposure_gate?: string;
  workflow_command_exposure_status?: string;
  allowed_workflow_command_contracts?: WorkflowCommandContract[];
  workflow_command_control_summary?: WorkflowCommandControlSummary;
  default_workflow_command_contract?: WorkflowCommandContract;
  [key: string]: JsonValue | undefined;
}

export interface OperationActionRegistryResponse {
  status: string;
  contract?: string;
  action_registry: Record<string, OperationActionRegistryEntry>;
  [key: string]: JsonValue | undefined;
}

export interface OperationEventRecord extends JsonObject {
  event_id?: string;
  workspace_id?: string;
  event_stream_id?: string;
  operation_run_id?: string;
  action_id?: string;
  event_family?: string;
  event_type?: string;
  sequence_number?: number;
  actor?: string;
  source?: string;
  payload?: JsonObject;
  occurred_at?: string;
  recorded_at?: string;
}

export interface OperationActionRecord extends JsonObject {
  action_id?: string;
  workspace_id?: string;
  conversation_id?: string;
  action_type?: string;
  owner_module?: string;
  operation_type?: string;
  display_contract?: OperationActionDisplayContract;
  target_ref?: JsonObject;
  input?: JsonObject;
  approval_status?: string;
  approval_policy?: string;
  budget?: JsonObject;
  status?: string;
  result_ref?: JsonObject;
  metadata?: JsonObject;
  created_at?: string;
  updated_at?: string;
}

export interface OperationRunRecord extends JsonObject {
  operation_run_id?: string;
  workspace_id?: string;
  action_id?: string;
  owner_module?: string;
  operation_type?: string;
  display_contract?: OperationActionDisplayContract;
  status?: string;
  progress?: JsonObject;
  workflow_ref?: JsonObject;
  cost_budget?: JsonObject;
  result_ref?: JsonObject;
  metadata?: JsonObject;
  control_state?: OperationRunControlState;
  status_summary?: OperationRunStatusSummary;
  started_at?: string;
  completed_at?: string;
  created_at?: string;
  updated_at?: string;
}

export interface OperationRunControlState extends JsonObject {
  operation_status?: string;
  action_status?: string;
  operation_phase?: string;
  can_dispatch?: boolean;
  can_cancel?: boolean;
  can_retry?: boolean;
  can_resume?: boolean;
  allowed_actions?: string[];
  disabled_reasons?: JsonObject;
  control_source_of_truth?: string;
  fallback_status?: string;
  module_state_mutated_on_control?: boolean;
  schema_version?: string;
}

export interface OperationRunStatusSummary extends JsonObject {
  source?: string;
  fallback_status?: string;
  fallback_used?: boolean;
  module_state_mutated?: boolean;
  operation_status?: string;
  operation_phase?: string;
  workflow_command_count?: number;
  operation_event_count?: number;
  command_status_counts?: JsonObject;
  latest_event_type?: string;
  latest_event?: OperationEventRecord;
  latest_workflow_command?: WorkflowCommandRecord;
}

export interface OperationActionListResponse {
  status?: string;
  contract?: string;
  actions: OperationActionRecord[];
  [key: string]: JsonValue | undefined;
}

export interface OperationActionDetailResponse {
  status: string;
  contract?: string;
  action?: OperationActionRecord;
  operation_run?: OperationRunRecord;
  events?: OperationEventRecord[];
  [key: string]: JsonValue | undefined;
}

export interface OperationRunListResponse {
  status?: string;
  contract?: string;
  operation_runs: OperationRunRecord[];
  [key: string]: JsonValue | undefined;
}

export interface OperationRunDetailResponse {
  status: string;
  contract?: string;
  operation_run?: OperationRunRecord;
  events?: OperationEventRecord[];
  [key: string]: JsonValue | undefined;
}

export interface OperationRunProvenanceResponse {
  status: string;
  contract?: string;
  action?: OperationActionRecord;
  operation_run?: OperationRunRecord;
  action_events?: OperationEventRecord[];
  operation_events?: OperationEventRecord[];
  event_timeline?: OperationEventRecord[];
  workflow_commands?: WorkflowCommandRecord[];
  module_state_mutated?: boolean;
  [key: string]: JsonValue | undefined;
}

export interface OperationRunControlResponse {
  status: string;
  reason?: string;
  contract?: string;
  action?: OperationActionRecord;
  operation_run?: OperationRunRecord;
  parent_operation_run?: OperationRunRecord;
  display_contract?: OperationActionDisplayContract;
  control_state?: OperationRunControlState;
  workflow_command?: WorkflowCommandRecord;
  events?: OperationEventRecord[];
  module_state_mutated?: boolean;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandRegistryResponse {
  status: string;
  contract?: string;
  command_registry: Record<string, WorkflowCommandContract>;
  module_state_mutated?: boolean;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandRecord extends JsonObject {
  command_id?: string;
  workflow_run_id?: string;
  operation_id?: string;
  command_type?: string;
  owner?: string;
  agent_exposure_status?: string;
  agent_exposure_gate?: string;
  status?: string;
  stage_id?: string;
  readiness_effect?: string;
  display_contract?: WorkflowCommandDisplayContract;
  control_policy?: WorkflowCommandControlPolicy;
  control_state?: WorkflowCommandControlState;
  activity_spine_policy?: WorkflowCommandActivitySpinePolicy;
  execution_summary?: WorkflowCommandExecutionSummary;
}

export interface WorkflowCommandExecutionSummary extends JsonObject {
  source?: string;
  fallback_status?: string;
  fallback_used?: boolean;
  module_state_mutated?: boolean;
  activity_count?: number;
  attempt_count?: number;
  entity_delta_count?: number;
  activity_status_counts?: JsonObject;
  attempt_status_counts?: JsonObject;
  entity_delta_status_counts?: JsonObject;
  entity_delta_kind_counts?: JsonObject;
  latest_effect_status?: string;
  latest_activity?: WorkflowActivityRecord;
  latest_attempt?: WorkflowActivityAttemptRecord;
  latest_entity_delta?: WorkflowEntityDeltaRecord;
  sample_limit?: number;
  sample_truncated?: boolean;
}

export interface WorkflowCommandListResponse {
  status?: string;
  contract?: string;
  workflow_commands: WorkflowCommandRecord[];
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandDetailResponse {
  status: string;
  contract?: string;
  workflow_command?: WorkflowCommandRecord;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowCommandControlResponse {
  status: string;
  reason?: string;
  command_status?: string;
  workflow_command?: WorkflowCommandRecord;
  operation_sync?: JsonObject;
  display_contract?: WorkflowCommandDisplayContract;
  control_policy?: WorkflowCommandControlPolicy;
  control_state?: WorkflowCommandControlState;
  activity_spine_policy?: WorkflowCommandActivitySpinePolicy;
  module_state_mutated?: boolean;
  owner_specific_control?: boolean;
  contract?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowActivityControlTarget {
  target_type?: string;
  command_id?: string;
  command_type?: string;
  owner?: string;
  display_contract?: WorkflowCommandDisplayContract;
  control_policy?: WorkflowCommandControlPolicy;
  control_state?: WorkflowCommandControlState;
  activity_spine_policy?: WorkflowCommandActivitySpinePolicy;
  fallback_status?: string;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowActivityRecord extends JsonObject {
  activity_run_id?: string;
  workflow_run_id?: string;
  operation_run_id?: string;
  acquisition_run_id?: string;
  command_id?: string;
  activity_type?: string;
  owner?: string;
  status?: string;
  phase?: string;
  mutation_contract?: string;
  module_state_mutated?: boolean;
  control_target?: WorkflowActivityControlTarget;
}

export interface WorkflowActivityAttemptRecord extends JsonObject {
  attempt_id?: string;
  activity_run_id?: string;
  workflow_run_id?: string;
  command_id?: string;
  activity_type?: string;
  owner?: string;
  status?: string;
  provider?: string;
  mutation_contract?: string;
  module_state_mutated?: boolean;
  control_target?: WorkflowActivityControlTarget;
}

export interface WorkflowEntityDeltaRecord extends JsonObject {
  delta_id?: string;
  workflow_run_id?: string;
  operation_run_id?: string;
  command_id?: string;
  activity_run_id?: string;
  attempt_id?: string;
  entity_type?: string;
  entity_key?: string;
  delta_kind?: string;
  status?: string;
  reason?: string;
  mutation_contract?: string;
  module_state_mutated?: boolean;
  control_target?: WorkflowActivityControlTarget;
}

export interface AcquisitionDiscoveryLaneRecord {
  lane_id?: string;
  workspace_id?: string;
  acquisition_run_id?: string;
  workflow_run_id?: string;
  operation_run_id?: string;
  source_command_id?: string;
  activity_run_id?: string;
  target_company?: string;
  query?: string;
  provider?: string;
  status?: string;
  phase?: string;
  read_model_role?: string;
  mutation_contract?: string;
  module_state_mutated?: boolean;
  control_target?: WorkflowActivityControlTarget;
}

export interface WorkflowActivityListResponse {
  status?: string;
  contract?: string;
  workflow_activities: WorkflowActivityRecord[];
  [key: string]: JsonValue | undefined;
}

export interface WorkflowActivityDetailResponse {
  status: string;
  contract?: string;
  workflow_activity?: WorkflowActivityRecord;
  activity_attempts?: WorkflowActivityAttemptRecord[];
  [key: string]: JsonValue | undefined;
}

export interface WorkflowActivityAttemptListResponse {
  status?: string;
  contract?: string;
  workflow_activity_attempts: WorkflowActivityAttemptRecord[];
  [key: string]: JsonValue | undefined;
}

export interface WorkflowActivityAttemptDetailResponse {
  status: string;
  contract?: string;
  workflow_activity_attempt?: WorkflowActivityAttemptRecord;
  [key: string]: JsonValue | undefined;
}

export interface WorkflowEntityDeltaListResponse {
  status?: string;
  contract?: string;
  workflow_entity_deltas: WorkflowEntityDeltaRecord[];
  [key: string]: JsonValue | undefined;
}

export interface WorkflowEntityDeltaDetailResponse {
  status: string;
  contract?: string;
  workflow_entity_delta?: WorkflowEntityDeltaRecord;
  [key: string]: JsonValue | undefined;
}

export interface AcquisitionDiscoveryLaneListResponse {
  status?: string;
  contract?: string;
  acquisition_discovery_lanes: AcquisitionDiscoveryLaneRecord[];
}

export interface AcquisitionDiscoveryLaneDetailResponse {
  status: string;
  contract?: string;
  acquisition_discovery_lane?: AcquisitionDiscoveryLaneRecord;
}

export interface RefinementCompileResponse {
  status: "compiled" | "invalid" | "not_found";
  baseline_job_id?: string;
  reason?: string;
  request_patch?: JsonObject;
  request?: JsonObject;
  request_preview?: JsonObject;
  plan?: SourcingPlanSummary | JsonObject;
  instruction_compiler?: InstructionCompiler;
  baseline_candidate_source?: JsonObject;
  intent_rewrite?: IntentRewritePayload;
  [key: string]: JsonValue | undefined;
}

export interface RefinementApplyResponse {
  status: string;
  baseline_job_id?: string;
  rerun_job_id?: string;
  request_patch?: JsonObject;
  request?: JsonObject;
  request_preview?: JsonObject;
  plan?: SourcingPlanSummary | JsonObject;
  instruction_compiler?: InstructionCompiler;
  intent_rewrite?: IntentRewritePayload;
  diff_id?: number;
  diff_artifact_path?: string;
  diff?: JsonObject;
  rerun_result?: RetrievalJobResponse | JsonObject;
  [key: string]: JsonValue | undefined;
}
