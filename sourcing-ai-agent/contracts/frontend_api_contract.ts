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
}

export interface IntentRewritePayload {
  request: IntentRewriteEntry;
  instruction?: IntentRewriteEntry;
  policy_catalog?: IntentRewritePolicyCatalogEntry[];
}

export interface IntentBrief {
  identified_request: string[];
  target_output: string[];
  default_execution_strategy: string[];
  review_focus: string[];
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
  matched_fields?: JsonObject[];
  explanation?: string;
  evidence?: JsonObject[];
  [key: string]: JsonValue | undefined;
}

export interface JobResultsResponse {
  job: JsonObject;
  events: JsonObject[];
  results: JsonObject[];
  manual_review_items: JsonObject[];
  agent_runtime_session: JsonObject;
  agent_trace_spans: JsonObject[];
  agent_workers: JsonObject[];
  intent_rewrite: IntentRewritePayload;
  request_preview?: JsonObject;
  workflow_stage_summaries?: WorkflowStageSummariesPayload;
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
