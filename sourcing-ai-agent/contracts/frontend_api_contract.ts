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
  keywords?: string[];
  targeting_terms?: string[];
  matched_terms?: string[];
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

export interface ProgressPayload {
  stage_order: string[];
  current_stage: string;
  completed_stages: string[];
  milestones: JobMilestone[];
  timing: JsonObject;
  latest_event?: JsonObject;
  worker_summary: WorkerSummary;
  latest_metrics?: JsonObject;
  counters: Record<string, number>;
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
  [key: string]: JsonValue | undefined;
}

export interface RetrievalJobResponse {
  job_id: string;
  status: string;
  request: JsonObject;
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

export interface RefinementCompileResponse {
  status: "compiled" | "invalid" | "not_found";
  baseline_job_id?: string;
  reason?: string;
  request_patch?: JsonObject;
  request?: JsonObject;
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
  plan?: SourcingPlanSummary | JsonObject;
  instruction_compiler?: InstructionCompiler;
  intent_rewrite?: IntentRewritePayload;
  diff_id?: number;
  diff_artifact_path?: string;
  diff?: JsonObject;
  rerun_result?: RetrievalJobResponse | JsonObject;
  [key: string]: JsonValue | undefined;
}
