import type {
  InstructionCompiler,
  IntentBrief,
  IntentRewriteEntry,
  IntentRewritePayload,
  JsonObject,
  JobProgressResponse,
  JobResultsResponse,
  MatchResult,
  PlanResponse,
  PlanReviewGate,
  QueryDispatchListResponse,
  QueryDispatchRecord,
  RefinementApplyResponse,
  RefinementCompileResponse,
  RetrievalJobResponse,
  ReviewInstructionCompileResponse,
  ReviewPlanApplyResponse,
  SourcingPlanSummary,
  WorkflowStartResponse,
  WorkerLaneSummary,
  WorkerSummary,
} from "./frontend_api_contract";

export type FetchLike = typeof fetch;

export interface SourcingAgentApiClientOptions {
  baseUrl?: string;
  fetchImpl?: FetchLike;
  defaultHeaders?: HeadersInit;
}

export class SourcingAgentApiError extends Error {
  readonly status: number;
  readonly statusText: string;
  readonly bodyText: string;
  readonly payload?: unknown;

  constructor(params: {
    message: string;
    status: number;
    statusText: string;
    bodyText: string;
    payload?: unknown;
  }) {
    super(params.message);
    this.name = "SourcingAgentApiError";
    this.status = params.status;
    this.statusText = params.statusText;
    this.bodyText = params.bodyText;
    this.payload = params.payload;
  }
}

export class SourcingAgentApiClient {
  private readonly baseUrl: string;
  private readonly fetchImpl: FetchLike;
  private readonly defaultHeaders: HeadersInit;

  constructor(options: SourcingAgentApiClientOptions = {}) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl ?? "");
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.defaultHeaders = options.defaultHeaders ?? {};
  }

  async plan(payload: JsonObject): Promise<PlanResponse> {
    return this.post("/api/plan", payload, mapPlanResponse);
  }

  async compilePlanReviewInstruction(payload: JsonObject): Promise<ReviewInstructionCompileResponse> {
    return this.post("/api/plan/review/compile-instruction", payload, mapReviewInstructionCompileResponse);
  }

  async applyPlanReview(payload: JsonObject): Promise<ReviewPlanApplyResponse> {
    return this.post("/api/plan/review", payload, mapReviewPlanApplyResponse);
  }

  async startWorkflow(payload: JsonObject): Promise<WorkflowStartResponse> {
    return this.post("/api/workflows", payload, mapWorkflowStartResponse);
  }

  async runJob(payload: JsonObject): Promise<RetrievalJobResponse> {
    return this.post("/api/jobs", payload, mapRetrievalJobResponse);
  }

  async getJobProgress(jobId: string): Promise<JobProgressResponse> {
    return this.get(`/api/jobs/${encodeURIComponent(jobId)}/progress`, mapJobProgressResponse);
  }

  async getJobResults(jobId: string): Promise<JobResultsResponse> {
    return this.get(`/api/jobs/${encodeURIComponent(jobId)}/results`, mapJobResultsResponse);
  }

  async compileRefinement(payload: JsonObject): Promise<RefinementCompileResponse> {
    return this.post("/api/results/refine/compile-instruction", payload, mapRefinementCompileResponse);
  }

  async applyRefinement(payload: JsonObject): Promise<RefinementApplyResponse> {
    return this.post("/api/results/refine", payload, mapRefinementApplyResponse);
  }

  async listQueryDispatches(filters: JsonObject = {}): Promise<QueryDispatchListResponse> {
    const query = buildQueryString(filters);
    return this.get(`/api/query-dispatches${query}`, mapQueryDispatchListResponse);
  }

  private async get<T>(path: string, mapper: (payload: unknown) => T): Promise<T> {
    const response = await this.fetchImpl(buildUrl(this.baseUrl, path), {
      method: "GET",
      headers: this.defaultHeaders,
    });
    return parseResponse(response, mapper);
  }

  private async post<T>(path: string, payload: JsonObject, mapper: (payload: unknown) => T): Promise<T> {
    const headers: HeadersInit = {
      "Content-Type": "application/json",
      ...toHeaderRecord(this.defaultHeaders),
    };
    const response = await this.fetchImpl(buildUrl(this.baseUrl, path), {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    return parseResponse(response, mapper);
  }
}

export function createSourcingAgentApiClient(
  options: SourcingAgentApiClientOptions = {},
): SourcingAgentApiClient {
  return new SourcingAgentApiClient(options);
}

export function mapPlanResponse(payload: unknown): PlanResponse {
  const source = asObject(payload, "PlanResponse");
  return {
    ...(source as JsonObject),
    request: asJsonObject(source.request),
    plan: mapSourcingPlanSummary(source.plan),
    plan_review_gate: mapPlanReviewGate(source.plan_review_gate),
    plan_review_session: asJsonObject(source.plan_review_session),
    intent_rewrite: mapIntentRewritePayload(source.intent_rewrite),
    criteria_version_id: asOptionalNumber(source.criteria_version_id),
    criteria_compiler_run_id: asOptionalNumber(source.criteria_compiler_run_id),
    criteria_request_signature: asOptionalString(source.criteria_request_signature),
  };
}

export function mapReviewInstructionCompileResponse(payload: unknown): ReviewInstructionCompileResponse {
  const source = asObject(payload, "ReviewInstructionCompileResponse");
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    review_id: asOptionalNumber(source.review_id),
    reason: asOptionalString(source.reason),
    review_payload: source.review_payload ? asJsonObject(source.review_payload) : undefined,
    instruction_compiler: source.instruction_compiler
      ? mapInstructionCompiler(source.instruction_compiler)
      : undefined,
    intent_rewrite: source.intent_rewrite ? mapIntentRewritePayload(source.intent_rewrite) : undefined,
  };
}

export function mapReviewPlanApplyResponse(payload: unknown): ReviewPlanApplyResponse {
  const source = asObject(payload, "ReviewPlanApplyResponse");
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    review: source.review ? asJsonObject(source.review) : undefined,
    instruction_compiler: source.instruction_compiler
      ? mapInstructionCompiler(source.instruction_compiler)
      : undefined,
    intent_rewrite: source.intent_rewrite ? mapIntentRewritePayload(source.intent_rewrite) : undefined,
  };
}

export function mapWorkflowStartResponse(payload: unknown): WorkflowStartResponse {
  const source = asObject(payload, "WorkflowStartResponse");
  return {
    ...(source as JsonObject),
    job_id: asOptionalString(source.job_id),
    status: asString(source.status),
    stage: asOptionalString(source.stage),
    plan: source.plan ? mapSourcingPlanSummary(source.plan) : undefined,
    plan_review_session: source.plan_review_session ? asJsonObject(source.plan_review_session) : undefined,
    plan_review_gate: source.plan_review_gate ? mapPlanReviewGate(source.plan_review_gate) : undefined,
    reason: asOptionalString(source.reason),
    intent_rewrite: source.intent_rewrite ? mapIntentRewritePayload(source.intent_rewrite) : undefined,
    dispatch: source.dispatch ? mapQueryDispatchRecord(source.dispatch) : undefined,
    criteria_version_id: asOptionalNumber(source.criteria_version_id),
    criteria_compiler_run_id: asOptionalNumber(source.criteria_compiler_run_id),
    criteria_request_signature: asOptionalString(source.criteria_request_signature),
  };
}

export function mapJobProgressResponse(payload: unknown): JobProgressResponse {
  const source = asObject(payload, "JobProgressResponse");
  return {
    ...(source as JsonObject),
    job_id: asString(source.job_id),
    status: asString(source.status),
    stage: asString(source.stage),
    started_at: asOptionalString(source.started_at),
    updated_at: asOptionalString(source.updated_at),
    elapsed_seconds: asOptionalNumber(source.elapsed_seconds),
    blocked_task: asOptionalString(source.blocked_task),
    current_message: asOptionalString(source.current_message),
    progress: mapProgressPayload(source.progress),
  };
}

export function mapJobResultsResponse(payload: unknown): JobResultsResponse {
  const source = asObject(payload, "JobResultsResponse");
  return {
    ...(source as JsonObject),
    job: asJsonObject(source.job),
    events: asObjectArray(source.events),
    results: asObjectArray(source.results),
    manual_review_items: asObjectArray(source.manual_review_items),
    agent_runtime_session: asJsonObject(source.agent_runtime_session),
    agent_trace_spans: asObjectArray(source.agent_trace_spans),
    agent_workers: asObjectArray(source.agent_workers),
    intent_rewrite: mapIntentRewritePayload(source.intent_rewrite),
  };
}

export function mapRetrievalJobResponse(payload: unknown): RetrievalJobResponse {
  const source = asObject(payload, "RetrievalJobResponse");
  return {
    ...(source as JsonObject),
    job_id: asString(source.job_id),
    status: asString(source.status),
    request: asJsonObject(source.request),
    plan: mapSourcingPlanSummary(source.plan),
    intent_rewrite: mapIntentRewritePayload(source.intent_rewrite),
    summary: asJsonObject(source.summary),
    matches: asArray(source.matches).map(mapMatchResult),
    manual_review_items: asObjectArray(source.manual_review_items),
    criteria_patterns_applied: source.criteria_patterns_applied
      ? asObjectArray(source.criteria_patterns_applied)
      : undefined,
    semantic_hits: source.semantic_hits ? asObjectArray(source.semantic_hits) : undefined,
    confidence_policy: source.confidence_policy ? asJsonObject(source.confidence_policy) : undefined,
    confidence_policy_control: source.confidence_policy_control
      ? asJsonObject(source.confidence_policy_control)
      : undefined,
    runtime_policy: source.runtime_policy ? asJsonObject(source.runtime_policy) : undefined,
    artifact_path: asOptionalString(source.artifact_path),
  };
}

export function mapRefinementCompileResponse(payload: unknown): RefinementCompileResponse {
  const source = asObject(payload, "RefinementCompileResponse");
  return {
    ...(source as JsonObject),
    status: asString(source.status) as RefinementCompileResponse["status"],
    baseline_job_id: asOptionalString(source.baseline_job_id),
    reason: asOptionalString(source.reason),
    request_patch: source.request_patch ? asJsonObject(source.request_patch) : undefined,
    request: source.request ? asJsonObject(source.request) : undefined,
    plan: source.plan ? mapSourcingPlanSummary(source.plan) : undefined,
    instruction_compiler: source.instruction_compiler
      ? mapInstructionCompiler(source.instruction_compiler)
      : undefined,
    baseline_candidate_source: source.baseline_candidate_source
      ? asJsonObject(source.baseline_candidate_source)
      : undefined,
    intent_rewrite: source.intent_rewrite ? mapIntentRewritePayload(source.intent_rewrite) : undefined,
  };
}

export function mapRefinementApplyResponse(payload: unknown): RefinementApplyResponse {
  const source = asObject(payload, "RefinementApplyResponse");
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    baseline_job_id: asOptionalString(source.baseline_job_id),
    rerun_job_id: asOptionalString(source.rerun_job_id),
    request_patch: source.request_patch ? asJsonObject(source.request_patch) : undefined,
    request: source.request ? asJsonObject(source.request) : undefined,
    plan: source.plan ? mapSourcingPlanSummary(source.plan) : undefined,
    instruction_compiler: source.instruction_compiler
      ? mapInstructionCompiler(source.instruction_compiler)
      : undefined,
    intent_rewrite: source.intent_rewrite ? mapIntentRewritePayload(source.intent_rewrite) : undefined,
    diff_id: asOptionalNumber(source.diff_id),
    diff_artifact_path: asOptionalString(source.diff_artifact_path),
    diff: source.diff ? asJsonObject(source.diff) : undefined,
    rerun_result: source.rerun_result ? mapRetrievalJobResponse(source.rerun_result) : undefined,
  };
}

export function mapIntentRewritePayload(payload: unknown): IntentRewritePayload {
  const source = asObject(payload, "IntentRewritePayload");
  return {
    request: mapIntentRewriteEntry(source.request),
    instruction: source.instruction ? mapIntentRewriteEntry(source.instruction) : undefined,
  };
}

export function mapIntentRewriteEntry(payload: unknown): IntentRewriteEntry {
  const source = asObject(payload, "IntentRewriteEntry");
  return {
    matched: Boolean(source.matched),
    summary: asOptionalString(source.summary) ?? "",
    rewrite: source.rewrite ? mapIntentRewriteRule(source.rewrite) : {},
  };
}

export function mapIntentRewriteRule(payload: unknown) {
  const source = asObject(payload, "IntentRewriteRule");
  return {
    ...(source as JsonObject),
    rewrite_id: asOptionalString(source.rewrite_id),
    summary_label: asOptionalString(source.summary_label),
    keywords: asOptionalStringArray(source.keywords),
    targeting_terms: asOptionalStringArray(source.targeting_terms),
    matched_terms: asOptionalStringArray(source.matched_terms),
    notes: asOptionalString(source.notes),
  };
}

export function mapIntentBrief(payload: unknown): IntentBrief {
  const source = asObject(payload, "IntentBrief");
  return {
    identified_request: asOptionalStringArray(source.identified_request),
    target_output: asOptionalStringArray(source.target_output),
    default_execution_strategy: asOptionalStringArray(source.default_execution_strategy),
    review_focus: asOptionalStringArray(source.review_focus),
  };
}

export function mapPlanReviewGate(payload: unknown): PlanReviewGate {
  const source = asObject(payload, "PlanReviewGate");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    required_before_execution: asOptionalBoolean(source.required_before_execution),
    risk_level: asOptionalString(source.risk_level),
    reasons: asOptionalStringArray(source.reasons),
    confirmation_items: asOptionalStringArray(source.confirmation_items),
    editable_fields: asOptionalStringArray(source.editable_fields),
    suggested_actions: asOptionalStringArray(source.suggested_actions),
  };
}

export function mapSourcingPlanSummary(payload: unknown): SourcingPlanSummary {
  const source = asObject(payload, "SourcingPlanSummary");
  return {
    ...(source as JsonObject),
    target_company: asOptionalString(source.target_company),
    intent_summary: asOptionalString(source.intent_summary),
    criteria_summary: asOptionalString(source.criteria_summary),
    intent_brief: source.intent_brief ? mapIntentBrief(source.intent_brief) : undefined,
    retrieval_plan: source.retrieval_plan ? asJsonObject(source.retrieval_plan) : undefined,
    acquisition_strategy: source.acquisition_strategy ? asJsonObject(source.acquisition_strategy) : undefined,
    acquisition_tasks: source.acquisition_tasks ? asObjectArray(source.acquisition_tasks) : undefined,
    search_strategy: source.search_strategy ? asJsonObject(source.search_strategy) : undefined,
    open_questions: asOptionalStringArray(source.open_questions),
    assumptions: asOptionalStringArray(source.assumptions),
  };
}

export function mapInstructionCompiler(payload: unknown): InstructionCompiler {
  const source = asObject(payload, "InstructionCompiler");
  return {
    ...(source as JsonObject),
    source: asOptionalString(source.source),
    provider: asOptionalString(source.provider),
    allowed_fields: asOptionalStringArray(source.allowed_fields),
    model_decision: source.model_decision ? asJsonObject(source.model_decision) : undefined,
    deterministic_decision: source.deterministic_decision
      ? asJsonObject(source.deterministic_decision)
      : undefined,
    model_patch: source.model_patch ? asJsonObject(source.model_patch) : undefined,
    deterministic_patch: source.deterministic_patch ? asJsonObject(source.deterministic_patch) : undefined,
    supplemented_keys: asOptionalStringArray(source.supplemented_keys),
    policy_inferred_keys: asOptionalStringArray(source.policy_inferred_keys),
    fallback_used: asOptionalBoolean(source.fallback_used),
    request_intent_rewrite: source.request_intent_rewrite
      ? mapIntentRewriteRule(source.request_intent_rewrite)
      : undefined,
    instruction_intent_rewrite: source.instruction_intent_rewrite
      ? mapIntentRewriteRule(source.instruction_intent_rewrite)
      : undefined,
  };
}

export function mapProgressPayload(payload: unknown): JobProgressResponse["progress"] {
  const source = asObject(payload, "ProgressPayload");
  return {
    ...(source as JsonObject),
    stage_order: asOptionalStringArray(source.stage_order),
    current_stage: asOptionalString(source.current_stage) ?? "",
    completed_stages: asOptionalStringArray(source.completed_stages),
    milestones: asArray(source.milestones).map(mapJobMilestone),
    timing: source.timing ? asJsonObject(source.timing) : {},
    latest_event: source.latest_event ? asJsonObject(source.latest_event) : undefined,
    worker_summary: mapWorkerSummary(source.worker_summary),
    latest_metrics: source.latest_metrics ? asJsonObject(source.latest_metrics) : undefined,
    counters: asRecordOfNumber(source.counters),
  };
}

export function mapJobMilestone(payload: unknown): JobProgressResponse["progress"]["milestones"][number] {
  const source = asObject(payload, "JobMilestone");
  return {
    ...(source as JsonObject),
    stage: asString(source.stage),
    status: asString(source.status),
    started_at: asOptionalString(source.started_at),
    completed_at: asOptionalString(source.completed_at),
    latest_detail: asOptionalString(source.latest_detail),
    event_count: asOptionalNumber(source.event_count),
    elapsed_seconds: asOptionalNumber(source.elapsed_seconds),
  };
}

export function mapWorkerSummary(payload: unknown): WorkerSummary {
  const source = asObject(payload, "WorkerSummary");
  return {
    ...(source as JsonObject),
    by_status: asRecordOfNumber(source.by_status),
    raw_by_status: asRecordOfNumber(source.raw_by_status),
    by_lane: asArray(source.by_lane).map(mapWorkerLaneSummary),
  };
}

export function mapWorkerLaneSummary(payload: unknown): WorkerLaneSummary {
  const source = asObject(payload, "WorkerLaneSummary");
  return {
    ...(source as JsonObject),
    lane_id: asString(source.lane_id),
    worker_count: asOptionalNumber(source.worker_count) ?? 0,
    by_status: asRecordOfNumber(source.by_status),
    raw_by_status: asRecordOfNumber(source.raw_by_status),
    last_updated_at: asOptionalString(source.last_updated_at),
  };
}

export function mapMatchResult(payload: unknown): MatchResult {
  const source = asObject(payload, "MatchResult");
  return {
    ...(source as JsonObject),
    candidate_id: asOptionalString(source.candidate_id),
    display_name: asOptionalString(source.display_name),
    name_en: asOptionalString(source.name_en),
    name_zh: asOptionalString(source.name_zh),
    category: asOptionalString(source.category),
    target_company: asOptionalString(source.target_company),
    organization: asOptionalString(source.organization),
    employment_status: asOptionalString(source.employment_status),
    role_bucket: asOptionalString(source.role_bucket),
    functional_facets: asOptionalStringArray(source.functional_facets),
    role: asOptionalString(source.role),
    team: asOptionalString(source.team),
    focus_areas: asOptionalString(source.focus_areas),
    education: asOptionalString(source.education),
    work_history: asOptionalString(source.work_history),
    notes: asOptionalString(source.notes),
    linkedin_url: asOptionalString(source.linkedin_url),
    score: asOptionalNumber(source.score),
    semantic_score: asOptionalNumber(source.semantic_score),
    confidence_label: asOptionalString(source.confidence_label),
    confidence_score: asOptionalNumber(source.confidence_score),
    confidence_reason: asOptionalString(source.confidence_reason),
    rank: asOptionalNumber(source.rank),
    matched_fields: source.matched_fields ? asObjectArray(source.matched_fields) : undefined,
    explanation: asOptionalString(source.explanation),
    evidence: source.evidence ? asObjectArray(source.evidence) : undefined,
  };
}

export function mapQueryDispatchListResponse(payload: unknown): QueryDispatchListResponse {
  const source = asObject(payload, "QueryDispatchListResponse");
  return {
    ...(source as JsonObject),
    query_dispatches: asArray(source.query_dispatches).map(mapQueryDispatchRecord),
  };
}

export function mapQueryDispatchRecord(payload: unknown): QueryDispatchRecord {
  const source = asObject(payload, "QueryDispatchRecord");
  return {
    ...(source as JsonObject),
    dispatch_id: asOptionalNumber(source.dispatch_id),
    target_company: asOptionalString(source.target_company),
    request_signature: asOptionalString(source.request_signature),
    request_family_signature: asOptionalString(source.request_family_signature),
    requester_id: asOptionalString(source.requester_id),
    tenant_id: asOptionalString(source.tenant_id),
    idempotency_key: asOptionalString(source.idempotency_key),
    strategy: asOptionalString(source.strategy),
    status: asOptionalString(source.status),
    source_job_id: asOptionalString(source.source_job_id),
    created_job_id: asOptionalString(source.created_job_id),
    payload: source.payload ? asJsonObject(source.payload) : undefined,
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
  };
}

async function parseResponse<T>(response: Response, mapper: (payload: unknown) => T): Promise<T> {
  const bodyText = await response.text();
  const payload = bodyText ? safeJsonParse(bodyText) : {};
  if (!response.ok) {
    throw new SourcingAgentApiError({
      message: `SourcingAgent API request failed: ${response.status} ${response.statusText}`,
      status: response.status,
      statusText: response.statusText,
      bodyText,
      payload,
    });
  }
  return mapper(payload);
}

function safeJsonParse(input: string): unknown {
  try {
    return JSON.parse(input);
  } catch {
    return {};
  }
}

function buildUrl(baseUrl: string, path: string): string {
  if (!baseUrl) {
    return path;
  }
  return `${baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
}

function buildQueryString(params: JsonObject): string {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) {
      continue;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        if (isQueryParamPrimitive(item)) {
          searchParams.append(key, String(item));
        }
      }
      continue;
    }
    if (isQueryParamPrimitive(value)) {
      searchParams.set(key, String(value));
    }
  }
  const encoded = searchParams.toString();
  return encoded ? `?${encoded}` : "";
}

function isQueryParamPrimitive(value: unknown): value is string | number | boolean {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}

function normalizeBaseUrl(value: string): string {
  const normalized = value.trim();
  if (!normalized) {
    return "";
  }
  return normalized.endsWith("/") ? normalized.slice(0, -1) : normalized;
}

function toHeaderRecord(headers: HeadersInit): Record<string, string> {
  if (headers instanceof Headers) {
    return Object.fromEntries(headers.entries());
  }
  if (Array.isArray(headers)) {
    return Object.fromEntries(headers);
  }
  return { ...headers };
}

function asObject(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function asJsonObject(value: unknown): JsonObject {
  return asObject(value, "JsonObject") as JsonObject;
}

function asObjectArray(value: unknown): JsonObject[] {
  return asArray(value).map((item) => asJsonObject(item));
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asString(value: unknown): string {
  if (typeof value !== "string") {
    throw new Error("Expected string");
  }
  return value;
}

function asOptionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asOptionalBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function asOptionalNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function asOptionalStringArray(value: unknown): string[] {
  return asArray(value).filter((item): item is string => typeof item === "string");
}

function asRecordOfNumber(value: unknown): Record<string, number> {
  const source = value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
  const result: Record<string, number> = {};
  for (const [key, item] of Object.entries(source)) {
    if (typeof item === "number" && Number.isFinite(item)) {
      result[key] = item;
    }
  }
  return result;
}
