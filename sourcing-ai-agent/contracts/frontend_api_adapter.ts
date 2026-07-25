import {
  OPERATION_ACTION_DECISION_APPLIED_OUTCOMES,
  OPERATION_ACTION_DETAIL_SUCCESS_STATUSES,
  OPERATION_ACTION_QUERY_SUCCESS_STATUSES,
  OPERATION_ACTION_SUBMIT_FRESH_OUTCOMES,
  OPERATION_ACTION_SUBMIT_REPLAY_OUTCOMES,
  OPERATION_RUN_CONTROL_APPLIED_OUTCOMES,
  OPERATION_RUN_PROVENANCE_SUCCESS_STATUSES,
  WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES,
  WORKFLOW_PUBLIC_PROJECTION_LIMITS,
  workflowPublicJsonStringByteLength,
  workflowPublicTransportBodyIsOverLimit,
  workflowPublicTransportContentLengthIsOverLimit,
  workflowPublicUtf8ByteLength,
} from "./frontend_api_runtime_contract";
import type {
  AcquisitionDiscoveryLaneDetailResponse,
  AcquisitionDiscoveryLaneListResponse,
  AcquisitionDiscoveryLaneRecord,
  InstructionCompiler,
  IntentBrief,
  IntentRewritePolicyCatalogEntry,
  IntentRewriteEntry,
  IntentRewritePayload,
  JsonObject,
  JsonValue,
  JobProgressResponse,
  JobResultsResponse,
  JobRuntimeHealth,
  MatchResult,
  OperationActionDetailResponse,
  OperationActionDisplayContract,
  OperationActionListResponse,
  OperationActionRecord,
  OperationActionRegistryEntry,
  OperationActionRegistryResponse,
  OperationEventRecord,
  OperationRunControlResponse,
  OperationRunDetailResponse,
  OperationRunListResponse,
  OperationRunProvenanceResponse,
  OperationRunControlState,
  OperationRunRecord,
  OperationRunStatusSummary,
  PlanResponse,
  PlanReviewGate,
  ProgressMetrics,
  QueryDispatchListResponse,
  QueryDispatchRecord,
  RecoveryControlSummary,
  RefreshMetricsSummary,
  RefinementApplyResponse,
  RefinementCompileResponse,
  RetrievalJobResponse,
  ReviewInstructionCompileResponse,
  ReviewPlanApplyResponse,
  RuntimeHealthResponse,
  RuntimeHealthServicesSummary,
  RuntimeJobRecoveryItem,
  RuntimeMetricsResponse,
  RuntimeRecoverableWorkerItem,
  RuntimeRecoverableWorkersSummary,
  RuntimeRefreshMetricsSummary,
  RuntimeStaleJobItem,
  RuntimeStaleJobsSummary,
  RuntimeServicesSummary,
  ServiceStatusPayload,
  SystemProgressResponse,
  SourcingPlanSummary,
  TargetCandidatePublicWebBatch,
  TargetCandidatePublicWebDetailResponse,
  TargetCandidatePublicWebEvidenceLink,
  TargetCandidatePublicWebPromotion,
  TargetCandidatePublicWebPromotionResponse,
  TargetCandidatePublicWebRun,
  TargetCandidatePublicWebSearchState,
  TargetCandidatePublicWebSignal,
  TargetCandidatePublicWebStartResponse,
  TargetCandidatePublicWebStatus,
  WorkflowExplainResponse,
  WorkflowActivityAttemptDetailResponse,
  WorkflowActivityAttemptListResponse,
  WorkflowActivityAttemptRecord,
  WorkflowActivityControlTarget,
  WorkflowActivityDetailResponse,
  WorkflowActivityListResponse,
  WorkflowActivityRecord,
  WorkflowCommandActivitySpinePolicy,
  WorkflowCommandContract,
  WorkflowCommandControlResponse,
  WorkflowCommandControlSummary,
  WorkflowCommandControlPolicy,
  WorkflowCommandControlState,
  WorkflowCommandDetailResponse,
  WorkflowCommandDisplayContract,
  WorkflowCommandExecutionSummary,
  WorkflowCommandListResponse,
  WorkflowCommandOperationSync,
  WorkflowCommandRecord,
  WorkflowCommandRegistryResponse,
  WorkflowEntityDeltaDetailResponse,
  WorkflowEntityDeltaListResponse,
  WorkflowEntityDeltaRecord,
  WorkflowStartResponse,
  WorkflowStageSummariesPayload,
  WorkflowStageSummaryItem,
  WorkerLaneSummary,
  WorkerSummary,
} from "./frontend_api_contract";

export type FetchLike = typeof fetch;

export type PublicResponseForStatuses<
  TResponse extends { status: string },
  TStatuses extends readonly string[],
> = Omit<TResponse, "status"> & { status: TStatuses[number] };

type OperationActionFreshSubmitResponse = Omit<
  OperationActionDetailResponse,
  "status" | "idempotent_replay"
> & {
  status: (typeof OPERATION_ACTION_SUBMIT_FRESH_OUTCOMES)[number];
  idempotent_replay: false;
};
type OperationActionReplaySubmitResponse = Omit<
  OperationActionDetailResponse,
  "status" | "idempotent_replay"
> & {
  status: (typeof OPERATION_ACTION_SUBMIT_REPLAY_OUTCOMES)[number];
  idempotent_replay: true;
};
export type OperationActionSubmitResponse =
  | OperationActionFreshSubmitResponse
  | OperationActionReplaySubmitResponse;
export type OperationActionQueryResponse = PublicResponseForStatuses<
  OperationActionDetailResponse,
  typeof OPERATION_ACTION_QUERY_SUCCESS_STATUSES
>;
export type OperationActionDecisionResponse<
  TDecision extends keyof typeof OPERATION_ACTION_DECISION_APPLIED_OUTCOMES,
> = PublicResponseForStatuses<
  OperationActionDetailResponse,
  (typeof OPERATION_ACTION_DECISION_APPLIED_OUTCOMES)[TDecision]
>;
export type OperationRunControlResponseFor<
  TAction extends keyof typeof OPERATION_RUN_CONTROL_APPLIED_OUTCOMES,
> = PublicResponseForStatuses<
  OperationRunControlResponse,
  (typeof OPERATION_RUN_CONTROL_APPLIED_OUTCOMES)[TAction]
>;
export type WorkflowCommandControlResponseFor<
  TAction extends keyof typeof WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES,
> = PublicResponseForStatuses<
  WorkflowCommandControlResponse,
  (typeof WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES)[TAction]
>;

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

  async explainWorkflow(payload: JsonObject): Promise<WorkflowExplainResponse> {
    return this.post("/api/workflows/explain", payload, mapWorkflowExplainResponse);
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

  async getSystemProgress(filters: JsonObject = {}): Promise<SystemProgressResponse> {
    const query = buildQueryString(filters);
    return this.get(`/api/runtime/progress${query}`, mapSystemProgressResponse);
  }

  async getRuntimeMetrics(filters: JsonObject = {}): Promise<RuntimeMetricsResponse> {
    const query = buildQueryString(filters);
    return this.get(`/api/runtime/metrics${query}`, mapRuntimeMetricsResponse);
  }

  async getRuntimeHealth(filters: JsonObject = {}): Promise<RuntimeHealthResponse> {
    const query = buildQueryString(filters);
    return this.get(`/api/runtime/health${query}`, mapRuntimeHealthResponse);
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

  async getOperationActionRegistry(): Promise<OperationActionRegistryResponse> {
    return this.get("/api/operations/action-registry", mapOperationActionRegistryResponse);
  }

  async listOperationActions(filters: JsonObject = {}): Promise<OperationActionListResponse> {
    const query = buildQueryString(filters);
    return this.getWorkflowPublic(`/api/operations/actions${query}`, mapOperationActionListResponse);
  }

  async submitOperationAction(payload: JsonObject): Promise<OperationActionSubmitResponse> {
    return this.postWorkflowPublic(
      "/api/operations/actions",
      payload,
      mapOperationActionSubmitResponse,
    );
  }

  async getOperationAction(actionId: string): Promise<OperationActionQueryResponse> {
    return this.getWorkflowPublic(
      `/api/operations/actions/${encodeURIComponent(actionId)}`,
      (response) => mapPublicResponseForStatuses(
        response,
        mapOperationActionDetailResponse,
        OPERATION_ACTION_QUERY_SUCCESS_STATUSES,
        "Operation action query",
      ),
    );
  }

  async approveOperationAction(
    actionId: string,
    payload: JsonObject = {},
  ): Promise<OperationActionDecisionResponse<"approve">> {
    return this.postWorkflowPublic(
      `/api/operations/actions/${encodeURIComponent(actionId)}/approve`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapOperationActionDetailResponse,
        OPERATION_ACTION_DECISION_APPLIED_OUTCOMES.approve,
        "Operation action approve",
      ),
    );
  }

  async rejectOperationAction(
    actionId: string,
    payload: JsonObject = {},
  ): Promise<OperationActionDecisionResponse<"reject">> {
    return this.postWorkflowPublic(
      `/api/operations/actions/${encodeURIComponent(actionId)}/reject`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapOperationActionDetailResponse,
        OPERATION_ACTION_DECISION_APPLIED_OUTCOMES.reject,
        "Operation action reject",
      ),
    );
  }

  async listOperationRuns(filters: JsonObject = {}): Promise<OperationRunListResponse> {
    const query = buildQueryString(filters);
    return this.getWorkflowPublic(`/api/operations/runs${query}`, mapOperationRunListResponse);
  }

  async getOperationRun(operationRunId: string): Promise<OperationRunDetailResponse> {
    return this.getWorkflowPublic(
      `/api/operations/runs/${encodeURIComponent(operationRunId)}`,
      mapOperationRunDetailResponse,
    );
  }

  async getOperationRunProvenance(operationRunId: string): Promise<OperationRunProvenanceResponse> {
    return this.getWorkflowPublic(
      `/api/operations/runs/${encodeURIComponent(operationRunId)}/provenance`,
      mapOperationRunProvenanceResponse,
    );
  }

  async cancelOperationRun(
    operationRunId: string,
    payload: JsonObject = {},
  ): Promise<OperationRunControlResponseFor<"cancel">> {
    return this.postWorkflowPublic(
      `/api/operations/runs/${encodeURIComponent(operationRunId)}/cancel`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapOperationRunControlResponse,
        OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.cancel,
        "Operation run cancel",
      ),
    );
  }

  async retryOperationRun(
    operationRunId: string,
    payload: JsonObject = {},
  ): Promise<OperationRunControlResponseFor<"retry">> {
    return this.postWorkflowPublic(
      `/api/operations/runs/${encodeURIComponent(operationRunId)}/retry`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapOperationRunControlResponse,
        OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.retry,
        "Operation run retry",
      ),
    );
  }

  async resumeOperationRun(
    operationRunId: string,
    payload: JsonObject = {},
  ): Promise<OperationRunControlResponseFor<"resume">> {
    return this.postWorkflowPublic(
      `/api/operations/runs/${encodeURIComponent(operationRunId)}/resume`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapOperationRunControlResponse,
        OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.resume,
        "Operation run resume",
      ),
    );
  }

  async dispatchOperationRun(
    operationRunId: string,
    payload: JsonObject = {},
  ): Promise<OperationRunControlResponseFor<"dispatch">> {
    return this.postWorkflowPublic(
      `/api/operations/runs/${encodeURIComponent(operationRunId)}/dispatch`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapOperationRunControlResponse,
        OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.dispatch,
        "Operation run dispatch",
      ),
    );
  }

  async getWorkflowCommandRegistry(): Promise<WorkflowCommandRegistryResponse> {
    return this.get("/api/workflow/command-registry", mapWorkflowCommandRegistryResponse);
  }

  async listWorkflowCommands(filters: JsonObject = {}): Promise<WorkflowCommandListResponse> {
    const query = buildQueryString(filters);
    return this.getWorkflowPublic(`/api/workflow/commands${query}`, mapWorkflowCommandListResponse);
  }

  async getWorkflowCommand(commandId: string): Promise<WorkflowCommandDetailResponse> {
    return this.getWorkflowPublic(
      `/api/workflow/commands/${encodeURIComponent(commandId)}`,
      mapWorkflowCommandDetailResponse,
    );
  }

  async cancelWorkflowCommand(
    commandId: string,
    payload: JsonObject = {},
  ): Promise<WorkflowCommandControlResponseFor<"cancel">> {
    return this.postWorkflowPublic(
      `/api/workflow/commands/${encodeURIComponent(commandId)}/cancel`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapWorkflowCommandControlResponse,
        WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES.cancel,
        "Workflow command cancel",
      ),
    );
  }

  async retryWorkflowCommand(
    commandId: string,
    payload: JsonObject = {},
  ): Promise<WorkflowCommandControlResponseFor<"retry">> {
    return this.postWorkflowPublic(
      `/api/workflow/commands/${encodeURIComponent(commandId)}/retry`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapWorkflowCommandControlResponse,
        WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES.retry,
        "Workflow command retry",
      ),
    );
  }

  async resumeWorkflowCommand(
    commandId: string,
    payload: JsonObject = {},
  ): Promise<WorkflowCommandControlResponseFor<"resume">> {
    return this.postWorkflowPublic(
      `/api/workflow/commands/${encodeURIComponent(commandId)}/resume`,
      payload,
      (response) => mapPublicResponseForStatuses(
        response,
        mapWorkflowCommandControlResponse,
        WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES.resume,
        "Workflow command resume",
      ),
    );
  }

  async listWorkflowActivities(filters: JsonObject = {}): Promise<WorkflowActivityListResponse> {
    const query = buildQueryString(filters);
    return this.getWorkflowPublic(`/api/workflow/activities${query}`, mapWorkflowActivityListResponse);
  }

  async getWorkflowActivity(activityRunId: string): Promise<WorkflowActivityDetailResponse> {
    return this.getWorkflowPublic(
      `/api/workflow/activities/${encodeURIComponent(activityRunId)}`,
      mapWorkflowActivityDetailResponse,
    );
  }

  async listWorkflowActivityAttempts(filters: JsonObject = {}): Promise<WorkflowActivityAttemptListResponse> {
    const query = buildQueryString(filters);
    return this.getWorkflowPublic(
      `/api/workflow/activity-attempts${query}`,
      mapWorkflowActivityAttemptListResponse,
    );
  }

  async getWorkflowActivityAttempt(attemptId: string): Promise<WorkflowActivityAttemptDetailResponse> {
    return this.getWorkflowPublic(
      `/api/workflow/activity-attempts/${encodeURIComponent(attemptId)}`,
      mapWorkflowActivityAttemptDetailResponse,
    );
  }

  async listWorkflowEntityDeltas(filters: JsonObject = {}): Promise<WorkflowEntityDeltaListResponse> {
    const query = buildQueryString(filters);
    return this.getWorkflowPublic(`/api/workflow/entity-deltas${query}`, mapWorkflowEntityDeltaListResponse);
  }

  async getWorkflowEntityDelta(deltaId: string): Promise<WorkflowEntityDeltaDetailResponse> {
    return this.getWorkflowPublic(
      `/api/workflow/entity-deltas/${encodeURIComponent(deltaId)}`,
      mapWorkflowEntityDeltaDetailResponse,
    );
  }

  async listAcquisitionDiscoveryLanes(filters: JsonObject = {}): Promise<AcquisitionDiscoveryLaneListResponse> {
    const query = buildQueryString(filters);
    return this.get(`/api/workflow/discovery-lanes${query}`, mapAcquisitionDiscoveryLaneListResponse);
  }

  async getAcquisitionDiscoveryLane(laneId: string): Promise<AcquisitionDiscoveryLaneDetailResponse> {
    return this.get(
      `/api/workflow/discovery-lanes/${encodeURIComponent(laneId)}`,
      mapAcquisitionDiscoveryLaneDetailResponse,
    );
  }

  async listTargetCandidatePublicWebSearches(
    filters: JsonObject = {},
  ): Promise<TargetCandidatePublicWebSearchState> {
    return this.post(
      "/api/crm/records/public-web-search/poll",
      normalizeCrmPublicWebFilters(filters),
      mapTargetCandidatePublicWebSearchState,
    );
  }

  async getTargetCandidatePublicWebSearchDetail(
    recordId: string,
  ): Promise<TargetCandidatePublicWebDetailResponse> {
    return this.get(
      `/api/crm/records/${encodeURIComponent(recordId)}/public-web-search`,
      mapTargetCandidatePublicWebDetailResponse,
    );
  }

  async promoteTargetCandidatePublicWebSignal(
    recordId: string,
    payload: JsonObject,
  ): Promise<TargetCandidatePublicWebPromotionResponse> {
    return this.post(
      `/api/crm/records/${encodeURIComponent(recordId)}/public-web-promotions`,
      payload,
      mapTargetCandidatePublicWebPromotionResponse,
    );
  }

  async startTargetCandidatePublicWebSearch(
    payload: JsonObject,
  ): Promise<TargetCandidatePublicWebStartResponse> {
    return this.post(
      "/api/crm/records/public-web-search",
      normalizeCrmPublicWebStartPayload(payload),
      mapTargetCandidatePublicWebStartResponse,
    );
  }

  private async get<T>(path: string, mapper: (payload: unknown) => T): Promise<T> {
    const response = await this.fetchImpl(buildUrl(this.baseUrl, path), {
      method: "GET",
      headers: this.defaultHeaders,
    });
    return parseResponse(response, mapper, "unbounded");
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
    return parseResponse(response, mapper, "unbounded");
  }

  private async getWorkflowPublic<T>(
    path: string,
    mapper: (payload: unknown) => T,
  ): Promise<T> {
    const response = await this.fetchImpl(buildUrl(this.baseUrl, path), {
      method: "GET",
      headers: this.defaultHeaders,
    });
    return parseResponse(response, mapper, "workflow-public");
  }

  private async postWorkflowPublic<T>(
    path: string,
    payload: JsonObject,
    mapper: (payload: unknown) => T,
  ): Promise<T> {
    const headers: HeadersInit = {
      "Content-Type": "application/json",
      ...toHeaderRecord(this.defaultHeaders),
    };
    const response = await this.fetchImpl(buildUrl(this.baseUrl, path), {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    return parseResponse(response, mapper, "workflow-public");
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
    request_preview: source.request_preview ? asJsonObject(source.request_preview) : undefined,
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
    status: normalizeReviewInstructionCompileStatus(source.status),
    review_id: asOptionalNumber(source.review_id),
    reason: asOptionalString(source.reason),
    review_payload: source.review_payload ? asJsonObject(source.review_payload) : undefined,
    instruction_compiler: source.instruction_compiler
      ? mapInstructionCompiler(source.instruction_compiler)
      : undefined,
    intent_rewrite: source.intent_rewrite ? mapIntentRewritePayload(source.intent_rewrite) : undefined,
  };
}

function normalizeReviewInstructionCompileStatus(value: unknown): ReviewInstructionCompileResponse["status"] {
  const normalized = String(value || "").trim();
  if (normalized === "compiled" || normalized === "invalid" || normalized === "not_found") {
    return normalized;
  }
  return "invalid";
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

export function mapWorkflowExplainResponse(payload: unknown): WorkflowExplainResponse {
  const source = asObject(payload, "WorkflowExplainResponse");
  const cloudAssetOperations = asObject(
    source.cloud_asset_operations ?? {},
    "WorkflowExplainResponse.cloud_asset_operations",
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    reason: asOptionalString(source.reason),
    request: source.request ? asJsonObject(source.request) : undefined,
    request_preview: source.request_preview ? asJsonObject(source.request_preview) : undefined,
    intent_rewrite: source.intent_rewrite ? mapIntentRewritePayload(source.intent_rewrite) : undefined,
    plan_review_gate: source.plan_review_gate ? asJsonObject(source.plan_review_gate) : undefined,
    plan_review_session: source.plan_review_session ? asJsonObject(source.plan_review_session) : undefined,
    organization_execution_profile: source.organization_execution_profile
      ? asJsonObject(source.organization_execution_profile)
      : undefined,
    asset_reuse_plan: source.asset_reuse_plan ? asJsonObject(source.asset_reuse_plan) : undefined,
    ingress_normalization: source.ingress_normalization ? asJsonObject(source.ingress_normalization) : undefined,
    planning: source.planning ? asJsonObject(source.planning) : undefined,
    dispatch_matching_normalization: source.dispatch_matching_normalization
      ? asJsonObject(source.dispatch_matching_normalization)
      : undefined,
    dispatch_preview: source.dispatch_preview ? asJsonObject(source.dispatch_preview) : undefined,
    lane_preview: source.lane_preview ? asJsonObject(source.lane_preview) : undefined,
    generation_watermarks: source.generation_watermarks ? asJsonObject(source.generation_watermarks) : undefined,
    cloud_asset_operations: {
      ...(cloudAssetOperations as JsonObject),
      count: asOptionalNumber(cloudAssetOperations.count),
      items: asArray(cloudAssetOperations.items).map((item) => {
        const entry = asObject(item, "CloudAssetOperationItem");
        return {
          ...(entry as JsonObject),
          ledger_id: asOptionalNumber(entry.ledger_id),
          operation_type: asOptionalString(entry.operation_type),
          bundle_kind: asOptionalString(entry.bundle_kind),
          bundle_id: asOptionalString(entry.bundle_id),
          sync_run_id: asOptionalString(entry.sync_run_id),
          status: asOptionalString(entry.status),
          manifest_path: asOptionalString(entry.manifest_path),
          target_runtime_dir: asOptionalString(entry.target_runtime_dir),
          target_db_path: asOptionalString(entry.target_db_path),
          scoped_companies: asArray(entry.scoped_companies)
            .map((value) => asString(value))
            .filter(Boolean),
          scoped_snapshot_id: asOptionalString(entry.scoped_snapshot_id),
          summary: entry.summary ? asJsonObject(entry.summary) : undefined,
          metadata: entry.metadata ? asJsonObject(entry.metadata) : undefined,
          created_at: asOptionalString(entry.created_at),
          updated_at: asOptionalString(entry.updated_at),
        };
      }),
    },
    timings_ms: source.timings_ms ? asJsonObject(source.timings_ms) : undefined,
  };
}

export function mapTargetCandidatePublicWebSearchState(
  payload: unknown,
): TargetCandidatePublicWebSearchState {
  const source = asObject(payload, "TargetCandidatePublicWebSearchState");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status) ?? "",
    batches: asArray(source.batches).map(mapTargetCandidatePublicWebBatch),
    runs: asArray(source.runs).map(mapTargetCandidatePublicWebRun),
  };
}

export function mapTargetCandidatePublicWebStartResponse(
  payload: unknown,
): TargetCandidatePublicWebStartResponse {
  const source = asObject(payload, "TargetCandidatePublicWebStartResponse");
  const state = mapTargetCandidatePublicWebSearchState(source);
  return {
    ...state,
    batch: source.batch ? mapTargetCandidatePublicWebBatch(source.batch) : null,
    summary: source.summary ? asJsonObject(source.summary) : {},
    worker_summary: source.worker_summary ? asJsonObject(source.worker_summary) : {},
    job: source.job ? asJsonObject(source.job) : {},
  };
}

export function mapTargetCandidatePublicWebDetailResponse(
  payload: unknown,
): TargetCandidatePublicWebDetailResponse {
  const source = asObject(payload, "TargetCandidatePublicWebDetailResponse");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status) ?? "",
    record_id: asOptionalString(source.record_id),
    target_candidate: source.target_candidate ? asJsonObject(source.target_candidate) : null,
    latest_run: source.latest_run ? asJsonObject(source.latest_run) : null,
    person_asset: source.person_asset ? asJsonObject(source.person_asset) : null,
    signals: asArray(source.signals).map(mapTargetCandidatePublicWebSignal),
    email_candidates: asArray(source.email_candidates).map(mapTargetCandidatePublicWebSignal),
    profile_links: asArray(source.profile_links).map(mapTargetCandidatePublicWebSignal),
    grouped_signals: source.grouped_signals ? asJsonObject(source.grouped_signals) : {},
    evidence_links: asArray(source.evidence_links).map(mapTargetCandidatePublicWebEvidenceLink),
    promotions: asArray(source.promotions).map(mapTargetCandidatePublicWebPromotion),
    promotion_summary: source.promotion_summary ? asJsonObject(source.promotion_summary) : {},
    raw_asset_policy: source.raw_asset_policy ? asJsonObject(source.raw_asset_policy) : {},
  };
}

export function mapTargetCandidatePublicWebPromotionResponse(
  payload: unknown,
): TargetCandidatePublicWebPromotionResponse {
  const source = asObject(payload, "TargetCandidatePublicWebPromotionResponse");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status) ?? "",
    record_id: asOptionalString(source.record_id),
    signal: source.signal ? mapTargetCandidatePublicWebSignal(source.signal) : undefined,
    promotion: source.promotion ? mapTargetCandidatePublicWebPromotion(source.promotion) : undefined,
    target_candidate: source.target_candidate ? asJsonObject(source.target_candidate) : undefined,
    detail: source.detail ? mapTargetCandidatePublicWebDetailResponse(source.detail) : null,
    reason: asOptionalString(source.reason),
  };
}

export function mapWorkflowCommandControlPolicy(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandControlPolicy {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "WorkflowCommandControlPolicy",
    false,
    traversal,
    depth,
  );
  return {
    ...(source as JsonObject),
    schema_version: asOptionalString(source.schema_version),
    command_type: asOptionalString(source.command_type),
    owner: asOptionalString(source.owner),
    generic_control_contract: asOptionalString(source.generic_control_contract),
    provider_after_start_control_contract: asOptionalString(
      source.provider_after_start_control_contract,
    ),
    provider_after_start_control_status: asOptionalString(
      source.provider_after_start_control_status,
    ),
    provider_after_start_control_mode: asOptionalString(source.provider_after_start_control_mode),
    provider_after_start_control_owner: asOptionalString(
      source.provider_after_start_control_owner,
    ),
    provider_after_start_control_blocked_reason: asOptionalString(
      source.provider_after_start_control_blocked_reason,
    ),
    provider_after_start_control_upgrade_requirements: asSparseOptionalStringArray(
      source.provider_after_start_control_upgrade_requirements,
    ),
    module_state_mutated_on_provider_after_start_control: asOptionalBoolean(
      source.module_state_mutated_on_provider_after_start_control,
    ),
    running_control_category: asOptionalString(source.running_control_category),
    running_control_categories: asSparseOptionalStringArray(source.running_control_categories),
    running_control_maturity: asOptionalString(source.running_control_maturity),
    running_control_gap_status: asOptionalString(source.running_control_gap_status),
    running_control_surface: asOptionalString(source.running_control_surface),
    generic_cancel_statuses: asSparseOptionalStringArray(source.generic_cancel_statuses),
    generic_retry_statuses: asSparseOptionalStringArray(source.generic_retry_statuses),
    generic_resume_statuses: asSparseOptionalStringArray(source.generic_resume_statuses),
    running_cancel_supported: asOptionalBoolean(source.running_cancel_supported),
    running_cancel_statuses: asSparseOptionalStringArray(source.running_cancel_statuses),
    running_cancel_owner: asOptionalString(source.running_cancel_owner),
    running_cancel_delegate: asOptionalString(source.running_cancel_delegate),
    running_cancel_prerequisites: asSparseOptionalStringArray(source.running_cancel_prerequisites),
    running_cancel_blocked_reason: asOptionalString(source.running_cancel_blocked_reason),
    running_cancel_upgrade_requirements: asSparseOptionalStringArray(
      source.running_cancel_upgrade_requirements,
    ),
    running_cancel_contract: asOptionalString(source.running_cancel_contract),
    unsupported_running_cancel_reason: asOptionalString(source.unsupported_running_cancel_reason),
    module_state_mutated_on_running_cancel: asOptionalBoolean(source.module_state_mutated_on_running_cancel),
    running_resume_supported: asOptionalBoolean(source.running_resume_supported),
    running_resume_statuses: asSparseOptionalStringArray(source.running_resume_statuses),
    running_resume_owner: asOptionalString(source.running_resume_owner),
    running_resume_delegate: asOptionalString(source.running_resume_delegate),
    running_resume_prerequisites: asSparseOptionalStringArray(source.running_resume_prerequisites),
    running_resume_blocked_reason: asOptionalString(source.running_resume_blocked_reason),
    running_resume_upgrade_requirements: asSparseOptionalStringArray(
      source.running_resume_upgrade_requirements,
    ),
    running_resume_contract: asOptionalString(source.running_resume_contract),
    unsupported_running_resume_reason: asOptionalString(source.unsupported_running_resume_reason),
    module_state_mutated_on_running_resume: asOptionalBoolean(source.module_state_mutated_on_running_resume),
    control_source_of_truth: asOptionalString(source.control_source_of_truth),
    agent_callable_surface: asOptionalString(source.agent_callable_surface),
    fallback_status: asOptionalString(source.fallback_status),
  };
}

export function mapWorkflowCommandControlState(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandControlState {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "WorkflowCommandControlState",
    false,
    traversal,
    depth,
  );
  return {
    ...(source as JsonObject),
    schema_version: asOptionalString(source.schema_version),
    command_type: asOptionalString(source.command_type),
    owner: asOptionalString(source.owner),
    command_status: asOptionalString(source.command_status),
    can_cancel: asOptionalBoolean(source.can_cancel),
    can_retry: asOptionalBoolean(source.can_retry),
    can_resume: asOptionalBoolean(source.can_resume),
    cancel_mode: asOptionalString(source.cancel_mode),
    retry_mode: asOptionalString(source.retry_mode),
    resume_mode: asOptionalString(source.resume_mode),
    allowed_actions: asSparseOptionalStringArray(source.allowed_actions),
    disabled_reasons: asOptionalWorkflowCommandPublicMirrorObject(
      source.disabled_reasons,
      traversal,
      depth + 1,
    ),
    running_cancel_supported: asOptionalBoolean(source.running_cancel_supported),
    running_cancel_delegate: asOptionalString(source.running_cancel_delegate),
    running_cancel_prerequisites: asSparseOptionalStringArray(source.running_cancel_prerequisites),
    running_resume_supported: asOptionalBoolean(source.running_resume_supported),
    running_resume_delegate: asOptionalString(source.running_resume_delegate),
    running_resume_prerequisites: asSparseOptionalStringArray(source.running_resume_prerequisites),
    module_state_mutated_on_cancel: asOptionalBoolean(source.module_state_mutated_on_cancel),
    module_state_mutated_on_resume: asOptionalBoolean(source.module_state_mutated_on_resume),
    control_source_of_truth: asOptionalString(source.control_source_of_truth),
    policy_source_of_truth: asOptionalString(source.policy_source_of_truth),
    fallback_status: asOptionalString(source.fallback_status),
  };
}

export function mapWorkflowCommandActivitySpinePolicy(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandActivitySpinePolicy {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "WorkflowCommandActivitySpinePolicy",
    false,
    traversal,
    depth,
  );
  return {
    ...(source as JsonObject),
    schema_version: asOptionalString(source.schema_version),
    command_type: asOptionalString(source.command_type),
    owner: asOptionalString(source.owner),
    requirement: asOptionalString(source.requirement),
    must_write_activity_run: asOptionalBoolean(source.must_write_activity_run),
    must_write_activity_attempt: asOptionalBoolean(source.must_write_activity_attempt),
    must_write_entity_delta: asOptionalBoolean(source.must_write_entity_delta),
    downstream_activity_required: asOptionalBoolean(source.downstream_activity_required),
    agent_callable: asOptionalBoolean(source.agent_callable),
    activity_table: asOptionalString(source.activity_table),
    attempt_table: asOptionalString(source.attempt_table),
    entity_delta_table: asOptionalString(source.entity_delta_table),
    source_of_truth: asOptionalString(source.source_of_truth),
    agent_callable_surface: asOptionalString(source.agent_callable_surface),
    fallback_status: asOptionalString(source.fallback_status),
    migration_status: asOptionalString(source.migration_status),
    deletion_condition: asOptionalString(source.deletion_condition),
  };
}

export function mapWorkflowCommandDisplayContract(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandDisplayContract {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "WorkflowCommandDisplayContract",
    false,
    traversal,
    depth,
  );
  return {
    ...(source as JsonObject),
    schema_version: asOptionalString(source.schema_version),
    command_type: asOptionalString(source.command_type),
    owner: asOptionalString(source.owner),
    display_label: asOptionalString(source.display_label),
    display_category: asOptionalString(source.display_category),
    description: asOptionalString(source.description),
    source_of_truth: asOptionalString(source.source_of_truth),
    fallback_status: asOptionalString(source.fallback_status),
  };
}

export function mapOperationActionDisplayContract(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): OperationActionDisplayContract {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "OperationActionDisplayContract",
    false,
    traversal,
    depth,
  );
  const mapped: OperationActionDisplayContract = {
    ...(source as JsonObject),
    schema_version: asOptionalString(source.schema_version),
    action_type: asOptionalString(source.action_type),
    owner_module: asOptionalString(source.owner_module),
    operation_type: asOptionalString(source.operation_type),
    display_label: asOptionalString(source.display_label),
    display_category: asOptionalString(source.display_category),
    description: asOptionalString(source.description),
    source_of_truth: asOptionalString(source.source_of_truth),
    fallback_status: asOptionalString(source.fallback_status),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapWorkflowCommandContract(payload: unknown): WorkflowCommandContract {
  const source = asWorkflowCommandPublicMirrorSource(payload, "WorkflowCommandContract");
  return {
    ...(source as JsonObject),
    command_type: asString(source.command_type),
    owner: asOptionalString(source.owner),
    agent_exposure_status: asOptionalString(source.agent_exposure_status),
    agent_exposure_gate: asOptionalString(source.agent_exposure_gate),
    stage_id: asOptionalString(source.stage_id),
    readiness_effect: asOptionalString(source.readiness_effect),
    display_contract: mapOptionalPlainWorkflowPublicObject(
      source.display_contract,
      mapWorkflowCommandDisplayContract,
    ),
    control_policy: mapOptionalPlainWorkflowPublicObject(
      source.control_policy,
      mapWorkflowCommandControlPolicy,
    ),
    control_state: mapOptionalPlainWorkflowPublicObject(
      source.control_state,
      mapWorkflowCommandControlState,
    ),
    activity_spine_policy: mapOptionalPlainWorkflowPublicObject(
      source.activity_spine_policy,
      mapWorkflowCommandActivitySpinePolicy,
    ),
  };
}

export function mapWorkflowCommandControlSummary(
  payload: unknown,
): WorkflowCommandControlSummary {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "WorkflowCommandControlSummary",
  );
  return {
    ...(source as JsonObject),
    source_of_truth: asOptionalString(source.source_of_truth),
    fallback_status: asOptionalString(source.fallback_status),
    command_count: asOptionalNumber(source.command_count),
    running_control_maturity_counts: asSparseRecordOfNumber(
      source.running_control_maturity_counts,
    ),
    running_control_gap_status_counts: asSparseRecordOfNumber(
      source.running_control_gap_status_counts,
    ),
    running_control_category_counts: asSparseRecordOfNumber(
      source.running_control_category_counts,
    ),
    has_fail_closed_running_controls: asOptionalBoolean(
      source.has_fail_closed_running_controls,
    ),
    has_owner_specific_running_controls: asOptionalBoolean(
      source.has_owner_specific_running_controls,
    ),
    default_workflow_command_type: asOptionalString(source.default_workflow_command_type),
    default_running_control_maturity: asOptionalString(
      source.default_running_control_maturity,
    ),
    default_running_control_gap_status: asOptionalString(
      source.default_running_control_gap_status,
    ),
    agent_ui_guidance: asOptionalString(source.agent_ui_guidance),
  };
}

export function mapOperationActionRegistryEntry(payload: unknown): OperationActionRegistryEntry {
  const source = asWorkflowCommandPublicMirrorSource(payload, "OperationActionRegistryEntry");
  return {
    ...(source as JsonObject),
    owner_module: asOptionalString(source.owner_module),
    operation_type: asOptionalString(source.operation_type),
    approval_policy: asOptionalString(source.approval_policy),
    budget_required: asOptionalBoolean(source.budget_required),
    description: asOptionalString(source.description),
    display_contract: mapOptionalPlainWorkflowPublicObject(
      source.display_contract,
      mapOperationActionDisplayContract,
    ),
    allowed_workflow_command_types: asSparseOptionalStringArray(
      source.allowed_workflow_command_types,
    ),
    default_workflow_command_type: asOptionalString(source.default_workflow_command_type),
    workflow_command_exposure_gate: asOptionalString(source.workflow_command_exposure_gate),
    workflow_command_exposure_status: asOptionalString(source.workflow_command_exposure_status),
    allowed_workflow_command_contracts: mapSparsePlainWorkflowPublicObjectsSafely(
      source.allowed_workflow_command_contracts,
      mapWorkflowCommandContract,
    ),
    workflow_command_control_summary: mapOptionalPlainWorkflowPublicObject(
      source.workflow_command_control_summary,
      mapWorkflowCommandControlSummary,
    ),
    default_workflow_command_contract: mapOptionalPlainWorkflowPublicObjectSafely(
      source.default_workflow_command_contract,
      mapWorkflowCommandContract,
    ),
  };
}

export function mapOperationActionRegistryResponse(payload: unknown): OperationActionRegistryResponse {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "OperationActionRegistryResponse",
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    action_registry: asRecordOfMappedObject(source.action_registry, mapOperationActionRegistryEntry),
  };
}

export function mapOperationEventRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): OperationEventRecord {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "OperationEventRecord",
    false,
    traversal,
    depth,
  );
  const mapped: OperationEventRecord = {
    ...(source as JsonObject),
    event_id: asOptionalString(source.event_id),
    workspace_id: asOptionalString(source.workspace_id),
    event_stream_id: asOptionalString(source.event_stream_id),
    operation_run_id: asOptionalString(source.operation_run_id),
    action_id: asOptionalString(source.action_id),
    event_family: asOptionalString(source.event_family),
    event_type: asOptionalString(source.event_type),
    sequence_number: asOptionalNumber(source.sequence_number),
    actor: asOptionalString(source.actor),
    source: asOptionalString(source.source),
    payload: asOptionalWorkflowCommandPublicMirrorObject(source.payload, traversal, depth + 1),
    occurred_at: asOptionalString(source.occurred_at),
    recorded_at: asOptionalString(source.recorded_at),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapOperationActionRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): OperationActionRecord {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "OperationActionRecord",
    false,
    traversal,
    depth,
  );
  const mapped: OperationActionRecord = {
    ...(source as JsonObject),
    action_id: asOptionalString(source.action_id),
    workspace_id: asOptionalString(source.workspace_id),
    conversation_id: asOptionalString(source.conversation_id),
    action_type: asOptionalString(source.action_type),
    owner_module: asOptionalString(source.owner_module),
    operation_type: asOptionalString(source.operation_type),
    display_contract: mapOptionalPlainWorkflowPublicObject(
      source.display_contract,
      (value) => mapOperationActionDisplayContract(value, traversal, depth + 1),
    ),
    target_ref: asOptionalWorkflowCommandPublicMirrorObject(source.target_ref, traversal, depth + 1),
    input: asOptionalWorkflowCommandPublicMirrorObject(source.input, traversal, depth + 1),
    approval_status: asOptionalString(source.approval_status),
    approval_policy: asOptionalString(source.approval_policy),
    budget: asOptionalWorkflowCommandPublicMirrorObject(source.budget, traversal, depth + 1),
    status: asOptionalString(source.status),
    result_ref: asOptionalWorkflowCommandPublicMirrorObject(source.result_ref, traversal, depth + 1),
    metadata: asOptionalWorkflowCommandPublicMirrorObject(source.metadata, traversal, depth + 1),
    request_schema_version: asOptionalString(source.request_schema_version),
    request_schema_digest: asOptionalString(source.request_schema_digest),
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapOperationRunRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): OperationRunRecord {
  const { source, canonical } = captureWorkflowPublicEnvelope(
    payload,
    "OperationRunRecord",
    ["status_summary"],
    traversal,
    depth,
  );
  const mapped: OperationRunRecord = {
    ...(source as JsonObject),
    operation_run_id: asOptionalString(source.operation_run_id),
    workspace_id: asOptionalString(source.workspace_id),
    action_id: asOptionalString(source.action_id),
    owner_module: asOptionalString(source.owner_module),
    operation_type: asOptionalString(source.operation_type),
    display_contract: mapOptionalPlainWorkflowPublicObject(
      source.display_contract,
      (value) => mapOperationActionDisplayContract(value, traversal, depth + 1),
    ),
    status: asOptionalString(source.status),
    progress: asOptionalWorkflowCommandPublicMirrorObject(source.progress, traversal, depth + 1),
    workflow_ref: asOptionalWorkflowCommandPublicMirrorObject(source.workflow_ref, traversal, depth + 1),
    cost_budget: asOptionalWorkflowCommandPublicMirrorObject(source.cost_budget, traversal, depth + 1),
    result_ref: asOptionalWorkflowCommandPublicMirrorObject(source.result_ref, traversal, depth + 1),
    metadata: asOptionalWorkflowCommandPublicMirrorObject(source.metadata, traversal, depth + 1),
    request_schema_version: asOptionalString(source.request_schema_version),
    request_schema_digest: asOptionalString(source.request_schema_digest),
    control_state: mapOptionalPlainWorkflowPublicObject(
      source.control_state,
      (value) => mapOperationRunControlState(value, traversal, depth + 1),
    ),
    status_summary: mapCapturedPlainWorkflowPublicObject(
      canonical.status_summary,
      mapOperationRunStatusSummary,
      traversal,
      depth + 1,
    ),
    started_at: asOptionalString(source.started_at),
    completed_at: asOptionalString(source.completed_at),
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapOperationRunControlState(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): OperationRunControlState {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "OperationRunControlState",
    false,
    traversal,
    depth,
  );
  const mapped: OperationRunControlState = {
    ...(source as JsonObject),
    operation_status: asOptionalString(source.operation_status),
    action_status: asOptionalString(source.action_status),
    operation_phase: asOptionalString(source.operation_phase),
    can_dispatch: asOptionalBoolean(source.can_dispatch),
    can_cancel: asOptionalBoolean(source.can_cancel),
    can_retry: asOptionalBoolean(source.can_retry),
    can_resume: asOptionalBoolean(source.can_resume),
    allowed_actions: asSparseOptionalStringArray(source.allowed_actions),
    disabled_reasons: asOptionalWorkflowCommandPublicMirrorObject(
      source.disabled_reasons,
      traversal,
      depth + 1,
    ),
    control_source_of_truth: asOptionalString(source.control_source_of_truth),
    fallback_status: asOptionalString(source.fallback_status),
    module_state_mutated_on_control: asOptionalBoolean(source.module_state_mutated_on_control),
    schema_version: asOptionalString(source.schema_version),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapOperationRunStatusSummary(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): OperationRunStatusSummary {
  const { source, canonical } = captureWorkflowPublicEnvelope(
    payload ?? {},
    "OperationRunStatusSummary",
    ["latest_workflow_command"],
    traversal,
    depth,
  );
  const mapped: OperationRunStatusSummary = {
    ...(source as JsonObject),
    source: asOptionalString(source.source),
    fallback_status: asOptionalString(source.fallback_status),
    fallback_used: asOptionalBoolean(source.fallback_used),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
    operation_status: asOptionalString(source.operation_status),
    operation_phase: asOptionalString(source.operation_phase),
    workflow_command_count: asOptionalNumber(source.workflow_command_count),
    operation_event_count: asOptionalNumber(source.operation_event_count),
    command_status_counts: asSparseRecordOfNumber(source.command_status_counts),
    latest_event_type: asOptionalString(source.latest_event_type),
    latest_event: mapOptionalPlainWorkflowPublicObject(
      source.latest_event,
      (value) => mapOperationEventRecord(value, traversal, depth + 1),
    ),
    latest_workflow_command: mapCapturedPlainWorkflowPublicObject(
      canonical.latest_workflow_command,
      mapWorkflowCommandRecord,
      traversal,
      depth + 1,
    ),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapOperationActionListResponse(payload: unknown): OperationActionListResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "OperationActionListResponse",
    ["actions"],
  );
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    contract: asOptionalString(source.contract),
    actions: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.actions,
      mapOperationActionRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapOperationActionDetailResponse(payload: unknown): OperationActionDetailResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "OperationActionDetailResponse",
    ["action", "operation_run", "events"],
  );
  return {
    ...(source as JsonObject),
    status: asAllowedPublicResponseStatus(
      source.status,
      OPERATION_ACTION_DETAIL_SUCCESS_STATUSES,
      "OperationActionDetailResponse",
    ),
    idempotent_replay: asOptionalBoolean(source.idempotent_replay),
    contract: asOptionalString(source.contract),
    action: mapCapturedPlainWorkflowPublicObject(
      canonical.action,
      mapOperationActionRecord,
      traversal,
      depth + 1,
    ),
    operation_run: mapCapturedPlainWorkflowPublicObject(
      canonical.operation_run,
      mapOperationRunRecord,
      traversal,
      depth + 1,
    ),
    events: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.events,
      mapOperationEventRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapOperationActionSubmitResponse(payload: unknown): OperationActionSubmitResponse {
  const response = mapOperationActionDetailResponse(payload);
  if (response.idempotent_replay === true) {
    const status = asAllowedPublicResponseStatus(
      response.status,
      OPERATION_ACTION_SUBMIT_REPLAY_OUTCOMES,
      "Operation action submit replay",
    );
    return { ...response, status, idempotent_replay: true };
  }
  if (response.idempotent_replay === false) {
    const status = asAllowedPublicResponseStatus(
      response.status,
      OPERATION_ACTION_SUBMIT_FRESH_OUTCOMES,
      "Operation action submit fresh",
    );
    return { ...response, status, idempotent_replay: false };
  }
  throw new Error("Operation action submit idempotent_replay must be boolean");
}

export function mapOperationRunListResponse(payload: unknown): OperationRunListResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "OperationRunListResponse",
    ["operation_runs"],
  );
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    contract: asOptionalString(source.contract),
    operation_runs: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.operation_runs,
      mapOperationRunRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapOperationRunDetailResponse(payload: unknown): OperationRunDetailResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "OperationRunDetailResponse",
    ["operation_run", "events"],
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    operation_run: mapCapturedPlainWorkflowPublicObject(
      canonical.operation_run,
      mapOperationRunRecord,
      traversal,
      depth + 1,
    ),
    events: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.events,
      mapOperationEventRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapOperationRunProvenanceResponse(payload: unknown): OperationRunProvenanceResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "OperationRunProvenanceResponse",
    [
      "action",
      "operation_run",
      "action_events",
      "operation_events",
      "event_timeline",
      "workflow_commands",
    ],
  );
  return {
    ...(source as JsonObject),
    status: asAllowedPublicResponseStatus(
      source.status,
      OPERATION_RUN_PROVENANCE_SUCCESS_STATUSES,
      "OperationRunProvenanceResponse",
    ),
    contract: asOptionalString(source.contract),
    action: mapCapturedPlainWorkflowPublicObject(
      canonical.action,
      mapOperationActionRecord,
      traversal,
      depth + 1,
    ),
    operation_run: mapCapturedPlainWorkflowPublicObject(
      canonical.operation_run,
      mapOperationRunRecord,
      traversal,
      depth + 1,
    ),
    action_events: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.action_events,
      mapOperationEventRecord,
      traversal,
      depth + 1,
    ) ?? [],
    operation_events: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.operation_events,
      mapOperationEventRecord,
      traversal,
      depth + 1,
    ) ?? [],
    event_timeline: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.event_timeline,
      mapOperationEventRecord,
      traversal,
      depth + 1,
    ) ?? [],
    workflow_commands: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_commands,
      mapWorkflowCommandRecord,
      traversal,
      depth + 1,
    ) ?? [],
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
  };
}

export function mapOperationRunControlResponse(payload: unknown): OperationRunControlResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "OperationRunControlResponse",
    [
      "action",
      "operation_run",
      "parent_operation_run",
      "display_contract",
      "control_state",
      "workflow_command",
      "events",
    ],
  );
  return {
    ...(source as JsonObject),
    status: asAllowedPublicResponseStatus(
      source.status,
      [
        ...OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.cancel,
        ...OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.retry,
        ...OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.resume,
        ...OPERATION_RUN_CONTROL_APPLIED_OUTCOMES.dispatch,
      ] as const,
      "OperationRunControlResponse",
    ),
    reason: asOptionalString(source.reason),
    contract: asOptionalString(source.contract),
    action: mapCapturedPlainWorkflowPublicObject(
      canonical.action,
      mapOperationActionRecord,
      traversal,
      depth + 1,
    ),
    operation_run: mapCapturedPlainWorkflowPublicObject(
      canonical.operation_run,
      mapOperationRunRecord,
      traversal,
      depth + 1,
    ),
    parent_operation_run: mapCapturedPlainWorkflowPublicObject(
      canonical.parent_operation_run,
      mapOperationRunRecord,
      traversal,
      depth + 1,
    ),
    display_contract: mapCapturedPlainWorkflowPublicObject(
      canonical.display_contract,
      mapOperationActionDisplayContract,
      traversal,
      depth + 1,
    ),
    control_state: mapCapturedPlainWorkflowPublicObject(
      canonical.control_state,
      mapOperationRunControlState,
      traversal,
      depth + 1,
    ),
    workflow_command: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_command,
      mapWorkflowCommandRecord,
      traversal,
      depth + 1,
    ),
    events: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.events,
      mapOperationEventRecord,
      traversal,
      depth + 1,
    ) ?? [],
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
  };
}

export function mapWorkflowCommandRegistryResponse(payload: unknown): WorkflowCommandRegistryResponse {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "WorkflowCommandRegistryResponse",
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    command_registry: asRecordOfMappedObject(source.command_registry, mapWorkflowCommandContract),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
  };
}

export function mapWorkflowCommandExecutionSummary(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandExecutionSummary {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "WorkflowCommandExecutionSummary",
    false,
    traversal,
    depth,
  );
  return {
    ...(source as JsonObject),
    source: asOptionalString(source.source),
    fallback_status: asOptionalString(source.fallback_status),
    fallback_used: asOptionalBoolean(source.fallback_used),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
    activity_count: asOptionalNumber(source.activity_count),
    attempt_count: asOptionalNumber(source.attempt_count),
    entity_delta_count: asOptionalNumber(source.entity_delta_count),
    activity_status_counts: asSparseRecordOfNumber(source.activity_status_counts),
    attempt_status_counts: asSparseRecordOfNumber(source.attempt_status_counts),
    entity_delta_status_counts: asSparseRecordOfNumber(source.entity_delta_status_counts),
    entity_delta_kind_counts: asSparseRecordOfNumber(source.entity_delta_kind_counts),
    latest_effect_status: asOptionalString(source.latest_effect_status),
    latest_activity: mapOptionalPlainWorkflowPublicObject(
      source.latest_activity,
      (value) => mapWorkflowActivityRecord(value, traversal, depth + 1),
    ),
    latest_attempt: mapOptionalPlainWorkflowPublicObject(
      source.latest_attempt,
      (value) => mapWorkflowActivityAttemptRecord(value, traversal, depth + 1),
    ),
    latest_entity_delta: mapOptionalPlainWorkflowPublicObject(
      source.latest_entity_delta,
      (value) => mapWorkflowEntityDeltaRecord(value, traversal, depth + 1),
    ),
    sample_limit: asOptionalNumber(source.sample_limit),
    sample_truncated: asOptionalBoolean(source.sample_truncated),
  };
}

export function mapWorkflowCommandRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandRecord {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "WorkflowCommandRecord",
    true,
    traversal,
    depth,
  );
  const mapped: WorkflowCommandRecord = {
    command_id: asOptionalString(source.command_id),
    workflow_run_id: asOptionalString(source.workflow_run_id),
    operation_id: asOptionalString(source.operation_id),
    command_type: asOptionalString(source.command_type),
    owner: asOptionalString(source.owner),
    stage_id: asOptionalString(source.stage_id),
    causal_group_id: asOptionalString(source.causal_group_id),
    parent_command_id: asOptionalString(source.parent_command_id),
    source_event_id: asOptionalString(source.source_event_id),
    source_event_type: asOptionalString(source.source_event_type),
    input_artifact_refs: asOptionalWorkflowCommandStringArray(source.input_artifact_refs),
    output_artifact_refs: asOptionalWorkflowCommandStringArray(source.output_artifact_refs),
    produced_entity_counts: asOptionalWorkflowCommandPublicMirrorObject(
      source.produced_entity_counts,
      traversal,
      depth + 1,
    ),
    no_op_reason: asOptionalString(source.no_op_reason),
    readiness_effect: asOptionalString(source.readiness_effect),
    downstream_command_ids: asOptionalWorkflowCommandStringArray(source.downstream_command_ids),
    causality_schema_version: asOptionalString(source.causality_schema_version),
    status: asOptionalString(source.status),
    idempotency_key: asOptionalString(source.idempotency_key),
    payload: asOptionalWorkflowCommandPublicMirrorObject(source.payload, traversal, depth + 1),
    artifact_refs: asOptionalWorkflowCommandStringArray(source.artifact_refs),
    not_before_at: asOptionalString(source.not_before_at),
    attempt: asOptionalNumber(source.attempt),
    max_attempts: asOptionalNumber(source.max_attempts),
    retry_policy: asOptionalWorkflowCommandPublicMirrorObject(
      source.retry_policy,
      traversal,
      depth + 1,
    ),
    lease_owner: asOptionalString(source.lease_owner),
    lease_expires_at: asOptionalString(source.lease_expires_at),
    heartbeat_at: asOptionalString(source.heartbeat_at),
    last_error: asOptionalString(source.last_error),
    result: asOptionalWorkflowCommandPublicMirrorObject(source.result, traversal, depth + 1),
    schema_version: asOptionalString(source.schema_version),
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
    claim_generation: asOptionalNonnegativeSafeInteger(source.claim_generation),
    control_epoch: asOptionalNonnegativeSafeInteger(source.control_epoch),
    agent_exposure_gate: asOptionalString(source.agent_exposure_gate),
    agent_exposure_status: asOptionalString(source.agent_exposure_status),
    display_contract: mapOptionalPlainWorkflowPublicObject(
      source.display_contract,
      (value) => mapWorkflowCommandDisplayContract(value, traversal, depth + 1),
    ),
    control_policy: mapOptionalPlainWorkflowPublicObject(
      source.control_policy,
      (value) => mapWorkflowCommandControlPolicy(value, traversal, depth + 1),
    ),
    control_state: mapOptionalPlainWorkflowPublicObject(
      source.control_state,
      (value) => mapWorkflowCommandControlState(value, traversal, depth + 1),
    ),
    activity_spine_policy: mapOptionalPlainWorkflowPublicObject(
      source.activity_spine_policy,
      (value) => mapWorkflowCommandActivitySpinePolicy(value, traversal, depth + 1),
    ),
    execution_summary: mapOptionalPlainWorkflowPublicObject(
      source.execution_summary,
      (value) => mapWorkflowCommandExecutionSummary(value, traversal, depth + 1),
    ),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal, true);
  return mapped;
}

export function mapWorkflowCommandOperationSync(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandOperationSync | undefined {
  const projection = projectWorkflowCommandOperationSync(payload, traversal, depth);
  return projection.payloadWasExactEmpty || Object.keys(projection.value).length > 0
    ? projection.value
    : undefined;
}

interface WorkflowCommandOperationSyncProjection {
  readonly value: WorkflowCommandOperationSync;
  readonly payloadWasExactEmpty: boolean;
}

function projectWorkflowCommandOperationSync(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal,
  depth: number,
): WorkflowCommandOperationSyncProjection {
  const { source, canonical, payloadWasExactEmpty } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowCommandOperationSync",
    ["operation_run", "event", "workflow_command"],
    traversal,
    depth,
  );
  const mapped: WorkflowCommandOperationSync = {
    status: asOptionalString(source.status),
    reason: asOptionalString(source.reason),
    operation_run_id: asOptionalString(source.operation_run_id),
    operation_status: asOptionalString(source.operation_status),
    control_action: asOptionalString(source.control_action),
    command_status: asOptionalString(source.command_status),
    operation_run: mapCapturedPlainWorkflowPublicObject(
      canonical.operation_run,
      mapOperationRunRecord,
      traversal,
      depth + 1,
    ),
    event: mapCapturedPlainWorkflowPublicObject(
      canonical.event,
      mapOperationEventRecord,
      traversal,
      depth + 1,
    ),
    workflow_command: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_command,
      mapWorkflowCommandGenericPublicCarrierRecord,
      traversal,
      depth + 1,
    ),
  };
  for (const field of Object.keys(mapped) as Array<keyof WorkflowCommandOperationSync>) {
    if (mapped[field] === undefined) {
      delete mapped[field];
    }
  }
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return { value: mapped, payloadWasExactEmpty };
}

function mapWorkflowCommandOperationSyncIfMeaningful(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal,
  depth: number,
): WorkflowCommandOperationSync | undefined {
  const projection = projectWorkflowCommandOperationSync(payload, traversal, depth);
  return projection.payloadWasExactEmpty || Object.keys(projection.value).length > 0
    ? projection.value
    : undefined;
}

export function mapWorkflowCommandListResponse(payload: unknown): WorkflowCommandListResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowCommandListResponse",
    ["workflow_commands"],
  );
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    contract: asOptionalString(source.contract),
    workflow_commands: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_commands,
      mapWorkflowCommandRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapWorkflowCommandDetailResponse(payload: unknown): WorkflowCommandDetailResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowCommandDetailResponse",
    ["workflow_command"],
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    workflow_command: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_command,
      mapWorkflowCommandRecord,
      traversal,
      depth + 1,
    ),
  };
}

export function mapWorkflowCommandControlResponse(payload: unknown): WorkflowCommandControlResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowCommandControlResponse",
    [
      "workflow_command",
      "operation_sync",
      "workflow_activity",
      "workflow_activity_run",
      "workflow_activity_attempt",
      "workflow_entity_delta",
      "workflow_activity_runs",
      "workflow_activity_attempts",
      "workflow_entity_deltas",
      "display_contract",
      "control_policy",
      "control_state",
      "activity_spine_policy",
    ],
  );
  const publicSource = { ...(source as JsonObject) };
  for (const field of Object.keys(publicSource)) {
    if (
      isWorkflowActivityPublicCarrierField(field) &&
      !WORKFLOW_COMMAND_CONTROL_PUBLIC_ACTIVITY_CARRIER_FIELDS.has(field)
    ) {
      delete publicSource[field];
    }
  }
  const operationSync = mapCapturedPlainWorkflowPublicObject(
    canonical.operation_sync,
    mapWorkflowCommandOperationSyncIfMeaningful,
    traversal,
    depth + 1,
  );
  const mapped: WorkflowCommandControlResponse = {
    ...publicSource,
    status: asAllowedPublicResponseStatus(
      source.status,
      [
        ...WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES.cancel,
        ...WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES.retry,
        ...WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES.resume,
      ] as const,
      "WorkflowCommandControlResponse",
    ),
    reason: asOptionalString(source.reason),
    command_status: asOptionalString(source.command_status),
    workflow_command: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_command,
      mapWorkflowCommandGenericPublicCarrierRecord,
      traversal,
      depth + 1,
    ),
    operation_sync: operationSync,
    workflow_activity: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_activity,
      mapWorkflowActivityRecord,
      traversal,
      depth + 1,
    ),
    workflow_activity_run: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_activity_run,
      mapWorkflowActivityRecord,
      traversal,
      depth + 1,
    ),
    workflow_activity_attempt: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_activity_attempt,
      mapWorkflowActivityAttemptRecord,
      traversal,
      depth + 1,
    ),
    workflow_entity_delta: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_entity_delta,
      mapWorkflowEntityDeltaRecord,
      traversal,
      depth + 1,
    ),
    workflow_activity_runs: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_activity_runs,
      mapWorkflowActivityRecord,
      traversal,
      depth + 1,
    ),
    workflow_activity_attempts: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_activity_attempts,
      mapWorkflowActivityAttemptRecord,
      traversal,
      depth + 1,
    ),
    workflow_entity_deltas: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_entity_deltas,
      mapWorkflowEntityDeltaRecord,
      traversal,
      depth + 1,
    ),
    display_contract: mapCapturedPlainWorkflowPublicObject(
      canonical.display_contract,
      mapWorkflowCommandDisplayContract,
      traversal,
      depth + 1,
    ),
    control_policy: mapCapturedPlainWorkflowPublicObject(
      canonical.control_policy,
      mapWorkflowCommandControlPolicy,
      traversal,
      depth + 1,
    ),
    control_state: mapCapturedPlainWorkflowPublicObject(
      canonical.control_state,
      mapWorkflowCommandControlState,
      traversal,
      depth + 1,
    ),
    activity_spine_policy: mapCapturedPlainWorkflowPublicObject(
      canonical.activity_spine_policy,
      mapWorkflowCommandActivitySpinePolicy,
      traversal,
      depth + 1,
    ),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
    owner_specific_control: asOptionalBoolean(source.owner_specific_control),
    contract: asOptionalString(source.contract),
  };
  if (operationSync === undefined) {
    delete mapped.operation_sync;
  }
  return mapped;
}

export function mapWorkflowActivityControlTarget(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowActivityControlTarget {
  const source = asWorkflowCommandPublicMirrorSource(
    payload ?? {},
    "WorkflowActivityControlTarget",
    false,
    traversal,
    depth,
  );
  return {
    target_type: asOptionalString(source.target_type),
    command_id: asOptionalString(source.command_id),
    command_type: asOptionalString(source.command_type),
    owner: asOptionalString(source.owner),
    command_status: asOptionalString(source.command_status),
    display_contract: mapOptionalPlainWorkflowPublicObject(
      source.display_contract,
      (value) => mapWorkflowCommandDisplayContract(value, traversal, depth + 1),
    ),
    control_policy: mapOptionalPlainWorkflowPublicObject(
      source.control_policy,
      (value) => mapWorkflowCommandControlPolicy(value, traversal, depth + 1),
    ),
    control_state: mapOptionalPlainWorkflowPublicObject(
      source.control_state,
      (value) => mapWorkflowCommandControlState(value, traversal, depth + 1),
    ),
    activity_spine_policy: mapOptionalPlainWorkflowPublicObject(
      source.activity_spine_policy,
      (value) => mapWorkflowCommandActivitySpinePolicy(value, traversal, depth + 1),
    ),
    fallback_status: asOptionalString(source.fallback_status),
  };
}

export function mapWorkflowActivityRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowActivityRecord {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "WorkflowActivityRecord",
    false,
    traversal,
    depth,
  );
  const mapped: WorkflowActivityRecord = {
    activity_run_id: asOptionalString(source.activity_run_id),
    workspace_id: asOptionalString(source.workspace_id),
    workflow_run_id: asOptionalString(source.workflow_run_id),
    operation_run_id: asOptionalString(source.operation_run_id),
    acquisition_run_id: asOptionalString(source.acquisition_run_id),
    command_id: asOptionalString(source.command_id),
    parent_activity_run_id: asOptionalString(source.parent_activity_run_id),
    activity_type: asOptionalString(source.activity_type),
    owner: asOptionalString(source.owner),
    status: asOptionalString(source.status),
    phase: asOptionalString(source.phase),
    idempotency_key: asOptionalString(source.idempotency_key),
    provider_ref: asOptionalWorkflowCommandPublicMirrorObject(source.provider_ref, traversal, depth + 1),
    input: asOptionalWorkflowCommandPublicMirrorObject(source.input, traversal, depth + 1),
    output: asOptionalWorkflowCommandPublicMirrorObject(source.output, traversal, depth + 1),
    artifact_refs: asOptionalWorkflowCommandJsonArray(source.artifact_refs, traversal, depth + 1),
    entity_counts: asOptionalWorkflowCommandPublicMirrorObject(source.entity_counts, traversal, depth + 1),
    metadata: asOptionalWorkflowCommandPublicMirrorObject(source.metadata, traversal, depth + 1),
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
    mutation_contract: asOptionalString(source.mutation_contract),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
    control_target: mapOptionalPlainWorkflowPublicObject(
      source.control_target,
      (value) => mapWorkflowActivityControlTarget(value, traversal, depth + 1),
    ),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapWorkflowActivityAttemptRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowActivityAttemptRecord {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "WorkflowActivityAttemptRecord",
    false,
    traversal,
    depth,
  );
  const mapped: WorkflowActivityAttemptRecord = {
    attempt_id: asOptionalString(source.attempt_id),
    workspace_id: asOptionalString(source.workspace_id),
    activity_run_id: asOptionalString(source.activity_run_id),
    workflow_run_id: asOptionalString(source.workflow_run_id),
    command_id: asOptionalString(source.command_id),
    attempt_number: asOptionalNonnegativeSafeInteger(source.attempt_number),
    activity_type: asOptionalString(source.activity_type),
    owner: asOptionalString(source.owner),
    status: asOptionalString(source.status),
    provider: asOptionalString(source.provider),
    provider_request_ref: asOptionalString(source.provider_request_ref),
    provider_run_ref: asOptionalString(source.provider_run_ref),
    started_at: asOptionalString(source.started_at),
    completed_at: asOptionalString(source.completed_at),
    next_retry_at: asOptionalString(source.next_retry_at),
    rate_limit_ref: asOptionalWorkflowCommandPublicMirrorObject(source.rate_limit_ref, traversal, depth + 1),
    error: asOptionalWorkflowCommandPublicMirrorObject(source.error, traversal, depth + 1),
    input: asOptionalWorkflowCommandPublicMirrorObject(source.input, traversal, depth + 1),
    output: asOptionalWorkflowCommandPublicMirrorObject(source.output, traversal, depth + 1),
    artifact_refs: asOptionalWorkflowCommandJsonArray(source.artifact_refs, traversal, depth + 1),
    idempotency_key: asOptionalString(source.idempotency_key),
    metadata: asOptionalWorkflowCommandPublicMirrorObject(source.metadata, traversal, depth + 1),
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
    mutation_contract: asOptionalString(source.mutation_contract),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
    control_target: mapOptionalPlainWorkflowPublicObject(
      source.control_target,
      (value) => mapWorkflowActivityControlTarget(value, traversal, depth + 1),
    ),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapWorkflowEntityDeltaRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowEntityDeltaRecord {
  const source = asWorkflowCommandPublicMirrorSource(
    payload,
    "WorkflowEntityDeltaRecord",
    false,
    traversal,
    depth,
  );
  const mapped: WorkflowEntityDeltaRecord = {
    delta_id: asOptionalString(source.delta_id),
    workspace_id: asOptionalString(source.workspace_id),
    workflow_run_id: asOptionalString(source.workflow_run_id),
    operation_run_id: asOptionalString(source.operation_run_id),
    command_id: asOptionalString(source.command_id),
    activity_run_id: asOptionalString(source.activity_run_id),
    attempt_id: asOptionalString(source.attempt_id),
    acquisition_run_id: asOptionalString(source.acquisition_run_id),
    activity_type: asOptionalString(source.activity_type),
    owner: asOptionalString(source.owner),
    entity_type: asOptionalString(source.entity_type),
    entity_key: asOptionalString(source.entity_key),
    delta_kind: asOptionalString(source.delta_kind),
    status: asOptionalString(source.status),
    reason: asOptionalString(source.reason),
    source_ref: asOptionalWorkflowCommandPublicMirrorObject(source.source_ref, traversal, depth + 1),
    entity_payload: asOptionalWorkflowCommandPublicMirrorObject(source.entity_payload, traversal, depth + 1),
    projection_effect: asOptionalWorkflowCommandPublicMirrorObject(source.projection_effect, traversal, depth + 1),
    artifact_refs: asOptionalWorkflowCommandJsonArray(source.artifact_refs, traversal, depth + 1),
    idempotency_key: asOptionalString(source.idempotency_key),
    metadata: asOptionalWorkflowCommandPublicMirrorObject(source.metadata, traversal, depth + 1),
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
    mutation_contract: asOptionalString(source.mutation_contract),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
    control_target: mapOptionalPlainWorkflowPublicObject(
      source.control_target,
      (value) => mapWorkflowActivityControlTarget(value, traversal, depth + 1),
    ),
  };
  markWorkflowPublicProjectionClosed(mapped, traversal);
  return mapped;
}

export function mapWorkflowActivityListResponse(payload: unknown): WorkflowActivityListResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowActivityListResponse",
    ["workflow_activities"],
  );
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    contract: asOptionalString(source.contract),
    workflow_activities: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_activities,
      mapWorkflowActivityRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapWorkflowActivityDetailResponse(payload: unknown): WorkflowActivityDetailResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowActivityDetailResponse",
    ["workflow_activity", "activity_attempts"],
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    workflow_activity: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_activity,
      mapWorkflowActivityRecord,
      traversal,
      depth + 1,
    ),
    activity_attempts: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.activity_attempts,
      mapWorkflowActivityAttemptRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapWorkflowActivityAttemptListResponse(payload: unknown): WorkflowActivityAttemptListResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowActivityAttemptListResponse",
    ["workflow_activity_attempts"],
  );
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    contract: asOptionalString(source.contract),
    workflow_activity_attempts: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_activity_attempts,
      mapWorkflowActivityAttemptRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapWorkflowActivityAttemptDetailResponse(payload: unknown): WorkflowActivityAttemptDetailResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowActivityAttemptDetailResponse",
    ["workflow_activity_attempt"],
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    workflow_activity_attempt: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_activity_attempt,
      mapWorkflowActivityAttemptRecord,
      traversal,
      depth + 1,
    ),
  };
}

export function mapWorkflowEntityDeltaListResponse(payload: unknown): WorkflowEntityDeltaListResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowEntityDeltaListResponse",
    ["workflow_entity_deltas"],
  );
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    contract: asOptionalString(source.contract),
    workflow_entity_deltas: mapCapturedPlainWorkflowPublicObjectArray(
      canonical.workflow_entity_deltas,
      mapWorkflowEntityDeltaRecord,
      traversal,
      depth + 1,
    ) ?? [],
  };
}

export function mapWorkflowEntityDeltaDetailResponse(payload: unknown): WorkflowEntityDeltaDetailResponse {
  const { source, canonical, traversal, depth } = captureWorkflowPublicEnvelope(
    payload,
    "WorkflowEntityDeltaDetailResponse",
    ["workflow_entity_delta"],
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    workflow_entity_delta: mapCapturedPlainWorkflowPublicObject(
      canonical.workflow_entity_delta,
      mapWorkflowEntityDeltaRecord,
      traversal,
      depth + 1,
    ),
  };
}

export function mapAcquisitionDiscoveryLaneRecord(payload: unknown): AcquisitionDiscoveryLaneRecord {
  const source = asObject(payload, "AcquisitionDiscoveryLaneRecord");
  return {
    ...(source as JsonObject),
    lane_id: asOptionalString(source.lane_id),
    workspace_id: asOptionalString(source.workspace_id),
    acquisition_run_id: asOptionalString(source.acquisition_run_id),
    workflow_run_id: asOptionalString(source.workflow_run_id),
    operation_run_id: asOptionalString(source.operation_run_id),
    source_command_id: asOptionalString(source.source_command_id),
    activity_run_id: asOptionalString(source.activity_run_id),
    target_company: asOptionalString(source.target_company),
    query: asOptionalString(source.query),
    provider: asOptionalString(source.provider),
    status: asOptionalString(source.status),
    phase: asOptionalString(source.phase),
    read_model_role: asOptionalString(source.read_model_role),
    mutation_contract: asOptionalString(source.mutation_contract),
    module_state_mutated: asOptionalBoolean(source.module_state_mutated),
    control_target: mapOptionalPlainWorkflowPublicObject(
      source.control_target,
      mapWorkflowActivityControlTarget,
    ),
  };
}

export function mapAcquisitionDiscoveryLaneListResponse(payload: unknown): AcquisitionDiscoveryLaneListResponse {
  const source = asObject(payload, "AcquisitionDiscoveryLaneListResponse");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    contract: asOptionalString(source.contract),
    acquisition_discovery_lanes: asArray(source.acquisition_discovery_lanes).map(
      mapAcquisitionDiscoveryLaneRecord,
    ),
  };
}

export function mapAcquisitionDiscoveryLaneDetailResponse(payload: unknown): AcquisitionDiscoveryLaneDetailResponse {
  const source = asObject(payload, "AcquisitionDiscoveryLaneDetailResponse");
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    contract: asOptionalString(source.contract),
    acquisition_discovery_lane: source.acquisition_discovery_lane
      ? mapAcquisitionDiscoveryLaneRecord(source.acquisition_discovery_lane)
      : undefined,
  };
}

export function mapTargetCandidatePublicWebSignal(payload: unknown): TargetCandidatePublicWebSignal {
  const source = asObject(payload, "TargetCandidatePublicWebSignal");
  return {
    ...(source as JsonObject),
    signal_id: asOptionalString(source.signal_id),
    run_id: asOptionalString(source.run_id),
    asset_id: asOptionalString(source.asset_id),
    person_identity_key: asOptionalString(source.person_identity_key),
    record_id: asOptionalString(source.record_id),
    candidate_id: asOptionalString(source.candidate_id),
    signal_kind: asOptionalString(source.signal_kind) ?? "",
    signal_type: asOptionalString(source.signal_type),
    email_type: asOptionalString(source.email_type),
    value: asOptionalString(source.value),
    normalized_value: asOptionalString(source.normalized_value),
    url: asOptionalString(source.url),
    source_url: asOptionalString(source.source_url),
    source_domain: asOptionalString(source.source_domain),
    source_family: asOptionalString(source.source_family),
    source_title: asOptionalString(source.source_title),
    confidence_label: asOptionalString(source.confidence_label),
    confidence_score: asOptionalNumber(source.confidence_score),
    identity_match_label: asOptionalString(source.identity_match_label),
    identity_match_score: asOptionalNumber(source.identity_match_score),
    publishable: asOptionalBoolean(source.publishable),
    promotion_status: asOptionalString(source.promotion_status),
    promotion_id: asOptionalString(source.promotion_id),
    promotion_action: asOptionalString(source.promotion_action),
    promoted_field: asOptionalString(source.promoted_field),
    promoted_value: asOptionalString(source.promoted_value),
    previous_value: asOptionalString(source.previous_value),
    promoted_by: asOptionalString(source.promoted_by),
    promoted_at: asOptionalString(source.promoted_at),
    promotion_note: asOptionalString(source.promotion_note),
    promotion_override_reason: asOptionalString(source.promotion_override_reason),
    promotion_override_validation_reason: asOptionalString(source.promotion_override_validation_reason),
    promotion_requires_manual_override: asOptionalBoolean(source.promotion_requires_manual_override),
    suppression_reason: asOptionalString(source.suppression_reason),
    evidence_excerpt: asOptionalString(source.evidence_excerpt),
    artifact_refs: source.artifact_refs ? asJsonObject(source.artifact_refs) : undefined,
    model_provider: asOptionalString(source.model_provider),
    model_version: asOptionalString(source.model_version),
    link_shape_warnings: asOptionalStringArray(source.link_shape_warnings),
    clean_profile_link: asOptionalBoolean(source.clean_profile_link),
    metadata: source.metadata ? asJsonObject(source.metadata) : undefined,
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
  };
}

export function mapTargetCandidatePublicWebPromotion(payload: unknown): TargetCandidatePublicWebPromotion {
  const source = asObject(payload, "TargetCandidatePublicWebPromotion");
  return {
    ...(source as JsonObject),
    promotion_id: asOptionalString(source.promotion_id),
    signal_id: asOptionalString(source.signal_id),
    run_id: asOptionalString(source.run_id),
    record_id: asOptionalString(source.record_id),
    signal_kind: asOptionalString(source.signal_kind),
    signal_type: asOptionalString(source.signal_type),
    email_type: asOptionalString(source.email_type),
    new_value: asOptionalString(source.new_value),
    previous_value: asOptionalString(source.previous_value),
    source_url: asOptionalString(source.source_url),
    source_domain: asOptionalString(source.source_domain),
    confidence_label: asOptionalString(source.confidence_label),
    identity_match_label: asOptionalString(source.identity_match_label),
    action: asOptionalString(source.action),
    promotion_status: asOptionalString(source.promotion_status),
    operator: asOptionalString(source.operator),
    note: asOptionalString(source.note),
    override_reason: asOptionalString(source.override_reason),
    override_validation_reason: asOptionalString(source.override_validation_reason),
    requires_manual_override: asOptionalBoolean(source.requires_manual_override),
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
  };
}

export function mapTargetCandidatePublicWebEvidenceLink(
  payload: unknown,
): TargetCandidatePublicWebEvidenceLink {
  const source = asObject(payload, "TargetCandidatePublicWebEvidenceLink");
  return {
    ...(source as JsonObject),
    source_url: asOptionalString(source.source_url) ?? "",
    source_domain: asOptionalString(source.source_domain),
    source_family: asOptionalString(source.source_family),
    source_title: asOptionalString(source.source_title),
    signal_ids: asOptionalStringArray(source.signal_ids),
    signal_kinds: asOptionalStringArray(source.signal_kinds),
    signal_types: asOptionalStringArray(source.signal_types),
    identity_match_labels: asOptionalStringArray(source.identity_match_labels),
    max_confidence_score: asOptionalNumber(source.max_confidence_score),
  };
}

export function mapTargetCandidatePublicWebBatch(payload: unknown): TargetCandidatePublicWebBatch {
  const source = asObject(payload, "TargetCandidatePublicWebBatch");
  return {
    ...(source as JsonObject),
    batch_id: asOptionalString(source.batch_id),
    workspace_id: asOptionalString(source.workspace_id),
    status: normalizeTargetCandidatePublicWebStatus(source.status),
    requested_record_ids: asOptionalStringArray(source.requested_record_ids),
    run_ids: asOptionalStringArray(source.run_ids),
    source_families: asOptionalStringArray(source.source_families),
    summary: source.summary ? asJsonObject(source.summary) : {},
    created_at: asOptionalString(source.created_at),
    updated_at: asOptionalString(source.updated_at),
  };
}

export function mapTargetCandidatePublicWebRun(payload: unknown): TargetCandidatePublicWebRun {
  const source = asObject(payload, "TargetCandidatePublicWebRun");
  return {
    ...(source as JsonObject),
    run_id: asOptionalString(source.run_id),
    workspace_id: asOptionalString(source.workspace_id),
    batch_id: asOptionalString(source.batch_id),
    record_id: asOptionalString(source.record_id),
    candidate_id: asOptionalString(source.candidate_id),
    candidate_name: asOptionalString(source.candidate_name),
    current_company: asOptionalString(source.current_company),
    linkedin_url: asOptionalString(source.linkedin_url),
    status: normalizeTargetCandidatePublicWebStatus(source.status),
    phase: asOptionalString(source.phase),
    source_families: asOptionalStringArray(source.source_families),
    summary: source.summary ? asJsonObject(source.summary) : {},
    query_manifest: asObjectArray(source.query_manifest),
    search_checkpoint: source.search_checkpoint ? asJsonObject(source.search_checkpoint) : {},
    analysis_checkpoint: source.analysis_checkpoint ? asJsonObject(source.analysis_checkpoint) : {},
    phase_commands: source.phase_commands ? asJsonObject(source.phase_commands) : {},
    phase_command_display_line: asOptionalString(source.phase_command_display_line),
    run_control_state: source.run_control_state ? asJsonObject(source.run_control_state) : {},
    run_display_contract: source.run_display_contract ? asJsonObject(source.run_display_contract) : {},
    artifact_root: asOptionalString(source.artifact_root),
    last_error: asOptionalString(source.last_error),
    created_at: asOptionalString(source.created_at),
    started_at: asOptionalString(source.started_at),
    completed_at: asOptionalString(source.completed_at),
    updated_at: asOptionalString(source.updated_at),
  };
}

export function normalizeTargetCandidatePublicWebStatus(value: unknown): TargetCandidatePublicWebStatus {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (
    normalized === "queued" ||
    normalized === "search_submitted" ||
    normalized === "searching" ||
    normalized === "entry_links_ready" ||
    normalized === "fetching" ||
    normalized === "documents_fetched" ||
    normalized === "analyzing" ||
    normalized === "adjudication_completed" ||
    normalized === "analysis_completed" ||
    normalized === "completed" ||
    normalized === "completed_with_errors" ||
    normalized === "needs_review" ||
    normalized === "failed" ||
    normalized === "cancelled"
  ) {
    return normalized;
  }
  return "unknown";
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
    workflow_stage_summaries: source.workflow_stage_summaries
      ? mapWorkflowStageSummariesPayload(source.workflow_stage_summaries)
      : undefined,
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
    request_preview: source.request_preview ? asJsonObject(source.request_preview) : undefined,
    workflow_stage_summaries: source.workflow_stage_summaries
      ? mapWorkflowStageSummariesPayload(source.workflow_stage_summaries)
      : undefined,
  };
}

export function mapSystemProgressResponse(payload: unknown): SystemProgressResponse {
  const source = asObject(payload, "SystemProgressResponse");
  const workflowJobs = asObject(source.workflow_jobs ?? {}, "SystemProgressResponse.workflow_jobs");
  const objectSync = asObject(source.object_sync ?? {}, "SystemProgressResponse.object_sync");
  const cloudAssetOperations = asObject(
    source.cloud_asset_operations ?? {},
    "SystemProgressResponse.cloud_asset_operations",
  );
  return {
    ...(source as JsonObject),
    status: asString(source.status),
    observed_at: asOptionalString(source.observed_at),
    runtime: source.runtime ? mapRuntimeMetricsResponse(source.runtime) : mapRuntimeMetricsResponse({}),
    workflow_jobs: {
      ...(workflowJobs as JsonObject),
      count: asOptionalNumber(workflowJobs.count) ?? 0,
      items: asArray(workflowJobs.items).map((item) => {
        const entry = asObject(item, "SystemProgressWorkflowItem");
        return {
          ...(entry as JsonObject),
          job_id: asString(entry.job_id),
          target_company: asOptionalString(entry.target_company),
          status: asOptionalString(entry.status),
          stage: asOptionalString(entry.stage),
          updated_at: asOptionalString(entry.updated_at),
          runtime_health: entry.runtime_health ? mapJobRuntimeHealth(entry.runtime_health) : undefined,
          counters: asRecordOfNumber(entry.counters),
          latest_metrics: entry.latest_metrics ? mapProgressMetrics(entry.latest_metrics) : undefined,
          refresh_metrics: entry.refresh_metrics ? mapRefreshMetricsSummary(entry.refresh_metrics) : undefined,
          pre_retrieval_refresh: entry.pre_retrieval_refresh ? asJsonObject(entry.pre_retrieval_refresh) : undefined,
          background_reconcile: entry.background_reconcile ? asJsonObject(entry.background_reconcile) : undefined,
        };
      }),
    },
    profile_registry: source.profile_registry ? asJsonObject(source.profile_registry) : {},
    object_sync: {
      ...(objectSync as JsonObject),
      tracked_bundle_count: asOptionalNumber(objectSync.tracked_bundle_count),
      bundle_index_updated_at: asOptionalString(objectSync.bundle_index_updated_at),
      active_transfer_count: asOptionalNumber(objectSync.active_transfer_count),
      status_counts: asRecordOfNumber(objectSync.status_counts),
      recent_transfers: asArray(objectSync.recent_transfers).map((item) => {
        const entry = asObject(item, "ObjectSyncTransferProgressItem");
        return {
          ...(entry as JsonObject),
          bundle_id: asOptionalString(entry.bundle_id),
          bundle_kind: asOptionalString(entry.bundle_kind),
          direction: asOptionalString(entry.direction),
          status: asOptionalString(entry.status),
          updated_at: asOptionalString(entry.updated_at),
          completion_ratio: asOptionalNumber(entry.completion_ratio),
          requested_file_count: asOptionalNumber(entry.requested_file_count),
          completed_file_count: asOptionalNumber(entry.completed_file_count),
          remaining_file_count: asOptionalNumber(entry.remaining_file_count),
          transfer_mode: asOptionalString(entry.transfer_mode),
          bundle_dir: asOptionalString(entry.bundle_dir),
          progress_path: asOptionalString(entry.progress_path),
          archive: entry.archive ? asJsonObject(entry.archive) : undefined,
        };
      }),
    },
    cloud_asset_operations: {
      ...(cloudAssetOperations as JsonObject),
      count: asOptionalNumber(cloudAssetOperations.count),
      items: asArray(cloudAssetOperations.items).map((item) => {
        const entry = asObject(item, "CloudAssetOperationItem");
        return {
          ...(entry as JsonObject),
          ledger_id: asOptionalNumber(entry.ledger_id),
          operation_type: asOptionalString(entry.operation_type),
          bundle_kind: asOptionalString(entry.bundle_kind),
          bundle_id: asOptionalString(entry.bundle_id),
          sync_run_id: asOptionalString(entry.sync_run_id),
          status: asOptionalString(entry.status),
          manifest_path: asOptionalString(entry.manifest_path),
          target_runtime_dir: asOptionalString(entry.target_runtime_dir),
          target_db_path: asOptionalString(entry.target_db_path),
          scoped_companies: asArray(entry.scoped_companies)
            .map((value) => asString(value))
            .filter(Boolean),
          scoped_snapshot_id: asOptionalString(entry.scoped_snapshot_id),
          summary: entry.summary ? asJsonObject(entry.summary) : undefined,
          metadata: entry.metadata ? asJsonObject(entry.metadata) : undefined,
          created_at: asOptionalString(entry.created_at),
          updated_at: asOptionalString(entry.updated_at),
        };
      }),
    },
    company_asset: source.company_asset ? asJsonObject(source.company_asset) : undefined,
  };
}

export function mapRuntimeMetricsResponse(payload: unknown): RuntimeMetricsResponse {
  const source = asObject(payload, "RuntimeMetricsResponse");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status) ?? "",
    observed_at: asOptionalString(source.observed_at),
    metrics: source.metrics ? asJsonObject(source.metrics) : {},
    refresh_metrics: source.refresh_metrics ? mapRuntimeRefreshMetricsSummary(source.refresh_metrics) : undefined,
    event_level_efficiency: source.event_level_efficiency
      ? asJsonObject(source.event_level_efficiency)
      : undefined,
    services: source.services ? mapRuntimeServicesSummary(source.services) : undefined,
  };
}

export function mapRuntimeHealthResponse(payload: unknown): RuntimeHealthResponse {
  const source = asObject(payload, "RuntimeHealthResponse");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status) ?? "",
    observed_at: asOptionalString(source.observed_at),
    providers: source.providers ? asJsonObject(source.providers) : undefined,
    services: source.services ? mapRuntimeHealthServicesSummary(source.services) : undefined,
    metrics: source.metrics ? asJsonObject(source.metrics) : undefined,
    stalled_jobs: asArray(source.stalled_jobs).map((item) => {
      const entry = asObject(item, "RuntimeHealthResponse.stalled_jobs");
      return {
        ...(entry as JsonObject),
        job_id: asOptionalString(entry.job_id),
        status: asOptionalString(entry.status),
        stage: asOptionalString(entry.stage),
        updated_at: asOptionalString(entry.updated_at),
        runtime_health: entry.runtime_health ? mapJobRuntimeHealth(entry.runtime_health) : undefined,
      };
    }),
    stale_jobs: source.stale_jobs ? mapRuntimeStaleJobsSummary(source.stale_jobs) : undefined,
    recoverable_workers: source.recoverable_workers
      ? mapRuntimeRecoverableWorkersSummary(source.recoverable_workers)
      : undefined,
  };
}

export function mapRuntimeRefreshMetricsSummary(payload: unknown): RuntimeRefreshMetricsSummary {
  const source = asObject(payload, "RuntimeRefreshMetricsSummary");
  return {
    ...(source as JsonObject),
    pre_retrieval_refresh_job_count: asOptionalNumber(source.pre_retrieval_refresh_job_count),
    inline_search_seed_worker_count: asOptionalNumber(source.inline_search_seed_worker_count),
    inline_harvest_prefetch_worker_count: asOptionalNumber(source.inline_harvest_prefetch_worker_count),
    background_reconcile_job_count: asOptionalNumber(source.background_reconcile_job_count),
    background_search_seed_reconcile_job_count: asOptionalNumber(source.background_search_seed_reconcile_job_count),
    background_harvest_prefetch_reconcile_job_count: asOptionalNumber(
      source.background_harvest_prefetch_reconcile_job_count,
    ),
  };
}

export function mapRuntimeServicesSummary(payload: unknown): RuntimeServicesSummary {
  const source = asObject(payload, "RuntimeServicesSummary");
  return {
    ...(source as JsonObject),
    shared_recovery: source.shared_recovery ? mapServiceStatusPayload(source.shared_recovery) : undefined,
    tracked_job_recovery_count: asOptionalNumber(source.tracked_job_recovery_count),
  };
}

export function mapRuntimeHealthServicesSummary(payload: unknown): RuntimeHealthServicesSummary {
  const source = asObject(payload, "RuntimeHealthServicesSummary");
  return {
    ...(source as JsonObject),
    shared_recovery: source.shared_recovery ? mapServiceStatusPayload(source.shared_recovery) : undefined,
    job_recoveries: asArray(source.job_recoveries).map(mapRuntimeJobRecoveryItem),
  };
}

export function mapRuntimeJobRecoveryItem(payload: unknown): RuntimeJobRecoveryItem {
  const source = asObject(payload, "RuntimeJobRecoveryItem");
  return {
    ...(source as JsonObject),
    job_id: asOptionalString(source.job_id),
    status: asOptionalString(source.status),
    stage: asOptionalString(source.stage),
    job_recovery: source.job_recovery ? mapRecoveryControlSummary(source.job_recovery) : undefined,
  };
}

export function mapRuntimeStaleJobsSummary(payload: unknown): RuntimeStaleJobsSummary {
  const source = asObject(payload, "RuntimeStaleJobsSummary");
  return {
    ...(source as JsonObject),
    acquiring: asArray(source.acquiring).map(mapRuntimeStaleJobItem),
    queued: asArray(source.queued).map(mapRuntimeStaleJobItem),
  };
}

export function mapRuntimeStaleJobItem(payload: unknown): RuntimeStaleJobItem {
  const source = asObject(payload, "RuntimeStaleJobItem");
  return {
    ...(source as JsonObject),
    job_id: asOptionalString(source.job_id),
    status: asOptionalString(source.status),
    stage: asOptionalString(source.stage),
    updated_at: asOptionalString(source.updated_at),
  };
}

export function mapRuntimeRecoverableWorkersSummary(payload: unknown): RuntimeRecoverableWorkersSummary {
  const source = asObject(payload, "RuntimeRecoverableWorkersSummary");
  return {
    ...(source as JsonObject),
    count: asOptionalNumber(source.count),
    sample: asArray(source.sample).map(mapRuntimeRecoverableWorkerItem),
  };
}

export function mapRuntimeRecoverableWorkerItem(payload: unknown): RuntimeRecoverableWorkerItem {
  const source = asObject(payload, "RuntimeRecoverableWorkerItem");
  return {
    ...(source as JsonObject),
    worker_id: asOptionalNumber(source.worker_id),
    job_id: asOptionalString(source.job_id),
    lane_id: asOptionalString(source.lane_id),
    status: asOptionalString(source.status),
  };
}

export function mapServiceStatusPayload(payload: unknown): ServiceStatusPayload {
  const source = asObject(payload, "ServiceStatusPayload");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    lock_status: asOptionalString(source.lock_status),
    pid: asOptionalNumber(source.pid),
    started_at: asOptionalString(source.started_at),
    updated_at: asOptionalString(source.updated_at),
    heartbeat_at: asOptionalString(source.heartbeat_at),
    detail: asOptionalString(source.detail),
  };
}

export function mapRecoveryControlSummary(payload: unknown): RecoveryControlSummary {
  const source = asObject(payload, "RecoveryControlSummary");
  return {
    ...(source as JsonObject),
    status: asOptionalString(source.status),
    service_name: asOptionalString(source.service_name),
    service_ready: asOptionalBoolean(source.service_ready),
    service_status: source.service_status ? mapServiceStatusPayload(source.service_status) : undefined,
  };
}

export function mapJobRuntimeHealth(payload: unknown): JobRuntimeHealth {
  const source = asObject(payload, "JobRuntimeHealth");
  return {
    ...(source as JsonObject),
    state: asOptionalString(source.state),
    classification: asOptionalString(source.classification),
    detail: asOptionalString(source.detail),
    blocked_task: asOptionalString(source.blocked_task),
    pending_worker_count: asOptionalNumber(source.pending_worker_count),
    active_worker_count: asOptionalNumber(source.active_worker_count),
  };
}

export function mapRetrievalJobResponse(payload: unknown): RetrievalJobResponse {
  const source = asObject(payload, "RetrievalJobResponse");
  return {
    ...(source as JsonObject),
    job_id: asString(source.job_id),
    status: asString(source.status),
    request: asJsonObject(source.request),
    request_preview: source.request_preview ? asJsonObject(source.request_preview) : undefined,
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
    request_preview: source.request_preview ? asJsonObject(source.request_preview) : undefined,
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
    request_preview: source.request_preview ? asJsonObject(source.request_preview) : undefined,
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
    policy_catalog: source.policy_catalog
      ? asArray(source.policy_catalog).map((item) => mapIntentRewritePolicyCatalogEntry(item))
      : undefined,
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
    policy_layer: asOptionalString(source.policy_layer),
    keywords: asOptionalStringArray(source.keywords),
    must_have_facets: asOptionalStringArray(source.must_have_facets),
    must_have_primary_role_buckets: asOptionalStringArray(source.must_have_primary_role_buckets),
    must_have_keywords: asOptionalStringArray(source.must_have_keywords),
    targeting_terms: asOptionalStringArray(source.targeting_terms),
    matched_terms: asOptionalStringArray(source.matched_terms),
    request_patch: source.request_patch ? asJsonObject(source.request_patch) : undefined,
    trigger_sources: source.trigger_sources ? asJsonObject(source.trigger_sources) : undefined,
    additional_rewrites: source.additional_rewrites
      ? asArray(source.additional_rewrites).map((item) => mapIntentRewriteRule(item))
      : undefined,
    notes: asOptionalString(source.notes),
  };
}

export function mapIntentRewritePolicyCatalogEntry(payload: unknown): IntentRewritePolicyCatalogEntry {
  const source = asObject(payload, "IntentRewritePolicyCatalogEntry");
  return {
    ...(source as JsonObject),
    rewrite_id: asOptionalString(source.rewrite_id),
    summary_label: asOptionalString(source.summary_label),
    policy_layer: asOptionalString(source.policy_layer),
    trigger_sources: source.trigger_sources ? asJsonObject(source.trigger_sources) : undefined,
    request_patch: source.request_patch ? asJsonObject(source.request_patch) : undefined,
    targeting_terms: asOptionalStringArray(source.targeting_terms),
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
    latest_metrics: source.latest_metrics ? mapProgressMetrics(source.latest_metrics) : undefined,
    counters: asRecordOfNumber(source.counters),
  };
}

export function mapWorkflowStageSummariesPayload(payload: unknown): WorkflowStageSummariesPayload {
  const source = asObject(payload, "WorkflowStageSummariesPayload");
  return {
    ...(source as JsonObject),
    directory: asOptionalString(source.directory),
    stage_order: asOptionalStringArray(source.stage_order),
    summaries: asRecordOfMappedObject(source.summaries, mapWorkflowStageSummaryItem),
  };
}

export function mapWorkflowStageSummaryItem(payload: unknown): WorkflowStageSummaryItem {
  const source = asObject(payload, "WorkflowStageSummaryItem");
  return {
    ...(source as JsonObject),
    stage: asOptionalString(source.stage),
    status: asOptionalString(source.status),
    summary_path: asOptionalString(source.summary_path),
  };
}

export function mapProgressMetrics(payload: unknown): ProgressMetrics {
  const source = asObject(payload, "ProgressMetrics");
  return {
    ...(source as JsonObject),
    refresh_metrics: source.refresh_metrics ? mapRefreshMetricsSummary(source.refresh_metrics) : undefined,
    pre_retrieval_refresh: source.pre_retrieval_refresh ? asJsonObject(source.pre_retrieval_refresh) : undefined,
    background_reconcile: source.background_reconcile ? asJsonObject(source.background_reconcile) : undefined,
  };
}

export function mapRefreshMetricsSummary(payload: unknown): RefreshMetricsSummary {
  const source = asObject(payload, "RefreshMetricsSummary");
  return {
    ...(source as JsonObject),
    pre_retrieval_refresh_count: asOptionalNumber(source.pre_retrieval_refresh_count),
    pre_retrieval_refresh_status: asOptionalString(source.pre_retrieval_refresh_status),
    inline_search_seed_worker_count: asOptionalNumber(source.inline_search_seed_worker_count),
    inline_harvest_prefetch_worker_count: asOptionalNumber(source.inline_harvest_prefetch_worker_count),
    pre_retrieval_refresh_snapshot_id: asOptionalString(source.pre_retrieval_refresh_snapshot_id),
    background_reconcile_count: asOptionalNumber(source.background_reconcile_count),
    background_search_seed_reconcile_count: asOptionalNumber(source.background_search_seed_reconcile_count),
    background_search_seed_reconcile_status: asOptionalString(source.background_search_seed_reconcile_status),
    background_search_seed_worker_count: asOptionalNumber(source.background_search_seed_worker_count),
    background_search_seed_added_entry_count: asOptionalNumber(source.background_search_seed_added_entry_count),
    background_harvest_prefetch_reconcile_count: asOptionalNumber(source.background_harvest_prefetch_reconcile_count),
    background_harvest_prefetch_reconcile_status: asOptionalString(source.background_harvest_prefetch_reconcile_status),
    background_harvest_prefetch_worker_count: asOptionalNumber(source.background_harvest_prefetch_worker_count),
    background_exploration_reconcile_count: asOptionalNumber(source.background_exploration_reconcile_count),
    background_exploration_reconcile_status: asOptionalString(source.background_exploration_reconcile_status),
    background_exploration_worker_count: asOptionalNumber(source.background_exploration_worker_count),
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
    matched_keywords: asOptionalStringArray(source.matched_keywords),
    matched_fields: source.matched_fields ? asObjectArray(source.matched_fields) : undefined,
    source_matches: source.source_matches ? asObjectArray(source.source_matches) : undefined,
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

function workflowPublicResponseContentLength(response: Response): string | null {
  try {
    return response.headers?.get("Content-Length") ?? null;
  } catch {
    return null;
  }
}

async function readWorkflowPublicResponseText(response: Response): Promise<string> {
  if (
    workflowPublicTransportContentLengthIsOverLimit(
      workflowPublicResponseContentLength(response),
    )
  ) {
    throw new Error("SourcingAgent API response exceeds the public transport body budget");
  }
  const bodyText = await response.text();
  if (workflowPublicTransportBodyIsOverLimit(bodyText)) {
    throw new Error("SourcingAgent API response exceeds the public transport body budget");
  }
  return bodyText;
}

type SourcingAgentResponseBodyBudget = "unbounded" | "workflow-public";

async function parseResponse<T>(
  response: Response,
  mapper: (payload: unknown) => T,
  bodyBudget: SourcingAgentResponseBodyBudget,
): Promise<T> {
  const bodyText = bodyBudget === "workflow-public"
    ? await readWorkflowPublicResponseText(response)
    : await response.text();
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

function normalizeCrmPublicWebFilters(filters: JsonObject): JsonObject {
  const recordIds = normalizeStringList(filters.crm_record_ids ?? filters.record_ids ?? filters.recordIds);
  const workspaceId = requireCrmPublicWebWorkspaceId(filters.workspace_id ?? filters.workspaceId);
  return {
    ...filters,
    crm_record_ids: recordIds,
    workspace_id: workspaceId,
    batch_id: filters.batch_id ?? filters.batchId ?? "",
    status: filters.status ?? "",
    limit: filters.limit ?? 1000,
  };
}

function normalizeCrmPublicWebStartPayload(payload: JsonObject): JsonObject {
  const recordIds = normalizeStringList(payload.crm_record_ids ?? payload.record_ids ?? payload.recordIds);
  const workspaceId = requireCrmPublicWebWorkspaceId(payload.workspace_id ?? payload.workspaceId);
  return {
    ...payload,
    crm_record_ids: recordIds,
    workspace_id: workspaceId,
    options: payload.options && typeof payload.options === "object" && !Array.isArray(payload.options)
      ? payload.options
      : {},
    force_refresh: payload.force_refresh ?? payload.forceRefresh ?? false,
    requested_by: payload.requested_by ?? payload.requestedBy ?? "",
  };
}

function requireCrmPublicWebWorkspaceId(value: JsonValue | undefined): string {
  const workspaceId = asOptionalString(value)?.trim() ?? "";
  if (!workspaceId) {
    throw new Error("CRM Public Web body-style requests require workspace_id.");
  }
  return workspaceId;
}

function normalizeStringList(value: JsonValue | undefined): string[] {
  const source = Array.isArray(value) ? value : typeof value === "string" ? [value] : [];
  return Array.from(
    new Set(
      source
        .filter((item): item is string | number | boolean => isQueryParamPrimitive(item))
        .map((item) => String(item).trim())
        .filter(Boolean),
    ),
  );
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

const WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS = [
  "authority_id",
  "authority_seal",
  "bootstrap_authority",
  "bootstrap_authority_id",
  "bootstrap_authority_digest",
  "bootstrap_receipt",
  "claim_authority",
  "claim_authority_id",
  "claim_authority_seal",
  "claim_authority_spec_digest",
  "claim_capability",
  "claim_identity",
  "claim_receipt",
  "claim_secret",
  "claim_selection_generation",
  "claim_token",
  "claim_token_digest",
  "consumed_claim_authority_id",
  "issuer_digest",
  "issuer_revision",
  "last_heartbeat_id",
  "lease_identity",
  "lease_token",
  "scoped_review_session_bootstrap_authority",
  "scoped_review_session_bootstrap_receipt",
] as const;

const WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS: ReadonlySet<string> = new Set(WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS);

const WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOT_SPECS = Array.from(
  WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS,
  (root) => ({
    normalized: root,
    compact: root.replace(/_/g, ""),
  }),
);

const WORKFLOW_COMMAND_PUBLIC_CARRIER_FIELDS: ReadonlySet<string> = new Set([
  "workflow_command",
  "latest_workflow_command",
]);
const WORKFLOW_COMMAND_PUBLIC_CARRIER_LIST_FIELDS: ReadonlySet<string> = new Set([
  "workflow_commands",
]);
const WORKFLOW_COMMAND_CONTROL_PUBLIC_ACTIVITY_CARRIER_FIELDS: ReadonlySet<string> = new Set([
  "workflow_activity",
  "workflow_activity_run",
  "workflow_activity_attempt",
  "workflow_entity_delta",
  "workflow_activity_runs",
  "workflow_activity_attempts",
  "workflow_entity_deltas",
]);
const WORKFLOW_PUBLIC_HAZARDOUS_MIRROR_FIELDS: ReadonlySet<string> = new Set([
  "__proto__",
  "prototype",
  "constructor",
]);

const WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS: ReadonlySet<string> = new Set([
  "workflow_activity",
  "workflow_activity_run",
]);
const WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS: ReadonlySet<string> = new Set([
  "workflow_activity_attempt",
]);
const WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS: ReadonlySet<string> = new Set([
  "workflow_entity_delta",
]);
const WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS: ReadonlySet<string> = new Set([
  "workflow_activities",
  "workflow_activity_runs",
]);
const WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS: ReadonlySet<string> = new Set([
  "workflow_activity_attempts",
]);
const WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS: ReadonlySet<string> = new Set([
  "workflow_entity_deltas",
]);
const WORKFLOW_ACTIVITY_RUN_TRUSTED_DERIVED_FIELDS: ReadonlySet<string> = new Set([
  "control_target",
  "module_state_mutated",
  "mutation_contract",
]);
const WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS: ReadonlySet<string> = new Set([
  "activity_type",
  "owner",
  "control_target",
  "module_state_mutated",
  "mutation_contract",
]);

function normalizeWorkflowCommandPublicMirrorFieldName(value: unknown): string {
  return String(value ?? "")
    .trim()
    .replace(/[-\s]+/g, "_")
    .replace(/(?<=[A-Z])(?=[A-Z][a-z])/g, "_")
    .replace(/(?<=[a-z0-9])(?=[A-Z])/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function isPrivateWorkflowCommandPublicMirrorField(value: unknown): boolean {
  const normalized = normalizeWorkflowCommandPublicMirrorFieldName(value);
  if (WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS.has(normalized)) {
    return true;
  }
  const compact = normalized.replace(/_/g, "");
  return WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOT_SPECS.some(
    (root) =>
      normalized.startsWith(`${root.normalized}_`) ||
      compact === root.compact ||
      compact.startsWith(root.compact),
  );
}

function isHazardousWorkflowPublicMirrorField(value: unknown): boolean {
  return WORKFLOW_PUBLIC_HAZARDOUS_MIRROR_FIELDS.has(String(value ?? "").trim().toLowerCase());
}

function isWorkflowActivityPublicCarrierField(value: unknown): boolean {
  const normalized = normalizeWorkflowCommandPublicMirrorFieldName(value);
  return (
    WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS.has(normalized) ||
    WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS.has(normalized) ||
    WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS.has(normalized) ||
    WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS.has(normalized) ||
    WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS.has(normalized) ||
    WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS.has(normalized)
  );
}

function isPlainWorkflowPublicObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  try {
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  } catch {
    return false;
  }
}

function mapOptionalPlainWorkflowPublicObject<T>(
  value: unknown,
  mapper: (payload: unknown) => T,
): T | undefined {
  return isPlainWorkflowPublicObject(value) ? mapper(value) : undefined;
}

function mapOptionalPlainWorkflowPublicObjectSafely<T>(
  value: unknown,
  mapper: (payload: unknown) => T,
): T | undefined {
  if (!isPlainWorkflowPublicObject(value)) {
    return undefined;
  }
  try {
    return mapper(value);
  } catch {
    return undefined;
  }
}

function mapSparsePlainWorkflowPublicObjectsSafely<T>(
  value: unknown,
  mapper: (payload: unknown) => T,
): T[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  const result: T[] = [];
  for (const item of asArray(value)) {
    const mapped = mapOptionalPlainWorkflowPublicObjectSafely(item, mapper);
    if (mapped !== undefined) {
      result.push(mapped);
    }
  }
  return result;
}

function defineWorkflowPublicOwnField(
  target: Record<string, unknown>,
  key: string,
  value: unknown,
): void {
  Object.defineProperty(target, key, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}

function hasWorkflowPublicSerializableFields(value: Record<string, unknown>): boolean {
  return Object.values(value).some((item) => item !== undefined);
}

export { WORKFLOW_PUBLIC_PROJECTION_LIMITS } from "./frontend_api_runtime_contract";

interface WorkflowPublicProjectionTraversal {
  visitedNodes: number;
  visitedBytes: number;
  readonly activeContainers: WeakSet<object>;
  readonly defaultClosedContainers: WeakSet<object>;
  readonly executionSummaryClosedContainers: WeakSet<object>;
  readonly defaultMemo: WeakMap<object, WorkflowPublicProjectionMemoEntry>;
  readonly executionSummaryMemo: WeakMap<object, WorkflowPublicProjectionMemoEntry>;
}

interface WorkflowPublicProjectionMemoEntry {
  readonly value: JsonValue | undefined;
  readonly nodes: number;
  readonly bytes: number;
  blocked: boolean;
}

interface WorkflowPublicOwnDataEntries {
  readonly entries: readonly [string, unknown][];
  readonly ownKeyCount: number;
  readonly hadRejectedDataProperty: boolean;
}

interface WorkflowPublicEnvelopeSnapshot {
  readonly source: Record<string, unknown>;
  readonly canonical: Record<string, unknown>;
  readonly traversal: WorkflowPublicProjectionTraversal;
  readonly depth: number;
  readonly payloadWasExactEmpty: boolean;
}

function createWorkflowPublicProjectionTraversal(): WorkflowPublicProjectionTraversal {
  return {
    visitedNodes: 0,
    visitedBytes: 0,
    activeContainers: new WeakSet<object>(),
    defaultClosedContainers: new WeakSet<object>(),
    executionSummaryClosedContainers: new WeakSet<object>(),
    defaultMemo: new WeakMap<object, WorkflowPublicProjectionMemoEntry>(),
    executionSummaryMemo: new WeakMap<object, WorkflowPublicProjectionMemoEntry>(),
  };
}

function consumeWorkflowPublicProjectionCost(
  traversal: WorkflowPublicProjectionTraversal,
  nodes: number,
  bytes: number,
): boolean {
  if (
    traversal.visitedNodes + nodes > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxNodes ||
    traversal.visitedBytes + bytes > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxOccurrenceBytes
  ) {
    return false;
  }
  traversal.visitedNodes += nodes;
  traversal.visitedBytes += bytes;
  return true;
}

function consumeWorkflowPublicProjectionNode(
  traversal: WorkflowPublicProjectionTraversal,
  bytes = 2,
): boolean {
  return consumeWorkflowPublicProjectionCost(traversal, 1, bytes);
}

function consumeWorkflowPublicProjectionKey(
  traversal: WorkflowPublicProjectionTraversal,
  key: string,
): boolean {
  return consumeWorkflowPublicProjectionCost(
    traversal,
    0,
    workflowPublicJsonStringByteLength(key) + 2,
  );
}

function workflowPublicPrimitiveByteLength(value: string | number | boolean | null): number {
  if (typeof value === "string") {
    return workflowPublicJsonStringByteLength(value) + 1;
  }
  return workflowPublicUtf8ByteLength(JSON.stringify(value)) + 1;
}

function captureWorkflowPublicOwnDataEntries(
  value: Record<string, unknown>,
): WorkflowPublicOwnDataEntries | undefined {
  let ownKeys: readonly PropertyKey[];
  try {
    ownKeys = Reflect.ownKeys(value);
  } catch {
    return undefined;
  }
  if (ownKeys.length > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries) {
    return undefined;
  }
  const entries: [string, unknown][] = [];
  let hadRejectedDataProperty = false;
  for (const key of ownKeys) {
    if (typeof key !== "string") {
      hadRejectedDataProperty = true;
      continue;
    }
    if (workflowPublicUtf8ByteLength(key) > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxKeyBytes) {
      hadRejectedDataProperty = true;
      continue;
    }
    if (
      isPrivateWorkflowCommandPublicMirrorField(key) ||
      isHazardousWorkflowPublicMirrorField(key)
    ) {
      continue;
    }
    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      return undefined;
    }
    if (!descriptor || !descriptor.enumerable || !("value" in descriptor)) {
      hadRejectedDataProperty = true;
      continue;
    }
    entries.push([key, descriptor.value]);
  }
  return { entries, ownKeyCount: ownKeys.length, hadRejectedDataProperty };
}

function captureWorkflowPublicArrayItems(value: unknown[]): readonly unknown[] | undefined {
  let lengthDescriptor: PropertyDescriptor | undefined;
  try {
    lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");
  } catch {
    return undefined;
  }
  if (
    !lengthDescriptor ||
    !("value" in lengthDescriptor) ||
    !Number.isSafeInteger(lengthDescriptor.value) ||
    lengthDescriptor.value < 0 ||
    lengthDescriptor.value > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxCollectionEntries
  ) {
    return undefined;
  }
  const items: unknown[] = [];
  for (let index = 0; index < lengthDescriptor.value; index += 1) {
    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    } catch {
      return undefined;
    }
    if (!descriptor || !descriptor.enumerable || !("value" in descriptor)) {
      continue;
    }
    items.push(descriptor.value);
  }
  return items;
}

function isExactEmptyWorkflowPublicDataObject(
  value: Record<string, unknown>,
  captured: WorkflowPublicOwnDataEntries,
): boolean {
  if (captured.ownKeyCount !== 0 || typeof structuredClone !== "function") {
    return false;
  }
  try {
    const cloned = structuredClone(value);
    return isPlainWorkflowPublicObject(cloned) && Reflect.ownKeys(cloned).length === 0;
  } catch {
    return false;
  }
}

function captureWorkflowPublicEnvelope(
  payload: unknown,
  label: string,
  canonicalFields: readonly string[],
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowPublicEnvelopeSnapshot {
  const rawSource = asObject(payload, label);
  if (!isPlainWorkflowPublicObject(rawSource)) {
    throw new Error(`${label} must be a plain object`);
  }
  const captured = captureWorkflowPublicOwnDataEntries(rawSource);
  if (!captured) {
    throw new Error(`${label} exceeds the public projection collection budget`);
  }
  const canonicalFieldSet = new Set(canonicalFields);
  const envelopeSource: Record<string, unknown> = {};
  const canonical: Record<string, unknown> = {};
  for (const [key, value] of captured.entries) {
    defineWorkflowPublicOwnField(
      canonicalFieldSet.has(key) ? canonical : envelopeSource,
      key,
      value,
    );
  }
  return {
    source: asWorkflowCommandPublicMirrorSource(
      envelopeSource,
      label,
      false,
      traversal,
      depth,
    ),
    canonical,
    traversal,
    depth,
    payloadWasExactEmpty: isExactEmptyWorkflowPublicDataObject(rawSource, captured),
  };
}

function mapCapturedPlainWorkflowPublicObject<T>(
  value: unknown,
  mapper: (
    payload: unknown,
    traversal: WorkflowPublicProjectionTraversal,
    depth: number,
  ) => T,
  traversal: WorkflowPublicProjectionTraversal,
  depth: number,
): T | undefined {
  if (!isPlainWorkflowPublicObject(value)) {
    return undefined;
  }
  try {
    return mapper(value, traversal, depth);
  } catch {
    return undefined;
  }
}

function mapCapturedPlainWorkflowPublicObjectArray<T>(
  value: unknown,
  mapper: (
    payload: unknown,
    traversal: WorkflowPublicProjectionTraversal,
    depth: number,
  ) => T,
  traversal: WorkflowPublicProjectionTraversal,
  depth: number,
): T[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  if (
    depth > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxDepth ||
    traversal.activeContainers.has(value) ||
    !consumeWorkflowPublicProjectionNode(traversal, 2)
  ) {
    return undefined;
  }
  const items = captureWorkflowPublicArrayItems(value);
  if (!items) {
    return undefined;
  }
  const result: T[] = [];
  traversal.activeContainers.add(value);
  try {
    for (const item of items) {
      const mapped = mapCapturedPlainWorkflowPublicObject(
        item,
        mapper,
        traversal,
        depth + 1,
      );
      if (
        mapped !== undefined &&
        (!isPlainWorkflowPublicObject(mapped) || hasWorkflowPublicSerializableFields(mapped))
      ) {
        result.push(mapped);
      }
    }
  } finally {
    traversal.activeContainers.delete(value);
  }
  markWorkflowPublicProjectionClosed(result, traversal);
  return result;
}

function markWorkflowPublicProjectionClosed(
  value: object,
  traversal: WorkflowPublicProjectionTraversal,
  allowExecutionSummaryAtCurrentLevel = false,
): void {
  if (allowExecutionSummaryAtCurrentLevel) {
    traversal.executionSummaryClosedContainers.add(value);
  } else {
    traversal.defaultClosedContainers.add(value);
  }
}

function isWorkflowPublicProjectionClosed(
  value: object,
  traversal: WorkflowPublicProjectionTraversal,
  allowExecutionSummaryAtCurrentLevel: boolean,
): boolean {
  return traversal.defaultClosedContainers.has(value) || (
    allowExecutionSummaryAtCurrentLevel &&
    traversal.executionSummaryClosedContainers.has(value)
  );
}

function workflowPublicProjectionMemo(
  traversal: WorkflowPublicProjectionTraversal,
  allowExecutionSummaryAtCurrentLevel: boolean,
): WeakMap<object, WorkflowPublicProjectionMemoEntry> {
  return allowExecutionSummaryAtCurrentLevel
    ? traversal.executionSummaryMemo
    : traversal.defaultMemo;
}

function consumeWorkflowPublicProjectionOccurrence(
  traversal: WorkflowPublicProjectionTraversal,
  entry: WorkflowPublicProjectionMemoEntry,
): boolean {
  if (entry.blocked) {
    return false;
  }
  if (!consumeWorkflowPublicProjectionCost(traversal, entry.nodes, entry.bytes)) {
    entry.blocked = true;
    return false;
  }
  return true;
}

function sanitizeWorkflowCommandPublicCarrier(
  key: string,
  value: unknown,
  traversal: WorkflowPublicProjectionTraversal,
  depth: number,
): { matched: boolean; value?: JsonValue } {
  const projectCommand = (
    command: Record<string, unknown>,
    commandDepth: number,
  ): JsonValue | undefined => {
    try {
      return mapWorkflowCommandGenericPublicCarrierRecord(
        command,
        traversal,
        commandDepth,
      ) as unknown as JsonValue;
    } catch {
      return undefined;
    }
  };
  const normalizedKey = normalizeWorkflowCommandPublicMirrorFieldName(key);
  if (WORKFLOW_COMMAND_PUBLIC_CARRIER_FIELDS.has(normalizedKey)) {
    return {
      matched: true,
      value: isPlainWorkflowPublicObject(value) ? projectCommand(value, depth) : undefined,
    };
  }
  if (!WORKFLOW_COMMAND_PUBLIC_CARRIER_LIST_FIELDS.has(normalizedKey)) {
    return { matched: false };
  }
  if (!Array.isArray(value)) {
    return { matched: true };
  }
  if (
    depth > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxDepth ||
    traversal.activeContainers.has(value) ||
    !consumeWorkflowPublicProjectionNode(traversal, 2)
  ) {
    return { matched: true };
  }
  const items = captureWorkflowPublicArrayItems(value);
  if (!items) {
    return { matched: true };
  }
  const projected: JsonValue[] = [];
  traversal.activeContainers.add(value);
  try {
    for (const item of items) {
      if (!isPlainWorkflowPublicObject(item)) {
        continue;
      }
      const sanitized = projectCommand(item, depth + 1);
      if (
        sanitized !== undefined &&
        (!isPlainWorkflowPublicObject(sanitized) || hasWorkflowPublicSerializableFields(sanitized))
      ) {
        projected.push(sanitized);
      }
    }
  } finally {
    traversal.activeContainers.delete(value);
  }
  markWorkflowPublicProjectionClosed(projected, traversal);
  return { matched: true, value: projected };
}

function mapWorkflowCommandGenericPublicCarrierRecord(
  payload: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): WorkflowCommandRecord {
  const projected = mapWorkflowCommandRecord(payload, traversal, depth);
  delete projected.execution_summary;
  markWorkflowPublicProjectionClosed(projected, traversal);
  return projected;
}

function sanitizeWorkflowActivityPublicCarrier(
  key: string,
  value: unknown,
  traversal: WorkflowPublicProjectionTraversal,
  depth: number,
): { matched: boolean; value?: JsonValue } {
  const normalizedKey = normalizeWorkflowCommandPublicMirrorFieldName(key);
  let mapper:
    | ((payload: unknown, traversal: WorkflowPublicProjectionTraversal, depth: number) => unknown)
    | undefined;
  let trustedDerivedFields: ReadonlySet<string> | undefined;
  if (WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS.has(normalizedKey)) {
    mapper = mapWorkflowActivityRecord;
    trustedDerivedFields = WORKFLOW_ACTIVITY_RUN_TRUSTED_DERIVED_FIELDS;
  } else if (WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS.has(normalizedKey)) {
    mapper = mapWorkflowActivityAttemptRecord;
    trustedDerivedFields = WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS;
  } else if (WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS.has(normalizedKey)) {
    mapper = mapWorkflowEntityDeltaRecord;
    trustedDerivedFields = WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS;
  }
  if (mapper) {
    let projected: Record<string, unknown> | undefined;
    if (isPlainWorkflowPublicObject(value)) {
      try {
        projected = mapper(value, traversal, depth) as Record<string, unknown>;
      } catch {
        projected = undefined;
      }
    }
    if (projected && typeof projected === "object" && !Array.isArray(projected)) {
      for (const field of trustedDerivedFields ?? []) {
        delete (projected as Record<string, unknown>)[field];
      }
      markWorkflowPublicProjectionClosed(projected, traversal);
    }
    return {
      matched: true,
      value: projected ? (projected as JsonValue) : undefined,
    };
  }

  let listMapper:
    | ((payload: unknown, traversal: WorkflowPublicProjectionTraversal, depth: number) => unknown)
    | undefined;
  let listTrustedDerivedFields: ReadonlySet<string> | undefined;
  if (WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS.has(normalizedKey)) {
    listMapper = mapWorkflowActivityRecord;
    listTrustedDerivedFields = WORKFLOW_ACTIVITY_RUN_TRUSTED_DERIVED_FIELDS;
  } else if (WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS.has(normalizedKey)) {
    listMapper = mapWorkflowActivityAttemptRecord;
    listTrustedDerivedFields = WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS;
  } else if (WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS.has(normalizedKey)) {
    listMapper = mapWorkflowEntityDeltaRecord;
    listTrustedDerivedFields = WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS;
  }
  if (!listMapper) {
    return { matched: false };
  }
  if (!Array.isArray(value)) {
    return { matched: true };
  }
  if (
    depth > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxDepth ||
    traversal.activeContainers.has(value) ||
    !consumeWorkflowPublicProjectionNode(traversal, 2)
  ) {
    return { matched: true };
  }
  const items = captureWorkflowPublicArrayItems(value);
  if (!items) {
    return { matched: true };
  }
  const projected: JsonValue[] = [];
  traversal.activeContainers.add(value);
  try {
    for (const item of items) {
      if (!isPlainWorkflowPublicObject(item)) {
        continue;
      }
      let mapped: Record<string, unknown>;
      try {
        mapped = listMapper(item, traversal, depth + 1) as Record<string, unknown>;
      } catch {
        continue;
      }
      for (const field of listTrustedDerivedFields ?? []) {
        delete mapped[field];
      }
      markWorkflowPublicProjectionClosed(mapped, traversal);
      if (hasWorkflowPublicSerializableFields(mapped)) {
        projected.push(mapped as JsonValue);
      }
    }
  } finally {
    traversal.activeContainers.delete(value);
  }
  markWorkflowPublicProjectionClosed(projected, traversal);
  return { matched: true, value: projected };
}

function sanitizeWorkflowCommandPublicMirrorValue(
  value: unknown,
  allowExecutionSummaryAtCurrentLevel = false,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): JsonValue | undefined {
  if (depth > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxDepth) {
    return undefined;
  }
  if (value === null) {
    return consumeWorkflowPublicProjectionNode(
      traversal,
      workflowPublicPrimitiveByteLength(null),
    ) ? null : undefined;
  }
  if (typeof value === "string") {
    if (workflowPublicUtf8ByteLength(value) > WORKFLOW_PUBLIC_PROJECTION_LIMITS.maxStringBytes) {
      return undefined;
    }
    return consumeWorkflowPublicProjectionNode(
      traversal,
      workflowPublicPrimitiveByteLength(value),
    ) ? value : undefined;
  }
  if (typeof value === "boolean") {
    return consumeWorkflowPublicProjectionNode(
      traversal,
      workflowPublicPrimitiveByteLength(value),
    ) ? value : undefined;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) && consumeWorkflowPublicProjectionNode(
      traversal,
      workflowPublicPrimitiveByteLength(value),
    ) ? value : undefined;
  }
  if (!value || typeof value !== "object") {
    return undefined;
  }
  if (traversal.activeContainers.has(value)) {
    return undefined;
  }
  if (
    isWorkflowPublicProjectionClosed(
      value,
      traversal,
      allowExecutionSummaryAtCurrentLevel,
    )
  ) {
    return value as JsonValue;
  }
  const memo = workflowPublicProjectionMemo(
    traversal,
    allowExecutionSummaryAtCurrentLevel,
  );
  if (memo.has(value)) {
    const cached = memo.get(value)!;
    return cached.value !== undefined && consumeWorkflowPublicProjectionOccurrence(traversal, cached)
      ? cached.value
      : undefined;
  }
  const memoStartNodes = traversal.visitedNodes;
  const memoStartBytes = traversal.visitedBytes;
  if (!consumeWorkflowPublicProjectionNode(traversal, 2)) {
    memo.set(value, { value: undefined, nodes: 0, bytes: 0, blocked: true });
    return undefined;
  }
  if (Array.isArray(value)) {
    const items = captureWorkflowPublicArrayItems(value);
    if (!items) {
      memo.set(value, { value: undefined, nodes: 0, bytes: 0, blocked: true });
      return undefined;
    }
    const result: JsonValue[] = [];
    traversal.activeContainers.add(value);
    try {
      for (const item of items) {
        const sanitized = sanitizeWorkflowCommandPublicMirrorValue(
          item,
          false,
          traversal,
          depth + 1,
        );
        if (
          sanitized !== undefined &&
          (!isPlainWorkflowPublicObject(sanitized) || hasWorkflowPublicSerializableFields(sanitized))
        ) {
          result.push(sanitized);
        }
      }
    } finally {
      traversal.activeContainers.delete(value);
    }
    markWorkflowPublicProjectionClosed(result, traversal);
    memo.set(value, {
      value: result,
      nodes: traversal.visitedNodes - memoStartNodes,
      bytes: traversal.visitedBytes - memoStartBytes,
      blocked: false,
    });
    return result;
  }
  let prototype: object | null;
  try {
    prototype = Object.getPrototypeOf(value);
  } catch {
    memo.set(value, { value: undefined, nodes: 0, bytes: 0, blocked: true });
    return undefined;
  }
  if (prototype !== Object.prototype && prototype !== null) {
    memo.set(value, { value: undefined, nodes: 0, bytes: 0, blocked: true });
    return undefined;
  }
  const captured = captureWorkflowPublicOwnDataEntries(value as Record<string, unknown>);
  if (!captured) {
    memo.set(value, { value: undefined, nodes: 0, bytes: 0, blocked: true });
    return undefined;
  }
  const result: JsonObject = {};
  traversal.activeContainers.add(value);
  try {
    for (const [key, item] of captured.entries) {
      const normalizedKey = normalizeWorkflowCommandPublicMirrorFieldName(key);
      if (
        isPrivateWorkflowCommandPublicMirrorField(key) ||
        isHazardousWorkflowPublicMirrorField(key) ||
        (normalizedKey === "execution_summary" && !allowExecutionSummaryAtCurrentLevel)
      ) {
        continue;
      }
      if (!consumeWorkflowPublicProjectionKey(traversal, key)) {
        break;
      }
      const commandCarrier = sanitizeWorkflowCommandPublicCarrier(
        key,
        item,
        traversal,
        depth + 1,
      );
      if (commandCarrier.matched) {
        if (commandCarrier.value !== undefined) {
          defineWorkflowPublicOwnField(result, key, commandCarrier.value);
        }
        continue;
      }
      const carrier = sanitizeWorkflowActivityPublicCarrier(
        key,
        item,
        traversal,
        depth + 1,
      );
      if (carrier.matched) {
        if (carrier.value !== undefined) {
          defineWorkflowPublicOwnField(result, key, carrier.value);
        }
        continue;
      }
      const sanitized = sanitizeWorkflowCommandPublicMirrorValue(
        item,
        false,
        traversal,
        depth + 1,
      );
      if (sanitized !== undefined) {
        defineWorkflowPublicOwnField(result, key, sanitized);
      }
    }
  } finally {
    traversal.activeContainers.delete(value);
  }
  markWorkflowPublicProjectionClosed(
    result,
    traversal,
    allowExecutionSummaryAtCurrentLevel,
  );
  const publicResult = captured.hadRejectedDataProperty && Object.keys(result).length === 0
    ? undefined
    : result;
  memo.set(value, {
    value: publicResult,
    nodes: traversal.visitedNodes - memoStartNodes,
    bytes: traversal.visitedBytes - memoStartBytes,
    blocked: false,
  });
  return publicResult;
}

function asWorkflowCommandPublicMirrorSource(
  value: unknown,
  label: string,
  allowExecutionSummaryAtCurrentLevel = false,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): Record<string, unknown> {
  const source = asObject(value, label);
  const sanitized = sanitizeWorkflowCommandPublicMirrorValue(
    source,
    allowExecutionSummaryAtCurrentLevel,
    traversal,
    depth,
  );
  if (!sanitized || typeof sanitized !== "object" || Array.isArray(sanitized)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return sanitized as Record<string, unknown>;
}

function asOptionalWorkflowCommandPublicMirrorObject(
  value: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): JsonObject | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  const sanitized = sanitizeWorkflowCommandPublicMirrorValue(value, false, traversal, depth);
  return sanitized && typeof sanitized === "object" && !Array.isArray(sanitized)
    ? (sanitized as JsonObject)
    : undefined;
}

function asOptionalWorkflowCommandStringArray(value: unknown): string[] | undefined {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : undefined;
}

function asOptionalWorkflowCommandJsonArray(
  value: unknown,
  traversal: WorkflowPublicProjectionTraversal = createWorkflowPublicProjectionTraversal(),
  depth = 0,
): JsonValue[] | undefined {
  const sanitized = sanitizeWorkflowCommandPublicMirrorValue(value, false, traversal, depth);
  return Array.isArray(sanitized) ? sanitized : undefined;
}

function asOptionalNonnegativeSafeInteger(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    return undefined;
  }
  return value === 0 ? 0 : value;
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

function asAllowedPublicResponseStatus<const T extends readonly string[]>(
  value: unknown,
  allowedStatuses: T,
  label: string,
): T[number] {
  const status = asString(value);
  if (!allowedStatuses.some((allowedStatus) => allowedStatus === status)) {
    throw new Error(`${label} has unsupported status: ${status}`);
  }
  return status as T[number];
}

function mapPublicResponseForStatuses<
  TResponse extends { status: string },
  const TStatuses extends readonly string[],
>(
  payload: unknown,
  mapper: (value: unknown) => TResponse,
  allowedStatuses: TStatuses,
  label: string,
): PublicResponseForStatuses<TResponse, TStatuses> {
  const response = mapper(payload);
  asAllowedPublicResponseStatus(response.status, allowedStatuses, label);
  return response as PublicResponseForStatuses<TResponse, TStatuses>;
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

function asSparseOptionalStringArray(value: unknown): string[] | undefined {
  return value === undefined ? undefined : asOptionalStringArray(value);
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

function asSparseRecordOfNumber(value: unknown): Record<string, number> | undefined {
  return value === undefined ? undefined : asRecordOfNumber(value);
}

function asRecordOfMappedObject<T extends JsonObject>(
  value: unknown,
  mapper: (value: unknown) => T,
): Record<string, T> {
  const source = value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
  const result: Record<string, T> = {};
  for (const [key, item] of Object.entries(source)) {
    try {
      result[key] = mapper(item);
    } catch {
      continue;
    }
  }
  return result;
}
