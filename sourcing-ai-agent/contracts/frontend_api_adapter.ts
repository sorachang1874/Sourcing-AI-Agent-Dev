import type {
  InstructionCompiler,
  IntentBrief,
  IntentRewritePolicyCatalogEntry,
  IntentRewriteEntry,
  IntentRewritePayload,
  JsonObject,
  JobProgressResponse,
  JobResultsResponse,
  JobRuntimeHealth,
  MatchResult,
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
  WorkflowExplainResponse,
  WorkflowStartResponse,
  WorkflowStageSummariesPayload,
  WorkflowStageSummaryItem,
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
