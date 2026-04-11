import { mockCandidateDetails, mockDashboard, mockManualReviewItems, mockPlan, mockRunStatus } from "../data/mockData";
import type {
  Candidate,
  CandidateDetail,
  CandidateConfidence,
  CandidateExternalLink,
  DashboardData,
  DemoPlan,
  ManualReviewItem,
  RunStatusData,
} from "../types";

const runningLocally =
  typeof window !== "undefined" &&
  ["localhost", "127.0.0.1"].includes(window.location.hostname);
const useMock = import.meta.env.VITE_USE_MOCK === "true";
const preferLocalAssets = useMock && import.meta.env.VITE_USE_LOCAL_ASSETS !== "false";
const DEFAULT_API_TIMEOUT_MS = 30_000;
const PLAN_API_TIMEOUT_MS = 90_000;
const WORKFLOW_START_TIMEOUT_MS = 60_000;
const API_BASE_URL_STORAGE_KEY = "sourcing-ai-agent-demo-api-base-url";
const DEFAULT_RECALL_LIMITS = {
  top_k: 30,
  slug_resolution_limit: 20,
  profile_detail_limit: 20,
  publication_scan_limit: 20,
  publication_lead_limit: 30,
  exploration_limit: 20,
  semantic_rerank_limit: 30,
};
const FORCE_FRESH_EXECUTION_PREFERENCES = {
  force_fresh_run: true,
  allow_local_bootstrap_fallback: false,
};

function normalizeApiBaseUrl(value: string): string {
  return value.trim().replace(/\/+$/, "");
}

function readApiBaseUrlFromQuery(): string {
  if (typeof window === "undefined") {
    return "";
  }
  const searchParams = new URLSearchParams(window.location.search);
  return normalizeApiBaseUrl(
    searchParams.get("api_base_url") ||
      searchParams.get("apiBaseUrl") ||
      searchParams.get("backend") ||
      "",
  );
}

export function readRuntimeApiBaseUrl(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    return normalizeApiBaseUrl(window.localStorage.getItem(API_BASE_URL_STORAGE_KEY) || "");
  } catch {
    return "";
  }
}

export function saveRuntimeApiBaseUrl(value: string): string {
  const normalized = normalizeApiBaseUrl(value);
  if (typeof window === "undefined") {
    return normalized;
  }
  try {
    if (normalized) {
      window.localStorage.setItem(API_BASE_URL_STORAGE_KEY, normalized);
    } else {
      window.localStorage.removeItem(API_BASE_URL_STORAGE_KEY);
    }
  } catch {
    return normalized;
  }
  return normalized;
}

export function clearRuntimeApiBaseUrl(): void {
  saveRuntimeApiBaseUrl("");
}

export function isApiBaseUrlRuntimeConfigurable(): boolean {
  return !normalizeApiBaseUrl(import.meta.env.VITE_API_BASE_URL || "");
}

export function getConfiguredApiBaseUrl(): string {
  const envBaseUrl = normalizeApiBaseUrl(import.meta.env.VITE_API_BASE_URL || "");
  if (envBaseUrl) {
    return envBaseUrl;
  }
  const queryBaseUrl = readApiBaseUrlFromQuery();
  if (queryBaseUrl) {
    saveRuntimeApiBaseUrl(queryBaseUrl);
    return queryBaseUrl;
  }
  const storedBaseUrl = readRuntimeApiBaseUrl();
  if (storedBaseUrl) {
    return storedBaseUrl;
  }
  if (runningLocally) {
    return "http://127.0.0.1:8765";
  }
  return "";
}

async function fetchJson<T>(path: string, options?: RequestInit, timeoutMs = DEFAULT_API_TIMEOUT_MS): Promise<T> {
  const apiBaseUrl = getConfiguredApiBaseUrl();
  if (!apiBaseUrl) {
    throw new Error("API base URL is not configured. Please set a backend URL in the page before starting a search.");
  }
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  const requestHeaders = new Headers(options?.headers || {});
  if (!requestHeaders.has("Content-Type")) {
    requestHeaders.set("Content-Type", "application/json");
  }
  try {
    const response = await fetch(`${apiBaseUrl}${path}`, {
      ...options,
      headers: requestHeaders,
      signal: controller.signal,
    });
    if (!response.ok) {
      const detail = (await response.text()).trim();
      throw new Error(detail ? `Request failed: ${response.status} ${detail.slice(0, 240)}` : `Request failed: ${response.status}`);
    }
    return (await response.json()) as T;
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s.`);
    }
    if (
      error instanceof TypeError &&
      /localhost:8765|127\.0\.0\.1:8765/.test(apiBaseUrl)
    ) {
      throw new Error("Local backend is unreachable at http://127.0.0.1:8765. Please make sure the local service is running.");
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchPublicJson<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Public asset request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

async function fetchPublicJsonOptional<T>(path: string, fallback: T): Promise<T> {
  try {
    return await fetchPublicJson<T>(path);
  } catch {
    return fallback;
  }
}

function extractCandidateArray(payload: unknown): unknown[] {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (payload && typeof payload === "object") {
    const record = payload as Record<string, unknown>;
    if (Array.isArray(record.candidates)) {
      return record.candidates;
    }
  }
  return [];
}

function stripMarkdown(value: string): string {
  return value
    .replace(/[*_`>#-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function splitStructuredText(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => asString(item)).filter(Boolean);
  }
  const text = asString(value);
  if (!text) {
    return [];
  }
  const segments = text
    .split(/\n+|[•；;]+|\s\|\s|,(?=\s*[A-Za-z\u4e00-\u9fff])/)
    .map((item) => item.trim())
    .filter(Boolean);
  return segments.length > 1 ? segments : [text];
}

function mapSearchBundlesToLabels(payload: any): string[] {
  return asArray(payload?.plan?.search_strategy?.query_bundles)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .map((item) =>
      firstNonEmptyString([
        pickFirstString(item, ["objective"]),
        pickFirstString(item, ["source_family"]),
        pickFirstString(item, ["execution_mode"]),
        pickFirstString(item, ["bundle_id"]),
      ]),
    )
    .filter(Boolean);
}

function dedupeStrings(values: string[]): string[] {
  return values.filter((value, index) => value && values.indexOf(value) === index);
}

function containsPattern(value: string, patterns: RegExp[]): boolean {
  return patterns.some((pattern) => pattern.test(value));
}

function extractScopeLabels(queryText: string): string[] {
  const labels = Array.from(queryText.matchAll(/([A-Za-z0-9\u4e00-\u9fff][A-Za-z0-9\u4e00-\u9fff +/&._-]{1,40}?)\s*(?:组|团队|team)\b/gi))
    .map((match) => asString(match[1]))
    .map((value) => value.replace(/[、,，/]+$/g, "").trim())
    .filter(Boolean);
  return dedupeStrings(labels).map((label) => (/(组|团队)$/u.test(label) ? label : `${label} 组`));
}

function detectFocusLabels(queryText: string, payload: any): string[] {
  const corpus = [
    queryText,
    pickFirstString((payload.request as Record<string, unknown>) || {}, ["query", "raw_user_request"]),
    pickFirstString((payload.plan as Record<string, unknown>) || {}, ["criteria_summary", "intent_summary"]),
  ]
    .filter(Boolean)
    .join(" ");
  const rules = [
    { label: "多模态", patterns: [/multimodal/i, /多模态/u] },
    { label: "视频生成", patterns: [/video generation/i, /视频生成/u, /\bveo\b/i] },
    { label: "模型优化", patterns: [/model optimization/i, /模型优化/u] },
    { label: "Agent", patterns: [/\bagent\b/i, /智能体/u] },
    { label: "Applied AI", patterns: [/applied ai/i, /应用型?\s*AI/ui] },
    { label: "研究工程", patterns: [/research engineer/i, /研究工程/u] },
  ];
  return rules.filter((rule) => containsPattern(corpus, rule.patterns)).map((rule) => rule.label);
}

function inferPopulationLabel(queryText: string, payload: any): string {
  const corpus = [
    queryText,
    pickFirstString((payload.request as Record<string, unknown>) || {}, ["query", "raw_user_request"]),
    pickFirstString((payload.plan as Record<string, unknown>) || {}, ["target_population", "intent_summary"]),
  ]
    .filter(Boolean)
    .join(" ");
  const focusLabels = detectFocusLabels(queryText, payload);
  const focusPrefix = focusLabels.length > 0 ? `${focusLabels.slice(0, 2).join(" / ")}方向` : "";
  const backgroundLabel = containsPattern(corpus, [/华人/u, /\bChinese\b/i, /中文/u]) ? "华人" : "";
  const roleLabel =
    containsPattern(corpus, [/研究员/u, /researcher/i]) && containsPattern(corpus, [/工程师/u, /engineer/i])
      ? "研究员/工程师"
      : containsPattern(corpus, [/research engineer/i, /研究工程/u])
        ? "研究工程师"
        : containsPattern(corpus, [/工程师/u, /engineer/i])
          ? "工程师"
          : containsPattern(corpus, [/研究员/u, /researcher/i])
            ? "研究员"
            : "候选人";
  return `${focusPrefix}${backgroundLabel}${roleLabel}` || "目标候选人";
}

function inferProjectScopeLabel(queryText: string, payload: any): string {
  const scopeLabels = extractScopeLabels(queryText);
  if (scopeLabels.length > 0) {
    return scopeLabels.join("、");
  }
  const acquisitionStrategy = (payload.plan?.acquisition_strategy as Record<string, unknown>) || {};
  const strategyType = pickFirstString(acquisitionStrategy, ["strategy_type"]);
  const targetScope = pickFirstString((payload.plan as Record<string, unknown>) || {}, ["target_scope"]);
  if (strategyType === "full_company_roster" || targetScope === "full_company_asset") {
    return "目标公司全量范围";
  }
  if (containsPattern(queryText, [/在职/u, /current/i])) {
    return "在职员工优先范围";
  }
  return "目标团队定向范围";
}

function inferKeywordLabels(queryText: string, payload: any): string[] {
  const corpus = [
    queryText,
    pickFirstString((payload.request as Record<string, unknown>) || {}, ["query", "raw_user_request"]),
  ]
    .filter(Boolean)
    .join(" ");
  const keywords: string[] = [];
  if (containsPattern(corpus, [/multimodal/i, /多模态/u])) {
    keywords.push(/multimodal/i.test(corpus) ? "multimodal" : "多模态");
  }
  if (containsPattern(corpus, [/video generation/i, /视频生成/u, /\bveo\b/i])) {
    keywords.push(/video generation/i.test(corpus) ? "video generation" : "视频生成");
  }
  if (containsPattern(corpus, [/model optimization/i, /模型优化/u])) {
    keywords.push(/model optimization/i.test(corpus) ? "model optimization" : "模型优化");
  }
  if (containsPattern(corpus, [/\bagent\b/i, /智能体/u])) {
    keywords.push(/\bagent\b/i.test(corpus) ? "agent" : "智能体");
  }
  if (containsPattern(corpus, [/华人/u, /\bChinese\b/i])) {
    keywords.push("华人");
  }
  if (containsPattern(corpus, [/中文/u, /bilingual/i])) {
    keywords.push("中文");
  }
  const fallbackKeywords = asArray(payload.plan?.keywords)
    .map((value) => asString(value))
    .map((value) => {
      if (value === "Greater China experience") {
        return "华人";
      }
      if (value === "Chinese bilingual outreach") {
        return "中文";
      }
      return value;
    })
    .filter(Boolean);
  return dedupeStrings([...keywords, ...fallbackKeywords]).slice(0, 6);
}

function translateSearchChannelLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  const mappings: Record<string, string> = {
    general_web_search_relation_check: "公开网页关系验证",
    targeted_linkedin_web_search: "LinkedIn 定向搜索",
    provider_people_search_api: "人员搜索 API",
    profile_detail_api: "Profile 详情抓取",
    relationship_web: "公开网页关系验证",
    publication_surface: "论文 / 作者线索检索",
    targeted_people_search: "定向人物搜索",
  };
  return mappings[normalized] || value;
}

function inferStrategyLabel(queryText: string, payload: any): string {
  const acquisitionStrategy = (payload.plan?.acquisition_strategy as Record<string, unknown>) || {};
  const strategyType = pickFirstString(acquisitionStrategy, ["strategy_type"]);
  if (strategyType === "full_company_roster" || Boolean(payload.plan_review_gate?.required_before_execution)) {
    return "全公司扫描 + 定向检索";
  }
  if (containsPattern(queryText, [/publication/i, /论文/u, /scholar/i])) {
    return "定向检索 + 论文证据补强";
  }
  return "定向检索 + 多源证据补强";
}

function mapPlanPayloadToDemoPlan(payload: any, queryText: string): DemoPlan {
  const acquisitionStrategy = (payload.plan?.acquisition_strategy as Record<string, unknown>) || {};
  const searchStrategy = (payload.plan?.search_strategy as Record<string, unknown>) || {};
  const searchStrategyLabels = mapSearchBundlesToLabels(payload);
  const fallbackSearchChannels = asArray(acquisitionStrategy.search_channel_order)
    .map((value) => asString(value))
    .filter(Boolean);
  const configuredSearchStrategy =
    searchStrategyLabels.length > 0
      ? searchStrategyLabels
      : fallbackSearchChannels.length > 0
        ? fallbackSearchChannels
        : asArray(payload.plan?.intent_brief?.default_execution_strategy).map((value) => asString(value)).filter(Boolean);
  return {
    planId: String(payload.plan_review_session?.review_id || crypto.randomUUID()),
    rawUserRequest: payload.request?.raw_user_request || queryText,
    targetCompany: payload.plan?.target_company || payload.request?.target_company || "待确认公司",
    targetPopulation: inferPopulationLabel(queryText, payload),
    projectScope: inferProjectScopeLabel(queryText, payload),
    keywords: inferKeywordLabels(queryText, payload),
    acquisitionStrategy: inferStrategyLabel(queryText, payload),
    searchStrategy: configuredSearchStrategy.map((value) => translateSearchChannelLabel(value)),
    estimatedCostLevel: payload.plan_review_gate?.required_before_execution ? "high" : "medium",
    reviewRequired: Boolean(payload.plan_review_gate?.required_before_execution),
    status: payload.plan_review_session?.status === "pending" ? "pending_review" : "draft",
  };
}

export async function getPlan(queryText = ""): Promise<DemoPlan> {
  const payload = await fetchJson<any>("/api/plan", {
    method: "POST",
    body: JSON.stringify({
      raw_user_request: queryText,
      planning_mode: "model_assisted",
      execution_preferences: FORCE_FRESH_EXECUTION_PREFERENCES,
      ...DEFAULT_RECALL_LIMITS,
    }),
  }, PLAN_API_TIMEOUT_MS);
  return mapPlanPayloadToDemoPlan(payload, queryText);
}

export async function getPlanEnvelope(queryText: string): Promise<{ plan: DemoPlan; reviewId: string; raw: any }> {
  const payload = await fetchJson<any>("/api/plan", {
    method: "POST",
    body: JSON.stringify({
      raw_user_request: queryText,
      planning_mode: "model_assisted",
      execution_preferences: FORCE_FRESH_EXECUTION_PREFERENCES,
      ...DEFAULT_RECALL_LIMITS,
    }),
  }, PLAN_API_TIMEOUT_MS);
  return {
    plan: mapPlanPayloadToDemoPlan(payload, queryText),
    reviewId: String(payload.plan_review_session?.review_id || ""),
    raw: payload,
  };
}

export async function approvePlanReview(reviewId: string): Promise<any> {
  return fetchJson<any>("/api/plan/review", {
    method: "POST",
    body: JSON.stringify({
      review_id: Number(reviewId),
      action: "approved",
      reviewer: "frontend-demo",
      decision: {
        force_fresh_run: true,
        allow_local_bootstrap_fallback: false,
      },
    }),
  });
}

export async function startWorkflowRun(reviewId: string): Promise<{ jobId: string; raw: any }> {
  const payload = await fetchJson<any>("/api/workflows", {
    method: "POST",
    body: JSON.stringify({
      plan_review_id: Number(reviewId),
    }),
  }, WORKFLOW_START_TIMEOUT_MS);
  return {
    jobId: String(payload.job_id || ""),
    raw: payload,
  };
}

function mapProgressPayloadToRunStatus(payload: any): RunStatusData {
  const milestones = asArray(payload.progress?.milestones);
  const progressEvents = asArray(payload.progress?.events);
  const workerSummary = (payload.progress?.worker_summary as Record<string, unknown>) || {};
  const laneSummaries = asArray(workerSummary.by_lane).map((item) => (item as Record<string, unknown>) || {});
  const rawOverallStatus = pickFirstString(payload, ["status"]) || "running";
  const overallStatus: RunStatusData["status"] =
    rawOverallStatus === "completed" ||
    rawOverallStatus === "running" ||
    rawOverallStatus === "queued" ||
    rawOverallStatus === "blocked" ||
    rawOverallStatus === "failed"
      ? rawOverallStatus
      : "running";
  const completedAtFallback = pickFirstString(payload, ["updated_at"]);

  const normalizeEventStatus = (
    status: string,
    stage: string,
  ): RunStatusData["timeline"][number]["status"] => {
    const normalizedStatus =
      overallStatus === "completed" && status === "running" && stage !== "completed"
        ? "completed"
        : status || "running";
    if (
      normalizedStatus === "completed" ||
      normalizedStatus === "running" ||
      normalizedStatus === "queued" ||
      normalizedStatus === "blocked" ||
      normalizedStatus === "failed"
    ) {
      return normalizedStatus;
    }
    if (overallStatus === "failed") {
      return "failed";
    }
    if (overallStatus === "completed") {
      return "completed";
    }
    return "running";
  };

  const normalizeCompletedAt = (status: string, completedAt: string): string => {
    if (completedAt) {
      return completedAt;
    }
    if (overallStatus === "completed" && status === "completed") {
      return completedAtFallback;
    }
    return "";
  };

  return {
    jobId: String(payload.job_id || ""),
    status: overallStatus,
    currentStage: payload.stage || payload.progress?.current_stage || "Workflow",
    startedAt: payload.started_at || payload.updated_at || "unknown",
    metrics: [
      { label: "Candidates", value: String(payload.progress?.counters?.candidate_count || payload.progress?.counters?.results_count || 0) },
      { label: "Evidence", value: String(payload.progress?.counters?.evidence_count || 0) },
      { label: "Manual Review", value: String(payload.progress?.counters?.manual_review_count || 0) },
      { label: "Workers", value: String(Object.values((workerSummary.by_status as Record<string, number>) || {}).reduce((sum, value) => sum + Number(value || 0), 0)) },
    ],
    timeline:
      progressEvents.length > 0
        ? progressEvents.map((item, index) => {
            const event = item as Record<string, unknown>;
            const stage = pickFirstString(event, ["stage"]) || `Stage ${index + 1}`;
            const status = normalizeEventStatus(pickFirstString(event, ["status"]) || "running", stage);
            return {
              id: pickFirstString(event, ["id"]) || String(index + 1),
              stage,
              title: pickFirstString(event, ["title"]) || stage,
              detail: pickFirstString(event, ["detail"]) || pickFirstString(payload, ["current_message"]) || "Workflow in progress.",
              status,
              startedAt: pickFirstString(event, ["started_at"]) || "",
              completedAt: normalizeCompletedAt(status, pickFirstString(event, ["completed_at"]) || ""),
              sourceTags: asArray(event.source_tags).map((tag) => {
                const record = (tag as Record<string, unknown>) || {};
                return {
                  label: pickFirstString(record, ["label"]) || "source",
                  count: typeof record.count === "number" ? Number(record.count) : undefined,
                };
              }),
            };
          })
        : milestones.map((item, index) => {
            const milestone = item as Record<string, unknown>;
            const stage = pickFirstString(milestone, ["stage"]) || `Stage ${index + 1}`;
            const status = normalizeEventStatus(pickFirstString(milestone, ["status"]) || "running", stage);
            return {
              id: String(index + 1),
              stage,
              title: stage,
              detail: pickFirstString(milestone, ["latest_detail"]) || pickFirstString(payload, ["current_message"]) || "Workflow in progress.",
              status,
              startedAt: pickFirstString(milestone, ["started_at"]) || "",
              completedAt: normalizeCompletedAt(status, pickFirstString(milestone, ["completed_at"]) || ""),
              sourceTags: [],
            };
          }),
    workers: laneSummaries.map((item, index) => ({
      id: String(index + 1),
      lane: pickFirstString(item, ["lane_id"]) || `lane_${index + 1}`,
      status: Object.entries((item.by_status as Record<string, number>) || {}).find(([, value]) => Number(value) > 0)?.[0] || "idle",
      budget: `${pickFirstString(item, ["worker_count"]) || String(item.worker_count || 0)} workers`,
    })),
  };
}

export async function getRunStatus(jobId?: string): Promise<RunStatusData> {
  if (!jobId) {
    throw new Error("Missing job_id. Real-time workflow progress requires a valid backend job.");
  }
  try {
    const payload = await fetchJson<any>(`/api/jobs/${jobId}/progress`);
    return mapProgressPayloadToRunStatus(payload);
  } catch {
    const fallbackPayload = await fetchJson<any>(`/api/jobs/${jobId}`);
      return {
        jobId: fallbackPayload.job_id || jobId,
        status: fallbackPayload.status || "running",
        currentStage: fallbackPayload.stage || "Workflow",
        startedAt: fallbackPayload.created_at || fallbackPayload.updated_at || "unknown",
      metrics: [
        { label: "Candidates", value: String(fallbackPayload.summary?.candidate_count || 0) },
        { label: "Evidence", value: String(fallbackPayload.summary?.evidence_count || 0) },
        { label: "Manual Review", value: String(fallbackPayload.summary?.manual_review_queue_count || 0) },
        { label: "Workers", value: "0" },
      ],
        timeline: [],
        workers: [],
      };
    }
  }

function mapJobResultsToDashboard(payload: any): DashboardData {
  const resultRecords = asArray(payload.results)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .filter((item) => Object.keys(item).length > 0);
  const candidates = resultRecords.map((record) => deriveCandidate(record));
  const groups = Array.from(new Set(["All", ...candidates.map((candidate) => candidate.team || "Unknown")]));
  const high = candidates.filter((candidate) => candidate.confidence === "high").length;
  const leadOnly = candidates.filter((candidate) => candidate.confidence === "lead_only").length;
  const confirmed = candidates.filter((candidate) => candidate.confidence !== "lead_only").length;
  return {
    title: payload.job?.request?.raw_user_request || payload.job?.request?.query || "Sourcing results",
    snapshotId: payload.job?.summary?.snapshot_id || payload.job?.job_id || "live",
    queryLabel: payload.job?.request?.raw_user_request || payload.job?.request?.query || "Query",
    totalCandidates: candidates.length,
    totalEvidence: resultRecords.reduce((sum, record) => sum + asArray(record.evidence).length, 0),
    manualReviewCount: asArray(payload.manual_review_items).length,
    layers: [
      { id: "all", label: "Layer 0 · All Candidates", count: candidates.length },
      { id: "lead_only", label: "Layer 1 · Lead / Low Confidence", count: leadOnly },
      { id: "high", label: "Layer 2 · High Confidence", count: high },
      { id: "confirmed", label: "Layer 3 · Confirmed Direction", count: confirmed },
    ],
    groups,
    candidates,
  };
}

export async function getDashboard(jobId?: string): Promise<DashboardData> {
  if (!jobId) {
    throw new Error("Missing job_id. Real search results require a completed backend job.");
  }
  const payload = await fetchJson<any>(`/api/jobs/${jobId}/results`);
  return mapJobResultsToDashboard(payload);
}

export async function getManualReviewItems(): Promise<ManualReviewItem[]> {
  if (preferLocalAssets) {
    const localItems = await getManualReviewItemsFromLocalAssets();
    if (localItems) {
      return localItems;
    }
  }
  if (useMock) {
    return mockManualReviewItems;
  }
  const payload = await fetchJson<any>("/api/manual-review");
  const items = Array.isArray(payload.manual_review_items) ? payload.manual_review_items : [];
  return items.map((item: Record<string, unknown>) => ({
    id: pickFirstString(item, ["review_item_id", "id"]) || crypto.randomUUID(),
    candidateId: pickFirstString(item, ["candidate_id"]),
    candidateName: pickFirstString(item, ["candidate_name", "display_name"]) || "Unknown candidate",
    reviewType: pickFirstString(item, ["review_type"]) || "manual_review",
    status: (pickFirstString(item, ["status"]) as "pending" | "resolved" | "rejected") || "pending",
    summary: pickFirstString(item, ["summary", "reason"]) || "Manual review item recovered from API.",
    recommendedAction: pickFirstString(item, ["recommended_action", "notes"]) || "Review source evidence and confirm resolution.",
    evidenceLabels: asArray(item.evidence_labels).map((value) => asString(value)).filter(Boolean),
  }));
}

export async function getCandidateDetail(candidateId: string, jobId: string): Promise<CandidateDetail | null> {
  if (!jobId) {
    throw new Error("Missing job_id. Candidate detail requests must be scoped to a workflow result set.");
  }
  const payload = await fetchJson<any>(`/api/jobs/${jobId}/candidates/${encodeURIComponent(candidateId)}`);
  const candidateRecord = {
    ...((payload.candidate && typeof payload.candidate === "object" ? payload.candidate : {}) as Record<string, unknown>),
    evidence: asArray(payload.evidence),
  };
  return deriveCandidateDetail(candidateRecord);
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asString(value: unknown): string {
  if (typeof value !== "string") {
    return "";
  }
  const trimmed = value.trim();
  if (!trimmed || trimmed.toLowerCase() === "none" || trimmed.toLowerCase() === "null") {
    return "";
  }
  return trimmed;
}

function pickFirstString(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value;
    }
  }
  return "";
}

function firstNonEmptyString(values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value;
    }
  }
  return "";
}

function toConfidence(value: string): CandidateConfidence {
  if (value === "high" || value === "medium" || value === "lead_only") {
    return value;
  }
  return "lead_only";
}

function titleCase(value: string): string {
  return value
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function firstLine(value: string): string {
  return value.split(/\s*\|\s*|\n+/).map((item) => item.trim()).find(Boolean) || value;
}

function normalizeTeam(value: string): string {
  return value || "未分组";
}

function normalizeDatasetLabel(value: string): string {
  if (!value) {
    return "未标注来源";
  }
  return titleCase(value.replace(/^harvest_/, "").replace(/^company_/, ""));
}

function pickLinkedinUrlFromUrls(value: unknown): string {
  for (const item of asArray(value)) {
    const text = asString(item);
    if (text.includes("linkedin.com/in/")) {
      return text;
    }
  }
  return "";
}

function pickProfilePhoto(profilePhotoMap: Record<string, string>, candidateId: string): string {
  return asString(profilePhotoMap[candidateId] || "");
}

function toExternalLinkType(value: string): CandidateExternalLink["type"] {
  if (value === "email" || value === "github" || value === "twitter" || value === "scholar" || value === "website") {
    return value;
  }
  return "website";
}

function normalizeExternalLinks(payload: unknown): CandidateExternalLink[] {
  return asArray(payload)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .map((item) => ({
      label: pickFirstString(item, ["label"]) || "External",
      url: pickFirstString(item, ["url"]),
      type: toExternalLinkType(pickFirstString(item, ["type"])),
    }))
    .filter((item) => Boolean(item.url));
}

function mergeExternalLinks(
  candidate: Candidate,
  contactLinkMap: Record<string, unknown>,
  candidateId: string,
): Candidate {
  const externalLinks = normalizeExternalLinks(contactLinkMap[candidateId]);
  if (externalLinks.length === 0) {
    return candidate;
  }
  return {
    ...candidate,
    externalLinks,
  };
}

function buildLinkedinSearchUrl(name: string): string {
  const keyword = encodeURIComponent(name || "linkedin profile");
  return `https://www.linkedin.com/search/results/all/?keywords=${keyword}`;
}

function resolveLinkedinUrl(primary: string, fallbackName: string): string {
  return asString(primary) || buildLinkedinSearchUrl(fallbackName);
}

function sourcePathToPublicProfilePath(sourcePath: string): string {
  const filename = sourcePath.split("/").pop() || "";
  return filename ? `/tml/profiles/${filename}` : "";
}

function pickProfileItem(payload: unknown): Record<string, unknown> {
  if (payload && typeof payload === "object" && !Array.isArray(payload)) {
    const record = payload as Record<string, unknown>;
    if (record.item && typeof record.item === "object" && !Array.isArray(record.item)) {
      return record.item as Record<string, unknown>;
    }
    return record;
  }
  if (Array.isArray(payload)) {
    for (const entry of payload) {
      if (entry && typeof entry === "object" && !Array.isArray(entry)) {
        const item = (entry as Record<string, unknown>).item;
        if (item && typeof item === "object" && !Array.isArray(item)) {
          return item as Record<string, unknown>;
        }
        return entry as Record<string, unknown>;
      }
    }
  }
  return {};
}

function joinFragments(parts: Array<string | undefined>): string {
  return parts.map((part) => asString(part || "")).filter(Boolean).join(" · ");
}

function profileEducationLines(profileItem: Record<string, unknown>): string[] {
  const education = asArray(profileItem.educations).map((entry) => (entry as Record<string, unknown>) || {});
  const lines = education
    .map((entry) =>
      joinFragments([
        pickFirstString(entry, ["schoolName"]),
        pickFirstString(entry, ["degreeName"]),
        pickFirstString(entry, ["fieldOfStudy"]),
      ]),
    )
    .filter(Boolean);
  if (lines.length > 0) {
    return lines;
  }
  const topEducation = asArray(profileItem.profileTopEducation).map(
    (entry) => ((entry && typeof entry === "object" ? entry : {}) as Record<string, unknown>),
  );
  return topEducation.map((entry) => pickFirstString(entry, ["schoolName"])).filter(Boolean);
}

function profileExperienceLines(profileItem: Record<string, unknown>): string[] {
  return asArray(profileItem.experience)
    .map((entry) => ((entry && typeof entry === "object" ? entry : {}) as Record<string, unknown>))
    .map((entry) =>
      joinFragments([
        pickFirstString(entry, ["position"]),
        pickFirstString(entry, ["companyName"]),
        pickFirstString(entry, ["duration"]),
      ]),
    )
    .filter(Boolean);
}

function profileFocusAreas(profileItem: Record<string, unknown>): string[] {
  const topSkills = pickFirstString(profileItem, ["topSkills"]);
  if (!topSkills) {
    return [];
  }
  return topSkills
    .split(/[•,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildNarrativeSummary(
  name: string,
  profileItem: Record<string, unknown>,
  fallback: string,
  currentCompany?: string,
): string {
  const headline = pickFirstString(profileItem, ["headline"]);
  const about = pickFirstString(profileItem, ["about"]);
  const location = joinFragments([
    pickFirstString((profileItem.location as Record<string, unknown>) || {}, ["linkedinText"]),
    pickFirstString(profileItem, ["locationName"]),
  ]);
  const currentPosition = ((asArray(profileItem.currentPosition)[0] as Record<string, unknown>) || {});
  const positionTitle = pickFirstString(currentPosition, ["title", "position"]);
  const positionCompany = pickFirstString(currentPosition, ["companyName"]) || currentCompany || "";
  const experienceLines = profileExperienceLines(profileItem).slice(0, 2);
  const educationLines = profileEducationLines(profileItem).slice(0, 1);
  const focusAreas = profileFocusAreas(profileItem).slice(0, 4);

  const intro = joinFragments([
    name ? `${name}` : "",
    headline || positionTitle || "候选人",
    positionCompany ? `目前在 ${positionCompany}` : "",
    location ? `常驻 ${location}` : "",
  ]);

  const bodyParts = [
    intro ? `${intro}。` : "",
    about ? `${about}。` : "",
    experienceLines.length > 0 ? `近期经历包括：${experienceLines.join("；")}。` : "",
    educationLines.length > 0 ? `教育背景方面：${educationLines[0]}。` : "",
    focusAreas.length > 0 ? `技能与关注方向主要集中在 ${focusAreas.join("、")}。` : "",
  ].filter(Boolean);

  if (bodyParts.length > 0) {
    return bodyParts.join(" ");
  }
  return fallback;
}

function deriveCandidate(record: Record<string, unknown>): Candidate {
  const metadata = (record.metadata as Record<string, unknown>) || {};
  const evidenceList = asArray(record.evidence ?? metadata.evidence).map((item, index) => {
    const source = (item as Record<string, unknown>) || {};
    const url = pickFirstString(source, ["url", "source_url", "profile_url", "linkedin_url"]);
    return {
      label: pickFirstString(source, ["label", "title", "source_type"]) || `Evidence ${index + 1}`,
      type:
        (pickFirstString(source, ["type", "source_type"]) as
          | "linkedin"
          | "publication"
          | "homepage"
          | "github"
          | "cv") || "homepage",
      url: url || "#",
      excerpt:
        pickFirstString(source, ["excerpt", "summary", "reason", "snippet"]) ||
        "Recovered from backend evidence artifact.",
    };
  });

  const focusAreas = [
    ...splitStructuredText(record.focus_areas),
    ...splitStructuredText(metadata.focus_areas),
    ...splitStructuredText(record.tags),
  ]
    .map((value) => asString(value))
    .filter(Boolean);

  const team =
    pickFirstString(record, ["team", "group_name", "group"]) ||
    pickFirstString(metadata, ["team", "project_scope", "suspected_group"]) ||
    "Unknown";

  const employmentStatus =
    pickFirstString(record, ["employment_status", "status"]) ||
    pickFirstString(metadata, ["employment_status"]) ||
    "lead";

  const matchReasons = [
    ...asArray(record.match_reasons),
    ...asArray(record.confidence_reason ? [record.confidence_reason] : []),
    ...asArray(metadata.match_reasons),
    ...asArray(record.matched_fields).map((item) => {
      const source = ((item && typeof item === "object" ? item : {}) as Record<string, unknown>);
      return joinFragments([
        pickFirstString(source, ["field"]),
        pickFirstString(source, ["matched_on", "value"]),
      ]);
    }),
  ]
    .map((value) => asString(value))
    .filter(Boolean);

  return {
    id:
      pickFirstString(record, ["candidate_id", "id"]) ||
      pickFirstString(metadata, ["candidate_id"]) ||
      crypto.randomUUID(),
    name:
      pickFirstString(record, ["display_name", "name_en", "full_name", "name"]) ||
      pickFirstString(metadata, ["display_name"]) ||
      "Unknown Candidate",
    headline:
      pickFirstString(record, ["headline", "role", "title"]) ||
      pickFirstString(metadata, ["headline", "current_role"]) ||
      "Candidate profile",
    avatarUrl:
      pickFirstString(record, ["avatar_url", "photo_url"]) ||
      pickFirstString(metadata, ["avatar_url"]),
    team: normalizeTeam(team),
    employmentStatus:
      employmentStatus === "current" || employmentStatus === "former" ? employmentStatus : "lead",
    confidence: toConfidence(pickFirstString(record, ["confidence_label"]) || pickFirstString(metadata, ["confidence_label"])),
    summary:
      pickFirstString(record, ["summary", "explanation", "notes"]) ||
      pickFirstString(metadata, ["summary", "bio"]) ||
      "Recovered from normalized backend candidate artifacts.",
    currentCompany:
      pickFirstString(record, ["current_company", "organization"]) ||
      pickFirstString(metadata, ["current_company", "organization"]),
    location:
      pickFirstString(record, ["location"]) ||
      pickFirstString(metadata, ["location"]),
    linkedinUrl:
      pickFirstString(record, ["linkedin_url"]) ||
      pickFirstString(metadata, ["linkedin_url", "profile_url"]),
    sourceDataset:
      normalizeDatasetLabel(
        pickFirstString(record, ["source_dataset"]) || pickFirstString(metadata, ["source_dataset"]),
      ),
    notesSnippet: firstLine(pickFirstString(record, ["notes"]) || pickFirstString(metadata, ["notes"])),
    focusAreas,
    matchReasons,
    education: splitStructuredText(record.education ?? metadata.education)
      .map((value) => asString(value))
      .filter(Boolean),
    experience: splitStructuredText(record.work_history ?? record.experience ?? metadata.experience)
      .map((value) => asString(value))
      .filter(Boolean),
    evidence: evidenceList,
  };
}

function deriveCandidateFromNormalizedRecord(
  record: Record<string, unknown>,
  materializedRecord: Record<string, unknown> | null,
  profilePhotoMap: Record<string, string> = {},
  profileSummaryMap: Record<string, Record<string, unknown>> = {},
  contactLinkMap: Record<string, unknown> = {},
): Candidate {
  const materialized = materializedRecord || {};
  const base = deriveCandidate(record);
  const candidateId = pickFirstString(record, ["candidate_id", "id"]) || base.id;
  const profileSummary = (profileSummaryMap[candidateId] || {}) as Record<string, unknown>;
  const educationLines = asArray(profileSummary.education_lines).map((item) => asString(item)).filter(Boolean);
  const schoolLine = asString(profileSummary.school_line);
  const topSkills = asString(profileSummary.top_skills);
  const skillTags = topSkills
    .split(/[•,]/)
    .map((item) => item.trim())
    .filter(Boolean);
  const linkedinUrl =
    resolveLinkedinUrl(
      asString(profileSummary.linkedin_url) ||
        pickFirstString(materialized, ["linkedin_url"]) ||
        pickFirstString(record, ["linkedin_url"]) ||
        pickLinkedinUrlFromUrls(record.urls) ||
        base.linkedinUrl ||
        "",
      base.name,
    );

  const enriched = {
    ...base,
    headline:
      asString(profileSummary.headline) ||
      pickFirstString(materialized, ["role", "headline", "title"]) ||
      base.headline,
    avatarUrl:
      pickProfilePhoto(profilePhotoMap, candidateId) ||
      asString(profileSummary.photo_url) ||
      pickFirstString(materialized, ["profile_photo_url", "avatar_url", "photo_url", "media_url"]) ||
      base.avatarUrl,
    summary:
      asString(profileSummary.about) ||
      pickFirstString(materialized, ["notes", "summary", "description"]) ||
      base.summary,
    currentCompany:
      asString(profileSummary.current_company) ||
      pickFirstString(materialized, ["organization", "current_company"]) ||
      base.currentCompany,
    location:
      asString(profileSummary.location) ||
      pickFirstString(materialized, ["location"]) ||
      base.location,
    linkedinUrl,
    sourceDataset:
      normalizeDatasetLabel(pickFirstString(materialized, ["source_dataset"])) ||
      normalizeDatasetLabel(asString(asArray(record.source_datasets)[0])) ||
      base.sourceDataset,
    notesSnippet:
      firstLine(pickFirstString(materialized, ["notes"])) ||
      firstLine(pickFirstString(record, ["manual_review_rationale"])) ||
      base.notesSnippet,
    education:
      educationLines.length > 0
        ? educationLines
        : schoolLine
          ? [schoolLine]
          : base.education,
    focusAreas:
      base.focusAreas.length > 0 ? base.focusAreas : skillTags.slice(0, 6),
    matchReasons:
      base.matchReasons.length > 0
        ? base.matchReasons
        : [
            joinFragments([
              pickFirstString(materialized, ["role", "headline", "title"]) || asString(profileSummary.headline),
              asString(profileSummary.current_company) || pickFirstString(materialized, ["organization"]),
            ]),
          ].filter(Boolean),
  };
  return mergeExternalLinks(enriched, contactLinkMap, candidateId);
}

function deriveCandidateFromDocument(
  record: Record<string, unknown>,
  profilePhotoMap: Record<string, string> = {},
  contactLinkMap: Record<string, unknown> = {},
): Candidate {
  const evidenceItems = asArray(record.evidence).map((item, index) => {
    const source = ((item && typeof item === "object" ? item : {}) as Record<string, unknown>);
    return {
      label: pickFirstString(source, ["label", "source_type", "title"]) || `Evidence ${index + 1}`,
      type:
        (pickFirstString(source, ["type", "source_type"]) as
          | "linkedin"
          | "publication"
          | "homepage"
          | "github"
          | "cv") || "homepage",
      url: pickFirstString(source, ["url", "source_url", "linkedin_url"]) || "#",
      excerpt:
        pickFirstString(source, ["excerpt", "summary", "snippet", "reason"]) ||
        "Recovered from candidate document artifact.",
    };
  });

  const focusAreas = asArray(record.focus_areas ?? record.keywords ?? record.tags)
    .map((value) => asString(value))
    .filter(Boolean);

  const matchReasons = asArray(record.match_reasons ?? record.reasons)
    .map((value) => asString(value))
    .filter(Boolean);

  const employmentStatus = pickFirstString(record, ["employment_status", "status"]) || "lead";

  const candidate: Candidate = {
    id: pickFirstString(record, ["candidate_id", "id"]) || crypto.randomUUID(),
    name: pickFirstString(record, ["display_name", "name_en", "full_name", "name"]) || "Unknown Candidate",
    headline: pickFirstString(record, ["headline", "role", "title"]) || "Candidate profile",
    avatarUrl:
      pickProfilePhoto(profilePhotoMap, pickFirstString(record, ["candidate_id", "id"])) ||
      pickFirstString(record, ["avatar_url", "photo_url"]),
    team: normalizeTeam(pickFirstString(record, ["team", "group_name", "group"])),
    employmentStatus:
      employmentStatus === "current" || employmentStatus === "former" ? employmentStatus : "lead",
    confidence: toConfidence(pickFirstString(record, ["confidence_label"])),
    summary:
      pickFirstString(record, ["summary", "notes", "description"]) ||
      "Recovered from candidate documents.",
    currentCompany: pickFirstString(record, ["current_company", "organization"]),
    location: pickFirstString(record, ["location"]),
    linkedinUrl: resolveLinkedinUrl(pickFirstString(record, ["linkedin_url"]), pickFirstString(record, ["display_name", "name_en", "full_name", "name"])),
    sourceDataset: normalizeDatasetLabel(pickFirstString(record, ["source_dataset"])),
    notesSnippet: firstLine(pickFirstString(record, ["notes"])),
    focusAreas,
    matchReasons,
    education: asArray(record.education)
      .map((value) => asString(value))
      .filter(Boolean),
    experience: asArray(record.work_history ?? record.experience)
      .map((value) => asString(value))
      .filter(Boolean),
    evidence: evidenceItems,
  };
  return mergeExternalLinks(candidate, contactLinkMap, pickFirstString(record, ["candidate_id", "id"]));
}

function deriveCandidateDetail(
  candidateRecord: Record<string, unknown>,
  materializedRecord?: Record<string, unknown> | null,
): CandidateDetail {
  const base = deriveCandidate(candidateRecord);
  const materialized = materializedRecord || {};
  const metadata = (candidateRecord.metadata as Record<string, unknown>) || {};
  const aliases = [
    ...asArray(candidateRecord.aliases),
    ...asArray(metadata.aliases),
  ]
    .map((value) => asString(value))
    .filter(Boolean);

  const sourceSummary = [
    ...base.evidence.map((item) => item.label),
    ...asArray(materialized.source_summary).map((value) => asString(value)),
  ].filter(Boolean);

  const documents = asArray(materialized.documents ?? materialized.document_sections)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .map((item, index) => ({
      title: pickFirstString(item, ["title", "label"]) || `Document ${index + 1}`,
      body:
        pickFirstString(item, ["body", "summary", "content", "text"]) ||
        "Recovered document section from materialized candidate artifacts.",
    }));

  return {
    ...base,
    currentCompany:
      pickFirstString(candidateRecord, ["current_company", "organization"]) ||
      pickFirstString(metadata, ["current_company", "organization"]) ||
      "Unknown company",
    location:
      pickFirstString(candidateRecord, ["location"]) ||
      pickFirstString(metadata, ["location"]) ||
      "Unknown location",
    aliases: aliases.length ? aliases : [base.name],
    sourceSummary:
      sourceSummary.length > 0
        ? sourceSummary
        : [base.sourceDataset || "Normalized artifact", base.linkedinUrl ? "LinkedIn profile available" : ""].filter(
            Boolean,
          ),
    documents:
      documents.length > 0
        ? documents
        : [
            { title: "Candidate Summary", body: base.summary },
            { title: "Match Reasons", body: base.matchReasons.join(" ") || "No detailed match reason available." },
          ],
  };
}

function deriveManualReviewItem(record: Record<string, unknown>): ManualReviewItem {
  const metadata = (record.metadata as Record<string, unknown>) || {};
  const sourceLinks = asArray(record.source_links ?? metadata.source_links).map((item) => (item as Record<string, unknown>) || {});
  const evidenceLabels = sourceLinks
    .map((item) => firstNonEmptyString([item.label, item.title, item.source_type]))
    .filter(Boolean);

  const candidateId =
    pickFirstString(record, ["candidate_id", "id"]) ||
    pickFirstString(metadata, ["candidate_id"]);
  const candidateName =
    pickFirstString(record, ["display_name", "name_en", "full_name", "name"]) ||
    pickFirstString(metadata, ["display_name", "candidate_name"]) ||
    "Unknown candidate";
  const reviewType =
    pickFirstString(record, ["review_type", "backlog_type"]) ||
    pickFirstString(metadata, ["review_type"]) ||
    "manual_review";
  const summary =
    pickFirstString(record, ["summary", "reason", "notes"]) ||
    pickFirstString(metadata, ["summary", "reason"]) ||
    "Recovered from manual review backlog.";
  const recommendedAction =
    pickFirstString(record, ["recommended_action", "next_action"]) ||
    pickFirstString(metadata, ["recommended_action"]) ||
    "Inspect the attached evidence and decide whether to resolve, reject, or continue research.";

  return {
    id: candidateId || crypto.randomUUID(),
    candidateId,
    candidateName,
    reviewType,
    status: "pending",
    summary,
    recommendedAction,
    evidenceLabels: evidenceLabels.length ? evidenceLabels : ["Backlog item"],
  };
}

async function getDashboardFromLocalAssets(): Promise<DashboardData | null> {
  try {
    const indexPayload = await fetchPublicJson<{ snapshotId: string; files: string[] }>("/tml/index.json");
    const normalizedCandidates = await fetchPublicJsonOptional<unknown[]>("/tml/normalized_candidates.json", []);
    const materializedPayload = await fetchPublicJsonOptional<unknown>("/tml/materialized_candidate_documents.json", {});
    const candidateDocumentsPayload = await fetchPublicJsonOptional<unknown>("/tml/candidate_documents.json", []);
    const profilePhotoMap = await fetchPublicJsonOptional<Record<string, string>>("/tml/profile_photos.json", {});
    const profileSummaryMap = await fetchPublicJsonOptional<Record<string, Record<string, unknown>>>(
      "/tml/profile_summaries.json",
      {},
    );
    const enrichedContactLinkMap = await fetchPublicJsonOptional<Record<string, unknown>>(
      "/tml/enriched_contact_links.json",
      {},
    );
    const manualReview = await fetchPublicJson<unknown[]>("/tml/manual_review_backlog.json").catch(() => []);
    const assetRegistry = await fetchPublicJson<Record<string, unknown>>("/tml/asset_registry.json").catch(
      () => ({}) as Record<string, unknown>,
    );

    const materializedCandidates = extractCandidateArray(materializedPayload);
    const materializedById = new Map(
      materializedCandidates
        .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
        .map((item) => [pickFirstString(item, ["candidate_id", "id"]), item] as const),
    );
    const candidateDocuments = extractCandidateArray(candidateDocumentsPayload);
    const candidateSource = normalizedCandidates.length > 0 ? normalizedCandidates : candidateDocuments;
    const candidates = candidateSource
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .map((item) =>
        normalizedCandidates.length > 0
          ? deriveCandidateFromNormalizedRecord(
              item,
              materializedById.get(pickFirstString(item, ["candidate_id", "id"])) || null,
              profilePhotoMap,
              profileSummaryMap,
              enrichedContactLinkMap,
            )
          : deriveCandidateFromDocument(item, profilePhotoMap, enrichedContactLinkMap),
      );

    const currentCount = candidates.filter((candidate) => candidate.employmentStatus === "current").length;
    const formerCount = candidates.filter((candidate) => candidate.employmentStatus === "former").length;
    const highCount = candidates.filter((candidate) => candidate.confidence === "high").length;
    const leadCount = candidates.filter((candidate) => candidate.confidence === "lead_only").length;
    const groups = ["All", ...new Set(candidates.map((candidate) => normalizeTeam(candidate.team)).filter(Boolean))];
    const registryAssets = assetRegistry["assets"];
    const assetEntries = Array.isArray(registryAssets)
      ? registryAssets.length
      : Array.isArray((assetRegistry as { entries?: unknown[] }).entries)
        ? ((assetRegistry as { entries?: unknown[] }).entries || []).length
        : 0;

    return {
      title: "Thinking Machines Lab Talent Asset View",
      snapshotId: indexPayload.snapshotId,
      queryLabel: "Recovered local company artifacts for frontend demo and testing",
      totalCandidates: candidates.length,
      totalEvidence: assetEntries,
      manualReviewCount: manualReview.length,
      layers: [
        { id: "all", label: "Layer 0 · All Candidates", count: candidates.length },
        { id: "lead_only", label: "Layer 1 · Lead / Low Confidence", count: leadCount },
        { id: "high", label: "Layer 2 · High Confidence", count: highCount },
        { id: "confirmed", label: "Layer 3 · Confirmed Direction", count: currentCount + formerCount - leadCount },
      ],
      groups,
      candidates,
    };
  } catch {
    return null;
  }
}

async function getCandidateDetailFromLocalAssets(candidateId: string): Promise<CandidateDetail | null> {
  try {
    const normalizedCandidates = await fetchPublicJsonOptional<unknown[]>("/tml/normalized_candidates.json", []);
    const candidateDocumentsPayload = await fetchPublicJsonOptional<unknown>("/tml/candidate_documents.json", []);
    const profilePhotoMap = await fetchPublicJsonOptional<Record<string, string>>("/tml/profile_photos.json", {});
    const profileSummaryMap = await fetchPublicJsonOptional<Record<string, Record<string, unknown>>>(
      "/tml/profile_summaries.json",
      {},
    );
    const enrichedContactLinkMap = await fetchPublicJsonOptional<Record<string, unknown>>(
      "/tml/enriched_contact_links.json",
      {},
    );
    const materializedPayload = await fetchPublicJsonOptional<unknown>("/tml/materialized_candidate_documents.json", {});

    const candidateDocuments = extractCandidateArray(candidateDocumentsPayload);
    const materializedDocuments = extractCandidateArray(materializedPayload);
    const candidateRecords = (normalizedCandidates.length > 0 ? normalizedCandidates : candidateDocuments).map(
      (item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>),
    );
    const materializedRecords = materializedDocuments.map((item) =>
      ((item && typeof item === "object" ? item : {}) as Record<string, unknown>),
    );

    const candidateRecord =
      candidateRecords.find((item) => pickFirstString(item, ["candidate_id", "id"]) === candidateId) ||
      candidateRecords.find((item) => pickFirstString(item, ["display_name", "name_en"]) === candidateId) ||
      candidateRecords[0];

    if (!candidateRecord) {
      return null;
    }

    const candidateKey = pickFirstString(candidateRecord, ["candidate_id", "id"]);
    const materializedRecord =
      materializedRecords.find((item) => pickFirstString(item, ["candidate_id", "id"]) === candidateKey) ||
      materializedRecords.find(
        (item) =>
          pickFirstString(item, ["display_name", "name_en"]) ===
          pickFirstString(candidateRecord, ["display_name", "name_en"]),
      ) ||
      null;

    const profileSourcePath =
      pickFirstString(materializedRecord || {}, ["source_path"]) ||
      pickFirstString(candidateRecord, ["source_path"]);
    const profilePublicPath = sourcePathToPublicProfilePath(profileSourcePath);
    const profilePayload = profilePublicPath
      ? await fetchPublicJsonOptional<unknown>(profilePublicPath, {})
      : {};
    const profileItem = pickProfileItem(profilePayload);

    if (normalizedCandidates.length > 0) {
      const detail = deriveCandidateDetail(candidateRecord, materializedRecord);
      const enrichedBase = deriveCandidateFromNormalizedRecord(
        candidateRecord,
        materializedRecord,
        profilePhotoMap,
        profileSummaryMap,
        enrichedContactLinkMap,
      );
      const profileHeadline = pickFirstString(profileItem, ["headline"]);
      const profileAbout = pickFirstString(profileItem, ["about"]);
      const profileLocation = joinFragments([
        pickFirstString((profileItem.location as Record<string, unknown>) || {}, ["linkedinText"]),
        pickFirstString(profileItem, ["locationName"]),
      ]);
      const profileLinkedin = pickFirstString(profileItem, ["linkedinUrl"]);
      const profilePhoto =
        pickFirstString(profileItem, ["photo"]) ||
        pickFirstString((profileItem.profilePicture as Record<string, unknown>) || {}, ["url"]);
      const profileEducation = profileEducationLines(profileItem);
      const profileExperience = profileExperienceLines(profileItem);
      const profileFocus = profileFocusAreas(profileItem);
      const profileAlias = pickFirstString(profileItem, ["publicIdentifier"]);
      const profileCompany = pickFirstString(
        ((asArray(profileItem.currentPosition)[0] as Record<string, unknown>) || {}),
        ["companyName"],
      );
      const narrativeSummary = buildNarrativeSummary(
        detail.name,
        profileItem,
        enrichedBase.summary || detail.summary,
        profileCompany || enrichedBase.currentCompany || detail.currentCompany,
      );
      return {
        ...detail,
        avatarUrl: profilePhoto || enrichedBase.avatarUrl,
        headline: profileHeadline || enrichedBase.headline,
        summary: narrativeSummary,
        currentCompany: profileCompany || enrichedBase.currentCompany || detail.currentCompany,
        location: profileLocation || enrichedBase.location || detail.location,
        linkedinUrl: resolveLinkedinUrl(profileLinkedin || enrichedBase.linkedinUrl || detail.linkedinUrl || "", detail.name),
        sourceDataset: enrichedBase.sourceDataset || detail.sourceDataset,
        notesSnippet: enrichedBase.notesSnippet || detail.notesSnippet,
        education: profileEducation.length > 0 ? profileEducation : detail.education,
        experience: profileExperience.length > 0 ? profileExperience : detail.experience,
        focusAreas: profileFocus.length > 0 ? profileFocus : detail.focusAreas,
        aliases: profileAlias ? Array.from(new Set([profileAlias, ...detail.aliases])) : detail.aliases,
      };
    }

    const baseCandidate = deriveCandidateFromDocument(candidateRecord, profilePhotoMap, enrichedContactLinkMap);
    return {
      ...baseCandidate,
      currentCompany:
        pickFirstString(candidateRecord, ["current_company", "organization"]) || "Unknown company",
      location: pickFirstString(candidateRecord, ["location"]) || firstLine(pickFirstString(candidateRecord, ["notes"])) || "Unknown location",
      aliases: [baseCandidate.name],
      sourceSummary: [
        ...baseCandidate.evidence.map((item) => item.label),
        baseCandidate.sourceDataset || "",
        baseCandidate.linkedinUrl ? "LinkedIn profile available" : "",
      ].filter(Boolean),
      documents: materializedRecord
        ? [
            {
              title: "物化文档摘要",
              body:
                pickFirstString(materializedRecord, ["summary", "description"]) ||
                "Recovered from materialized candidate documents.",
            },
          ]
        : [
            {
              title: "候选人摘要",
              body: baseCandidate.summary,
            },
            ...(baseCandidate.notesSnippet
              ? [
                  {
                    title: "原始备注",
                    body: baseCandidate.notesSnippet,
                  },
                ]
              : []),
          ],
    };
  } catch {
    return null;
  }
}

async function getManualReviewItemsFromLocalAssets(): Promise<ManualReviewItem[] | null> {
  try {
    const payload = await fetchPublicJson<unknown[]>("/tml/manual_review_backlog.json");
    if (!Array.isArray(payload)) {
      return [];
    }
    return payload
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .map(deriveManualReviewItem);
  } catch {
    return null;
  }
}

async function getRunStatusFromLocalAssets(): Promise<RunStatusData | null> {
  try {
    const indexPayload = await fetchPublicJson<{ snapshotId: string; bundleId?: string; source?: string }>("/tml/index.json");
    const normalizedCandidates = await fetchPublicJsonOptional<unknown[]>("/tml/normalized_candidates.json", []);
    const candidateDocumentsPayload = await fetchPublicJsonOptional<unknown>("/tml/candidate_documents.json", []);
    const manualReview = await fetchPublicJsonOptional<unknown[]>("/tml/manual_review_backlog.json", []);
    const profileCompletion = await fetchPublicJsonOptional<unknown[]>("/tml/profile_completion_backlog.json", []);
    const assetRegistry = await fetchPublicJsonOptional<Record<string, unknown>>("/tml/asset_registry.json", {});

    const candidateDocuments = extractCandidateArray(candidateDocumentsPayload);
    const candidateSource = normalizedCandidates.length > 0 ? normalizedCandidates : candidateDocuments;
    const candidates = candidateSource
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .map((item) => (normalizedCandidates.length > 0 ? deriveCandidate(item) : deriveCandidateFromDocument(item)));

    const currentCount = candidates.filter((candidate) => candidate.employmentStatus === "current").length;
    const formerCount = candidates.filter((candidate) => candidate.employmentStatus === "former").length;
    const assetEntries = Array.isArray(assetRegistry["assets"])
      ? (assetRegistry["assets"] as unknown[]).length
      : Array.isArray((assetRegistry as { entries?: unknown[] }).entries)
        ? ((assetRegistry as { entries?: unknown[] }).entries || []).length
        : 0;

    const sourceLabel =
      indexPayload.source === "object_storage" ? "R2 asset pull" : "local runtime import";
    const bundleLabel = indexPayload.bundleId ? `Bundle ${indexPayload.bundleId}` : "Imported artifact set";

    return {
      jobId: indexPayload.bundleId || `tml_${indexPayload.snapshotId}`,
      status: "completed",
      currentStage: "Asset Recovery Ready",
      startedAt: indexPayload.snapshotId,
      metrics: [
        { label: "Candidates", value: String(candidates.length) },
        { label: "Evidence", value: String(assetEntries) },
        { label: "Manual Review", value: String(manualReview.length) },
        { label: "Profile Backlog", value: String(profileCompletion.length) },
      ],
      timeline: [
        {
          id: "local_1",
          stage: "Bundle Discovery",
          title: "Bundle discovery completed",
          detail: `${sourceLabel} resolved Thinking Machines Lab snapshot ${indexPayload.snapshotId}.`,
          status: "completed",
          startedAt: indexPayload.snapshotId,
          completedAt: indexPayload.snapshotId,
          sourceTags: [],
        },
        {
          id: "local_2",
          stage: "Candidate Materialization",
          title: "Candidate materialization completed",
          detail: `Loaded ${candidates.length} candidate records with ${currentCount} current and ${formerCount} former profiles.`,
          status: "completed",
          startedAt: indexPayload.snapshotId,
          completedAt: indexPayload.snapshotId,
          sourceTags: [],
        },
        {
          id: "local_3",
          stage: "Review Backlog",
          title: "Review backlog completed",
          detail: `${manualReview.length} manual review items and ${profileCompletion.length} profile completion items are available for demo workflows.`,
          status: "completed",
          startedAt: indexPayload.snapshotId,
          completedAt: indexPayload.snapshotId,
          sourceTags: [],
        },
      ],
      workers: [
        { id: "local_asset_loader", lane: "asset_loader", status: "completed", budget: bundleLabel },
        { id: "local_candidate_view", lane: "candidate_artifacts", status: "completed", budget: `${candidates.length} candidates materialized` },
        { id: "local_review_queue", lane: "review_surface", status: "completed", budget: `${manualReview.length} review items exposed` },
      ],
    };
  } catch {
    return null;
  }
}
