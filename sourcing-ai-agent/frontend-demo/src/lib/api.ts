import { mockCandidateDetails, mockDashboard, mockManualReviewItems, mockPlan, mockRunStatus } from "../data/mockData";
import {
  dashboardExpectedCandidateCount,
  dashboardHasRenderableCandidates,
  dashboardRowHydrationTargetCount,
} from "./dashboardHydration";
import { lifecycleEffectiveDeltaMaterializedCount } from "./resultViewLifecycle";
import { normalizeWorkflowStatus, resolveWorkflowStatus } from "./workflowStatus";
import type {
  Candidate,
  CandidateDetail,
  CandidateConfidence,
  CandidateEmailMetadata,
  CandidateExternalLink,
  CandidateFacetSummary,
  CandidateReviewRecord,
  CandidateReviewStatus,
  CandidateSourceMatch,
  BoardRuntimeState,
  DashboardData,
  DemoPlan,
  EffectiveExecutionSemantics,
  ExecutionPhaseContract,
  ExcelIntakeProgress,
  ExcelIntakeResponse,
  LinkedinStage1Progress,
  ManualReviewItem,
  PlanReviewDecision,
  PlanReviewEditableField,
  ProviderExecutionLanePreview,
  PlanReviewGate,
  ProfileFetchProgress,
  ResultViewLifecycle,
  RunStatusData,
  SupplementOperationResult,
  TargetCompanyIdentityPreview,
  TargetCandidateComposedProfile,
  TargetCandidateFollowUpStatus,
  TargetCandidateProfileDetail,
  TargetCandidatePublicWebBatch,
  TargetCandidatePublicWebActionResult,
  TargetCandidatePublicWebDetail,
  TargetCandidatePublicWebEvidenceLink,
  TargetCandidatePublicWebPromotion,
  TargetCandidatePublicWebRun,
  TargetCandidatePublicWebSearchState,
  TargetCandidatePublicWebSignal,
  TargetCandidatePublicWebStartResult,
  TargetCandidatePublicWebStatus,
  TargetCandidateRecord,
  WorkflowPhase,
} from "../types";

const runningLocally =
  typeof window !== "undefined" &&
  ["localhost", "127.0.0.1"].includes(window.location.hostname);
const useMock = import.meta.env.VITE_USE_MOCK === "true";
const preferLocalAssets = import.meta.env.VITE_USE_LOCAL_ASSETS === "true";
const DEFAULT_API_TIMEOUT_MS = 30_000;
const PLAN_API_TIMEOUT_MS = 90_000;
const WORKFLOW_START_TIMEOUT_MS = 60_000;
const RESULTS_API_TIMEOUT_MS = 120_000;
const EXPORT_POLL_INTERVAL_MS = 1_500;
const PROFILE_COMPLETION_TIMEOUT_MS = 180_000;
const DASHBOARD_INITIAL_CANDIDATE_CHUNK_SIZE = 96;
const DASHBOARD_BACKGROUND_CANDIDATE_CHUNK_SIZE = 96;
const DASHBOARD_CACHE_TTL_MS = 5 * 60_000;
const LIST_CACHE_TTL_MS = 60_000;
const CANDIDATE_DETAIL_CACHE_TTL_MS = 5 * 60_000;
const API_BASE_URL_STORAGE_KEY = "sourcing-ai-agent-demo-api-base-url";
const SAME_ORIGIN_API_BASE_URL = "same-origin";
const DEFAULT_RECALL_LIMITS = {
  top_k: 30,
  slug_resolution_limit: 20,
  profile_detail_limit: 20,
  publication_scan_limit: 20,
  publication_lead_limit: 30,
  exploration_limit: 20,
  semantic_rerank_limit: 30,
};
const candidateDetailBatchPromiseCache = new Map<string, Promise<Record<string, CandidateDetail | null>>>();
const candidateDetailCache = new Map<string, { value: CandidateDetail | null; cachedAt: number }>();
const dashboardPromiseCache = new Map<string, Promise<DashboardData>>();
const dashboardCache = new Map<string, { value: DashboardData; cachedAt: number }>();
const dashboardCandidatePagePromiseCache = new Map<string, Promise<DashboardCandidatePage>>();
const projectionDashboardPromiseCache = new Map<string, Promise<DashboardData>>();
const projectionDashboardCache = new Map<string, { value: DashboardData; cachedAt: number }>();
const dashboardRequestGeneration = new Map<string, number>();
const projectionDashboardRequestGeneration = new Map<string, number>();
const projectionCandidatePagePromiseCache = new Map<string, Promise<DashboardCandidatePage>>();
const runProjectionLinkPromiseCache = new Map<string, Promise<string>>();
const collectionProjectionLinkPromiseCache = new Map<string, Promise<string>>();
const collectionAssetOverviewPromiseCache = new Map<string, Promise<CollectionAssetOverview>>();
const collectionAssetOverviewCache = new Map<string, { value: CollectionAssetOverview; cachedAt: number }>();
const manualReviewItemsPromiseCache = new Map<string, Promise<ManualReviewItem[]>>();
const manualReviewItemsCache = new Map<string, { value: ManualReviewItem[]; cachedAt: number }>();
const candidateReviewRecordsPromiseCache = new Map<string, Promise<CandidateReviewRecord[]>>();
const candidateReviewRecordsCache = new Map<string, { value: CandidateReviewRecord[]; cachedAt: number }>();
const frontendHistoryPromiseCache = new Map<string, Promise<FrontendHistoryRecoveryEnvelope[]>>();
const frontendHistoryCache = new Map<string, { value: FrontendHistoryRecoveryEnvelope[]; cachedAt: number }>();
const targetCandidatesPromiseCache = new Map<string, Promise<TargetCandidateRecord[]>>();
const targetCandidatesCache = new Map<string, { value: TargetCandidateRecord[]; cachedAt: number }>();
const FULL_PROFILE_CAPTURE_KINDS = new Set([
  "harvest_profile_detail",
  "provider_profile_detail",
  "profile_registry_detail",
  "embedded_profile_detail",
]);
const PARTIAL_PROFILE_CAPTURE_KINDS = new Set([
  "search_seed_preview",
  "embedded_profile_preview",
  "roster_baseline_preview",
]);

export interface DashboardCandidatePage {
  jobId: string;
  resultMode: DashboardData["resultMode"];
  offset: number;
  limit: number;
  returnedCount: number;
  totalCandidates: number;
  filteredCandidateCount: number;
  hasMore: boolean;
  nextOffset: number | null;
  candidates: Candidate[];
  profileFetchProgress?: ProfileFetchProgress;
  linkedinStage1Progress?: LinkedinStage1Progress;
  resultViewLifecycle?: ResultViewLifecycle;
  boardRuntimeState?: BoardRuntimeState;
  candidateFacetSummary?: CandidateFacetSummary;
  candidateFacetSummaryScope?: string;
  filterSignature?: string;
  filterContract?: {
    source: string;
    facetCountScope: string;
    rowFilterScope: string;
    backendFilteredPagingSupported: boolean;
    filterSignature: string;
    filterActive: boolean;
  };
}

export interface BoardVisiblePatch {
  patchId: string;
  sequenceIndex: number;
  candidateCount: number;
  cumulativeCandidateCount: number;
  servedCandidateCount: number;
  displayReadyCandidateCount: number;
  profileDetailCandidateCount: number;
  explicitProfileCaptureCandidateCount: number;
  previewCandidateCount: number;
  needsProfileCompletionCandidateCount: number;
  lowProfileRichnessCandidateCount: number;
  qualityFieldsAvailable: boolean;
  publishedAt: string;
}

export interface BoardVisiblePatchLog {
  jobId: string;
  patches: BoardVisiblePatch[];
  returnedCount: number;
  hasMore: boolean;
  latestPublishedAt: string;
  latestSequenceIndex: number;
  boardRuntimeState?: BoardRuntimeState;
  resultViewLifecycle?: ResultViewLifecycle;
}

export interface WorkflowCommandExecutionSummary {
  source: string;
  fallbackStatus: string;
  fallbackUsed: boolean;
  moduleStateMutated: boolean;
  activityCount: number;
  attemptCount: number;
  entityDeltaCount: number;
  activityStatusCounts: Record<string, number>;
  attemptStatusCounts: Record<string, number>;
  entityDeltaStatusCounts: Record<string, number>;
  entityDeltaKindCounts: Record<string, number>;
  latestEffectStatus: string;
  latestActivity: Record<string, unknown>;
  latestAttempt: Record<string, unknown>;
  latestEntityDelta: Record<string, unknown>;
  sampleLimit: number;
  sampleTruncated: boolean;
}

export interface WorkflowCommandControlPolicy {
  commandType: string;
  owner: string;
  runningControlCategory: string;
  runningControlCategories: string[];
  runningControlMaturity: string;
  runningControlGapStatus: string;
  runningControlSurface: string;
  runningCancelBlockedReason: string;
  runningResumeBlockedReason: string;
  fallbackStatus: string;
  raw: Record<string, unknown>;
}

export interface WorkflowCommandRecord {
  commandId: string;
  workflowRunId: string;
  operationId: string;
  commandType: string;
  owner: string;
  agentExposureStatus: string;
  agentExposureGate: string;
  status: string;
  displayContract: Record<string, unknown>;
  controlPolicy: WorkflowCommandControlPolicy;
  controlState: Record<string, unknown>;
  activitySpinePolicy: Record<string, unknown>;
  executionSummary?: WorkflowCommandExecutionSummary;
  raw: Record<string, unknown>;
}

export interface WorkflowActivityRecord {
  activityRunId: string;
  workflowRunId: string;
  operationRunId: string;
  acquisitionRunId: string;
  commandId: string;
  activityType: string;
  owner: string;
  status: string;
  phase: string;
  mutationContract: string;
  moduleStateMutated: boolean;
  raw: Record<string, unknown>;
}

export interface WorkflowActivityAttemptRecord {
  attemptId: string;
  activityRunId: string;
  workflowRunId: string;
  commandId: string;
  activityType: string;
  owner: string;
  status: string;
  provider: string;
  mutationContract: string;
  moduleStateMutated: boolean;
  raw: Record<string, unknown>;
}

export interface WorkflowEntityDeltaRecord {
  deltaId: string;
  workflowRunId: string;
  operationRunId: string;
  commandId: string;
  activityRunId: string;
  attemptId: string;
  entityType: string;
  entityKey: string;
  deltaKind: string;
  status: string;
  reason: string;
  mutationContract: string;
  moduleStateMutated: boolean;
  raw: Record<string, unknown>;
}

export interface OperationRunStatusSummary {
  source: string;
  fallbackStatus: string;
  fallbackUsed: boolean;
  moduleStateMutated: boolean;
  operationStatus: string;
  operationPhase: string;
  workflowCommandCount: number;
  operationEventCount: number;
  commandStatusCounts: Record<string, number>;
  latestEventType: string;
  latestWorkflowCommand?: WorkflowCommandRecord;
}

export interface OperationRunControlState {
  operationStatus: string;
  actionStatus: string;
  operationPhase: string;
  canDispatch: boolean;
  canCancel: boolean;
  canRetry: boolean;
  canResume: boolean;
  allowedActions: string[];
  disabledReasons: Record<string, string>;
  controlSourceOfTruth: string;
  fallbackStatus: string;
  moduleStateMutatedOnControl: boolean;
  raw: Record<string, unknown>;
}

export interface OperationRunRecord {
  operationRunId: string;
  actionId: string;
  ownerModule: string;
  operationType: string;
  displayContract: Record<string, unknown>;
  status: string;
  progress: Record<string, unknown>;
  workflowRef: Record<string, unknown>;
  resultRef: Record<string, unknown>;
  controlState?: OperationRunControlState;
  statusSummary?: OperationRunStatusSummary;
  raw: Record<string, unknown>;
}

export interface OperationActionRecord {
  actionId: string;
  actionType: string;
  ownerModule: string;
  operationType: string;
  displayContract: Record<string, unknown>;
  approvalStatus: string;
  approvalPolicy: string;
  status: string;
  targetRef: Record<string, unknown>;
  input: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  raw: Record<string, unknown>;
}

export interface OperationActionDecisionResult {
  status: string;
  action: OperationActionRecord | null;
  operationRun: OperationRunRecord | null;
  raw: Record<string, unknown>;
}

export interface OperationEventRecord {
  eventId: string;
  eventType: string;
  sequenceNumber: number;
  actor: string;
  source: string;
  payload: Record<string, unknown>;
  recordedAt: string;
  raw: Record<string, unknown>;
}

export interface OperationRunProvenance {
  status: string;
  action: OperationActionRecord | null;
  operationRun: OperationRunRecord | null;
  actionEvents: OperationEventRecord[];
  operationEvents: OperationEventRecord[];
  eventTimeline: OperationEventRecord[];
  workflowCommands: WorkflowCommandRecord[];
  raw: Record<string, unknown>;
}

export { dashboardHasRenderableCandidates } from "./dashboardHydration";

function candidateDetailCacheKey(jobId: string, candidateId: string): string {
  return `${jobId}::${candidateId}`;
}

function readFreshCacheValue<T>(
  cache: Map<string, { value: T; cachedAt: number }>,
  key: string,
  ttlMs: number,
): T | null {
  const cached = cache.get(key);
  if (!cached) {
    return null;
  }
  if (Date.now() - cached.cachedAt > ttlMs) {
    cache.delete(key);
    return null;
  }
  return cached.value;
}

function writeCacheValue<T>(
  cache: Map<string, { value: T; cachedAt: number }>,
  key: string,
  value: T,
): T {
  cache.set(key, {
    value,
    cachedAt: Date.now(),
  });
  return value;
}

function evictPromiseCacheEntry<T>(
  cache: Map<string, Promise<T>>,
  key: string,
  promise: Promise<T>,
): void {
  if (cache.get(key) === promise) {
    cache.delete(key);
  }
}

export interface DashboardCandidatePageFilter {
  searchKeyword?: string;
  recallBuckets?: string[];
  employmentStatuses?: string[];
  locations?: string[];
  functionBuckets?: string[];
  layerIncludes?: string[];
  layerExcludes?: string[];
  auditStatuses?: string[];
}

function normalizeCandidatePageFilterList(values?: string[]): string[] {
  return Array.from(new Set((values || []).map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

export function dashboardCandidatePageFilterSignature(filter?: DashboardCandidatePageFilter): string {
  const normalized = {
    searchKeyword: String(filter?.searchKeyword || "").trim(),
    recallBuckets: normalizeCandidatePageFilterList(filter?.recallBuckets),
    employmentStatuses: normalizeCandidatePageFilterList(filter?.employmentStatuses),
    locations: normalizeCandidatePageFilterList(filter?.locations),
    functionBuckets: normalizeCandidatePageFilterList(filter?.functionBuckets),
    layerIncludes: normalizeCandidatePageFilterList(filter?.layerIncludes),
    layerExcludes: normalizeCandidatePageFilterList(filter?.layerExcludes),
    auditStatuses: normalizeCandidatePageFilterList(filter?.auditStatuses),
  };
  return JSON.stringify(normalized);
}

function candidatePageFilterQueryParams(filter?: DashboardCandidatePageFilter): Record<string, string | undefined> {
  const normalized = JSON.parse(dashboardCandidatePageFilterSignature(filter)) as Required<DashboardCandidatePageFilter>;
  return {
    search: normalized.searchKeyword || undefined,
    recall_buckets: normalized.recallBuckets.length > 0 ? normalized.recallBuckets.join(",") : undefined,
    employment_statuses: normalized.employmentStatuses.length > 0 ? normalized.employmentStatuses.join(",") : undefined,
    locations: normalized.locations.length > 0 ? normalized.locations.join(",") : undefined,
    function_buckets: normalized.functionBuckets.length > 0 ? normalized.functionBuckets.join(",") : undefined,
    layer_includes: normalized.layerIncludes.length > 0 ? normalized.layerIncludes.join(",") : undefined,
    layer_excludes: normalized.layerExcludes.length > 0 ? normalized.layerExcludes.join(",") : undefined,
    audit_statuses: normalized.auditStatuses.length > 0 ? normalized.auditStatuses.join(",") : undefined,
  };
}

function dashboardCandidatePageCacheKey(
  jobId: string,
  offset: number,
  limit: number,
  lightweight: boolean,
  filterSignature = "",
): string {
  return `${jobId}::${offset}::${limit}::${lightweight ? "lightweight" : "rich"}::${filterSignature}`;
}

function normalizeApiBaseUrl(value: string): string {
  const trimmed = value.trim().replace(/\/+$/, "");
  if (!trimmed) {
    return "";
  }
  if (
    trimmed === "/" ||
    ["same-origin", "same_origin", "sameorigin", "relative", "origin-relative"].includes(trimmed.toLowerCase())
  ) {
    return SAME_ORIGIN_API_BASE_URL;
  }
  if (runningLocally) {
    try {
      const parsed = new URL(trimmed);
      if (parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1") {
        parsed.hostname = "127.0.0.1";
        return parsed.toString().replace(/\/+$/, "");
      }
    } catch {
      return trimmed;
    }
  }
  return trimmed;
}

function isSameOriginApiBaseUrl(value: string): boolean {
  return value === SAME_ORIGIN_API_BASE_URL;
}

const ENV_API_BASE_URL = normalizeApiBaseUrl(import.meta.env.VITE_API_BASE_URL || "");
const HOSTED_API_BASE_URL_LOCKED = !runningLocally && Boolean(ENV_API_BASE_URL);
const LOOPBACK_FALLBACK_ENABLED = runningLocally;

// C2.4: static per-user bearer (no login UI). When VITE_SOURCING_API_BEARER_TOKEN
// is set at build time, attach it to every authenticated API request so the
// backend _AuthMiddleware (C2.1) accepts the call. Empty -> no header, which
// keeps the demo working against an open-mode (no-token) backend.
const ENV_API_BEARER_TOKEN = String(import.meta.env.VITE_SOURCING_API_BEARER_TOKEN || "").trim();

function applyApiAuthHeader(requestHeaders: Headers): void {
  if (ENV_API_BEARER_TOKEN && !requestHeaders.has("Authorization")) {
    requestHeaders.set("Authorization", `Bearer ${ENV_API_BEARER_TOKEN}`);
  }
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
  if (HOSTED_API_BASE_URL_LOCKED) {
    return false;
  }
  return runningLocally || !ENV_API_BASE_URL;
}

export function getConfiguredApiBaseUrl(): string {
  if (HOSTED_API_BASE_URL_LOCKED) {
    return ENV_API_BASE_URL;
  }
  const queryBaseUrl = readApiBaseUrlFromQuery();
  if (queryBaseUrl) {
    const normalizedQueryBaseUrl = normalizeApiBaseUrl(queryBaseUrl);
    saveRuntimeApiBaseUrl(normalizedQueryBaseUrl);
    return normalizedQueryBaseUrl;
  }
  const envBaseUrl = ENV_API_BASE_URL;
  if (envBaseUrl) {
    if (readRuntimeApiBaseUrl() !== envBaseUrl) {
      saveRuntimeApiBaseUrl(envBaseUrl);
    }
    return envBaseUrl;
  }
  const storedBaseUrl = readRuntimeApiBaseUrl();
  if (runningLocally && storedBaseUrl) {
    const normalizedStoredBaseUrl = normalizeApiBaseUrl(storedBaseUrl);
    if (normalizedStoredBaseUrl !== storedBaseUrl) {
      saveRuntimeApiBaseUrl(normalizedStoredBaseUrl);
    }
    return normalizedStoredBaseUrl;
  }
  if (storedBaseUrl) {
    return storedBaseUrl;
  }
  if (LOOPBACK_FALLBACK_ENABLED && typeof window !== "undefined") {
    const defaultLocalApiBaseUrl = `${window.location.protocol}//127.0.0.1:8765`;
    if (readRuntimeApiBaseUrl() !== defaultLocalApiBaseUrl) {
      saveRuntimeApiBaseUrl(defaultLocalApiBaseUrl);
    }
    return defaultLocalApiBaseUrl;
  }
  return "";
}

function buildAlternateLocalApiBaseUrl(apiBaseUrl: string): string {
  try {
    const parsed = new URL(apiBaseUrl);
    if (parsed.hostname === "localhost") {
      parsed.hostname = "127.0.0.1";
      return parsed.toString().replace(/\/+$/, "");
    }
    if (parsed.hostname === "127.0.0.1") {
      parsed.hostname = "localhost";
      return parsed.toString().replace(/\/+$/, "");
    }
  } catch {
    return "";
  }
  return "";
}

function listLocalApiFallbackBaseUrls(apiBaseUrl: string): string[] {
  if (!LOOPBACK_FALLBACK_ENABLED || typeof window === "undefined") {
    return [];
  }
  if (isSameOriginApiBaseUrl(apiBaseUrl)) {
    return [
      `${window.location.protocol}//127.0.0.1:8765`,
      `${window.location.protocol}//localhost:8765`,
    ];
  }
  if (isLikelyLocalApiBaseUrl(apiBaseUrl)) {
    const alternateApiBaseUrl = buildAlternateLocalApiBaseUrl(apiBaseUrl);
    return alternateApiBaseUrl && alternateApiBaseUrl !== apiBaseUrl ? [alternateApiBaseUrl] : [];
  }
  return [];
}

function isLikelyLocalApiBaseUrl(apiBaseUrl: string): boolean {
  return /^https?:\/\/(?:localhost|127\.0\.0\.1)(?::\d+)?$/i.test(apiBaseUrl);
}

function buildApiRequestUrl(apiBaseUrl: string, path: string): string {
  if (isSameOriginApiBaseUrl(apiBaseUrl)) {
    return path;
  }
  return `${apiBaseUrl}${path}`;
}

function resolveApiMediaUrl(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  if (/^(?:https?:|data:|blob:)/i.test(trimmed)) {
    return trimmed;
  }
  if (trimmed.startsWith("//")) {
    return `${typeof window !== "undefined" ? window.location.protocol : "https:"}${trimmed}`;
  }
  if (!trimmed.startsWith("/api/")) {
    return trimmed;
  }
  const apiBaseUrl = getConfiguredApiBaseUrl();
  if (runningLocally && isSameOriginApiBaseUrl(apiBaseUrl) && typeof window !== "undefined") {
    return `${window.location.protocol}//127.0.0.1:8765${trimmed}`;
  }
  return buildApiRequestUrl(apiBaseUrl, trimmed);
}

class LocalApiFallbackSignal extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LocalApiFallbackSignal";
  }
}

function canUseLocalApiFallback(apiBaseUrl: string): boolean {
  return LOOPBACK_FALLBACK_ENABLED && (isSameOriginApiBaseUrl(apiBaseUrl) || isLikelyLocalApiBaseUrl(apiBaseUrl));
}

function looksLikeSpaFallbackResponse(contentType: string, body: string): boolean {
  const normalizedContentType = contentType.toLowerCase();
  const trimmedBody = body.trimStart().slice(0, 160).toLowerCase();
  return (
    normalizedContentType.includes("text/html") ||
    trimmedBody.startsWith("<!doctype html") ||
    trimmedBody.startsWith("<html")
  );
}

function isLocalApiRecoverableError(error: unknown): boolean {
  return error instanceof TypeError || error instanceof LocalApiFallbackSignal;
}

function extractDownloadFilename(contentDisposition: string | null, fallback: string): string {
  const header = (contentDisposition || "").trim();
  if (!header) {
    return fallback;
  }
  const utf8Match = header.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch {
      return utf8Match[1];
    }
  }
  const quotedMatch = header.match(/filename\s*=\s*"([^"]+)"/i);
  if (quotedMatch?.[1]) {
    return quotedMatch[1];
  }
  const bareMatch = header.match(/filename\s*=\s*([^;]+)/i);
  if (bareMatch?.[1]) {
    return bareMatch[1].trim();
  }
  return fallback;
}

async function fetchJson<T>(path: string, options?: RequestInit, timeoutMs = DEFAULT_API_TIMEOUT_MS): Promise<T> {
  const apiBaseUrl = getConfiguredApiBaseUrl();
  if (!apiBaseUrl) {
    throw new Error(
      "API base URL is not configured. Please set a backend URL or enable same-origin API proxy mode before starting a search.",
    );
  }
  const fetchFromApiBaseUrl = async (resolvedApiBaseUrl: string): Promise<T> => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    const requestHeaders = new Headers(options?.headers || {});
    if (!requestHeaders.has("Content-Type") && !isFormDataBody(options?.body)) {
      requestHeaders.set("Content-Type", "application/json");
    }
    applyApiAuthHeader(requestHeaders);
    try {
      const response = await fetch(buildApiRequestUrl(resolvedApiBaseUrl, path), {
        ...options,
        headers: requestHeaders,
        signal: controller.signal,
      });
      const responseText = await response.text();
      const responseContentType = response.headers.get("Content-Type") || "";
      if (!response.ok) {
        if (
          canUseLocalApiFallback(resolvedApiBaseUrl) &&
          looksLikeSpaFallbackResponse(responseContentType, responseText)
        ) {
          throw new LocalApiFallbackSignal(
            `same-origin API request returned frontend shell for ${path}`,
          );
        }
        const detail = responseText.trim();
        throw new Error(detail ? `Request failed: ${response.status} ${detail.slice(0, 240)}` : `Request failed: ${response.status}`);
      }
      if (resolvedApiBaseUrl !== apiBaseUrl && !isSameOriginApiBaseUrl(resolvedApiBaseUrl)) {
        saveRuntimeApiBaseUrl(resolvedApiBaseUrl);
      }
      try {
        return JSON.parse(responseText) as T;
      } catch (error) {
        if (
          canUseLocalApiFallback(resolvedApiBaseUrl) &&
          looksLikeSpaFallbackResponse(responseContentType, responseText)
        ) {
          throw new LocalApiFallbackSignal(
            `same-origin API request returned non-JSON frontend shell for ${path}`,
          );
        }
        throw error;
      }
    } finally {
      clearTimeout(timeoutId);
    }
  };
  try {
    return await fetchFromApiBaseUrl(apiBaseUrl);
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s.`);
    }
    if (canUseLocalApiFallback(apiBaseUrl) && isLocalApiRecoverableError(error)) {
      let lastFallbackError: unknown = error;
      for (const fallbackApiBaseUrl of listLocalApiFallbackBaseUrls(apiBaseUrl)) {
        try {
          return await fetchFromApiBaseUrl(fallbackApiBaseUrl);
        } catch (fallbackError) {
          if (!isLocalApiRecoverableError(fallbackError)) {
            throw fallbackError;
          }
          lastFallbackError = fallbackError;
        }
      }
      if (isSameOriginApiBaseUrl(apiBaseUrl) || isLikelyLocalApiBaseUrl(apiBaseUrl)) {
        const detail = lastFallbackError instanceof Error ? ` Last error: ${lastFallbackError.message}` : "";
        throw new Error(
          `Local backend is unreachable via ${apiBaseUrl}. Please make sure the local service is running on http://127.0.0.1:8765.${detail}`,
        );
      }
    }
    throw error;
  }
}

function isFormDataBody(body: BodyInit | null | undefined): body is FormData {
  return typeof FormData !== "undefined" && body instanceof FormData;
}

function appendOptionalFormField(form: FormData, key: string, value: string | boolean | undefined): void {
  if (value === undefined) {
    return;
  }
  form.append(key, typeof value === "boolean" ? String(value) : value);
}

async function fetchBinary(
  path: string,
  options?: RequestInit,
  timeoutMs = DEFAULT_API_TIMEOUT_MS,
): Promise<{ blob: Blob; filename: string; contentType: string; headers: Headers }> {
  const apiBaseUrl = getConfiguredApiBaseUrl();
  if (!apiBaseUrl) {
    throw new Error(
      "API base URL is not configured. Please set a backend URL or enable same-origin API proxy mode before starting a search.",
    );
  }
  const fetchFromApiBaseUrl = async (
    resolvedApiBaseUrl: string,
  ): Promise<{ blob: Blob; filename: string; contentType: string; headers: Headers }> => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    const requestHeaders = new Headers(options?.headers || {});
    if (options?.body && !requestHeaders.has("Content-Type") && !isFormDataBody(options.body)) {
      requestHeaders.set("Content-Type", "application/json");
    }
    applyApiAuthHeader(requestHeaders);
    try {
      const response = await fetch(buildApiRequestUrl(resolvedApiBaseUrl, path), {
        ...options,
        headers: requestHeaders,
        signal: controller.signal,
      });
      if (!response.ok) {
        const detail = (await response.text()).trim();
        throw new Error(
          detail ? `Request failed: ${response.status} ${detail.slice(0, 240)}` : `Request failed: ${response.status}`,
        );
      }
      if (resolvedApiBaseUrl !== apiBaseUrl && !isSameOriginApiBaseUrl(resolvedApiBaseUrl)) {
        saveRuntimeApiBaseUrl(resolvedApiBaseUrl);
      }
      const blob = await response.blob();
      return {
        blob,
        filename: extractDownloadFilename(response.headers.get("Content-Disposition"), "download.bin"),
        contentType: response.headers.get("Content-Type") || blob.type || "application/octet-stream",
        headers: new Headers(response.headers),
      };
    } finally {
      clearTimeout(timeoutId);
    }
  };
  try {
    return await fetchFromApiBaseUrl(apiBaseUrl);
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s.`);
    }
    if (LOOPBACK_FALLBACK_ENABLED && error instanceof TypeError) {
      for (const fallbackApiBaseUrl of listLocalApiFallbackBaseUrls(apiBaseUrl)) {
        try {
          return await fetchFromApiBaseUrl(fallbackApiBaseUrl);
        } catch (fallbackError) {
          if (!(fallbackError instanceof TypeError)) {
            throw fallbackError;
          }
        }
      }
      if (isSameOriginApiBaseUrl(apiBaseUrl) || isLikelyLocalApiBaseUrl(apiBaseUrl)) {
        throw new Error(
          `Local backend is unreachable via ${apiBaseUrl}. Please make sure the local service is running on http://127.0.0.1:8765.`,
        );
      }
    }
    throw error;
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

function buildApiQueryString(
  params: Record<string, string | number | boolean | null | undefined>,
): string {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) {
      return;
    }
    const normalized = String(value).trim();
    if (!normalized) {
      return;
    }
    query.set(key, normalized);
  });
  const serialized = query.toString();
  return serialized ? `?${serialized}` : "";
}

function asObjectRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asNumberRecord(value: unknown): Record<string, number> {
  const source = asObjectRecord(value);
  return Object.fromEntries(
    Object.entries(source)
      .map(([key, item]) => [key, asNumber(item) ?? 0] as const)
      .filter(([, item]) => item > 0),
  );
}

function deriveWorkflowCommandExecutionSummary(record: Record<string, unknown>): WorkflowCommandExecutionSummary {
  return {
    source: asString(record.source),
    fallbackStatus: asString(record.fallback_status),
    fallbackUsed: asBoolean(record.fallback_used) === true,
    moduleStateMutated: asBoolean(record.module_state_mutated) === true,
    activityCount: asNumber(record.activity_count) ?? 0,
    attemptCount: asNumber(record.attempt_count) ?? 0,
    entityDeltaCount: asNumber(record.entity_delta_count) ?? 0,
    activityStatusCounts: asNumberRecord(record.activity_status_counts),
    attemptStatusCounts: asNumberRecord(record.attempt_status_counts),
    entityDeltaStatusCounts: asNumberRecord(record.entity_delta_status_counts),
    entityDeltaKindCounts: asNumberRecord(record.entity_delta_kind_counts),
    latestEffectStatus: asString(record.latest_effect_status),
    latestActivity: asObjectRecord(record.latest_activity),
    latestAttempt: asObjectRecord(record.latest_attempt),
    latestEntityDelta: asObjectRecord(record.latest_entity_delta),
    sampleLimit: asNumber(record.sample_limit) ?? 0,
    sampleTruncated: asBoolean(record.sample_truncated) === true,
  };
}

function deriveWorkflowCommandControlPolicy(record: Record<string, unknown>): WorkflowCommandControlPolicy {
  return {
    commandType: asString(record.command_type),
    owner: asString(record.owner),
    runningControlCategory: asString(record.running_control_category),
    runningControlCategories: asArray(record.running_control_categories).map((item) => asString(item)).filter(Boolean),
    runningControlMaturity: asString(record.running_control_maturity),
    runningControlGapStatus: asString(record.running_control_gap_status),
    runningControlSurface: asString(record.running_control_surface),
    runningCancelBlockedReason: asString(record.running_cancel_blocked_reason),
    runningResumeBlockedReason: asString(record.running_resume_blocked_reason),
    fallbackStatus: asString(record.fallback_status),
    raw: record,
  };
}

function deriveWorkflowCommandRecord(record: Record<string, unknown>): WorkflowCommandRecord {
  const executionSummary = asObjectRecord(record.execution_summary);
  const controlPolicy = asObjectRecord(record.control_policy);
  return {
    commandId: asString(record.command_id),
    workflowRunId: asString(record.workflow_run_id),
    operationId: asString(record.operation_id),
    commandType: asString(record.command_type),
    owner: asString(record.owner),
    agentExposureStatus: asString(record.agent_exposure_status),
    agentExposureGate: asString(record.agent_exposure_gate),
    status: asString(record.status),
    displayContract: asObjectRecord(record.display_contract),
    controlPolicy: deriveWorkflowCommandControlPolicy(controlPolicy),
    controlState: asObjectRecord(record.control_state),
    activitySpinePolicy: asObjectRecord(record.activity_spine_policy),
    executionSummary: Object.keys(executionSummary).length
      ? deriveWorkflowCommandExecutionSummary(executionSummary)
      : undefined,
    raw: record,
  };
}

function deriveWorkflowActivityRecord(record: Record<string, unknown>): WorkflowActivityRecord {
  return {
    activityRunId: asString(record.activity_run_id),
    workflowRunId: asString(record.workflow_run_id),
    operationRunId: asString(record.operation_run_id),
    acquisitionRunId: asString(record.acquisition_run_id),
    commandId: asString(record.command_id),
    activityType: asString(record.activity_type),
    owner: asString(record.owner),
    status: asString(record.status),
    phase: asString(record.phase),
    mutationContract: asString(record.mutation_contract),
    moduleStateMutated: asBoolean(record.module_state_mutated) === true,
    raw: record,
  };
}

function deriveWorkflowActivityAttemptRecord(record: Record<string, unknown>): WorkflowActivityAttemptRecord {
  return {
    attemptId: asString(record.attempt_id),
    activityRunId: asString(record.activity_run_id),
    workflowRunId: asString(record.workflow_run_id),
    commandId: asString(record.command_id),
    activityType: asString(record.activity_type),
    owner: asString(record.owner),
    status: asString(record.status),
    provider: asString(record.provider),
    mutationContract: asString(record.mutation_contract),
    moduleStateMutated: asBoolean(record.module_state_mutated) === true,
    raw: record,
  };
}

function deriveWorkflowEntityDeltaRecord(record: Record<string, unknown>): WorkflowEntityDeltaRecord {
  return {
    deltaId: asString(record.delta_id),
    workflowRunId: asString(record.workflow_run_id),
    operationRunId: asString(record.operation_run_id),
    commandId: asString(record.command_id),
    activityRunId: asString(record.activity_run_id),
    attemptId: asString(record.attempt_id),
    entityType: asString(record.entity_type),
    entityKey: asString(record.entity_key),
    deltaKind: asString(record.delta_kind),
    status: asString(record.status),
    reason: asString(record.reason),
    mutationContract: asString(record.mutation_contract),
    moduleStateMutated: asBoolean(record.module_state_mutated) === true,
    raw: record,
  };
}

function deriveOperationRunStatusSummary(record: Record<string, unknown>): OperationRunStatusSummary {
  const latestCommand = asObjectRecord(record.latest_workflow_command);
  return {
    source: asString(record.source),
    fallbackStatus: asString(record.fallback_status),
    fallbackUsed: asBoolean(record.fallback_used) === true,
    moduleStateMutated: asBoolean(record.module_state_mutated) === true,
    operationStatus: asString(record.operation_status),
    operationPhase: asString(record.operation_phase),
    workflowCommandCount: asNumber(record.workflow_command_count) ?? 0,
    operationEventCount: asNumber(record.operation_event_count) ?? 0,
    commandStatusCounts: asNumberRecord(record.command_status_counts),
    latestEventType: asString(record.latest_event_type),
    latestWorkflowCommand: Object.keys(latestCommand).length
      ? deriveWorkflowCommandRecord(latestCommand)
      : undefined,
  };
}

function deriveOperationRunControlState(record: Record<string, unknown>): OperationRunControlState {
  const disabledReasons = asObjectRecord(record.disabled_reasons);
  return {
    operationStatus: asString(record.operation_status),
    actionStatus: asString(record.action_status),
    operationPhase: asString(record.operation_phase),
    canDispatch: asBoolean(record.can_dispatch) === true,
    canCancel: asBoolean(record.can_cancel) === true,
    canRetry: asBoolean(record.can_retry) === true,
    canResume: asBoolean(record.can_resume) === true,
    allowedActions: asArray(record.allowed_actions).map((item) => String(item)).filter(Boolean),
    disabledReasons: Object.fromEntries(
      Object.entries(disabledReasons).map(([key, value]) => [key, String(value)]),
    ),
    controlSourceOfTruth: asString(record.control_source_of_truth),
    fallbackStatus: asString(record.fallback_status),
    moduleStateMutatedOnControl: asBoolean(record.module_state_mutated_on_control) === true,
    raw: record,
  };
}

function deriveOperationRunRecord(record: Record<string, unknown>): OperationRunRecord {
  const statusSummary = asObjectRecord(record.status_summary);
  const controlState = asObjectRecord(record.control_state);
  return {
    operationRunId: asString(record.operation_run_id),
    actionId: asString(record.action_id),
    ownerModule: asString(record.owner_module),
    operationType: asString(record.operation_type),
    displayContract: asObjectRecord(record.display_contract),
    status: asString(record.status),
    progress: asObjectRecord(record.progress),
    workflowRef: asObjectRecord(record.workflow_ref),
    resultRef: asObjectRecord(record.result_ref),
    controlState: Object.keys(controlState).length ? deriveOperationRunControlState(controlState) : undefined,
    statusSummary: Object.keys(statusSummary).length ? deriveOperationRunStatusSummary(statusSummary) : undefined,
    raw: record,
  };
}

function deriveOperationActionRecord(record: Record<string, unknown>): OperationActionRecord {
  return {
    actionId: asString(record.action_id),
    actionType: asString(record.action_type),
    ownerModule: asString(record.owner_module),
    operationType: asString(record.operation_type),
    displayContract: asObjectRecord(record.display_contract),
    approvalStatus: asString(record.approval_status),
    approvalPolicy: asString(record.approval_policy),
    status: asString(record.status),
    targetRef: asObjectRecord(record.target_ref),
    input: asObjectRecord(record.input),
    createdAt: asString(record.created_at),
    updatedAt: asString(record.updated_at),
    raw: record,
  };
}

function deriveOperationEventRecord(record: Record<string, unknown>): OperationEventRecord {
  return {
    eventId: asString(record.event_id),
    eventType: asString(record.event_type),
    sequenceNumber: asNumber(record.sequence_number) ?? 0,
    actor: asString(record.actor),
    source: asString(record.source),
    payload: asObjectRecord(record.payload),
    recordedAt: asString(record.recorded_at),
    raw: record,
  };
}

export async function listOperationRuns(options?: {
  status?: string;
  ownerModule?: string;
  actionId?: string;
  limit?: number;
  includeStatusSummary?: boolean;
}): Promise<OperationRunRecord[]> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/operations/runs${buildApiQueryString({
      status: options?.status,
      owner_module: options?.ownerModule,
      action_id: options?.actionId,
      limit: options?.limit ?? 50,
      include_status_summary: options?.includeStatusSummary !== false,
    })}`,
  );
  return asArray(payload.operation_runs)
    .map((item) => deriveOperationRunRecord(asObjectRecord(item)))
    .filter((item) => item.operationRunId);
}

export async function listOperationActions(options?: {
  status?: string;
  actionType?: string;
  ownerModule?: string;
  conversationId?: string;
  limit?: number;
}): Promise<OperationActionRecord[]> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/operations/actions${buildApiQueryString({
      status: options?.status,
      action_type: options?.actionType,
      owner_module: options?.ownerModule,
      conversation_id: options?.conversationId,
      limit: options?.limit ?? 50,
    })}`,
  );
  return asArray(payload.actions)
    .map((item) => deriveOperationActionRecord(asObjectRecord(item)))
    .filter((item) => item.actionId);
}

async function postOperationActionDecision(
  actionId: string,
  decision: "approve" | "reject",
  payload?: Record<string, unknown>,
): Promise<OperationActionDecisionResult> {
  const response = await fetchJson<Record<string, unknown>>(
    `/api/operations/actions/${encodeURIComponent(actionId)}/${decision}`,
    {
      method: "POST",
      body: JSON.stringify({
        actor: "frontend-demo",
        source: "operation_queue",
        ...(payload || {}),
      }),
    },
  );
  const action = asObjectRecord(response.action);
  const operationRun = asObjectRecord(response.operation_run);
  return {
    status: asString(response.status),
    action: Object.keys(action).length ? deriveOperationActionRecord(action) : null,
    operationRun: Object.keys(operationRun).length ? deriveOperationRunRecord(operationRun) : null,
    raw: response,
  };
}

export function approveOperationAction(actionId: string): Promise<OperationActionDecisionResult> {
  return postOperationActionDecision(actionId, "approve");
}

export function rejectOperationAction(actionId: string, reason = "rejected_from_operation_queue"): Promise<OperationActionDecisionResult> {
  return postOperationActionDecision(actionId, "reject", { reason });
}

export async function getOperationRunProvenance(operationRunId: string): Promise<OperationRunProvenance> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/operations/runs/${encodeURIComponent(operationRunId)}/provenance`,
  );
  return {
    status: asString(payload.status),
    action: Object.keys(asObjectRecord(payload.action)).length
      ? deriveOperationActionRecord(asObjectRecord(payload.action))
      : null,
    operationRun: Object.keys(asObjectRecord(payload.operation_run)).length
      ? deriveOperationRunRecord(asObjectRecord(payload.operation_run))
      : null,
    actionEvents: asArray(payload.action_events).map((item) => deriveOperationEventRecord(asObjectRecord(item))),
    operationEvents: asArray(payload.operation_events).map((item) => deriveOperationEventRecord(asObjectRecord(item))),
    eventTimeline: asArray(payload.event_timeline).map((item) => deriveOperationEventRecord(asObjectRecord(item))),
    workflowCommands: asArray(payload.workflow_commands).map((item) => deriveWorkflowCommandRecord(asObjectRecord(item))),
    raw: payload,
  };
}

async function postOperationRunControl(operationRunId: string, action: "cancel" | "retry" | "resume" | "dispatch"): Promise<OperationRunRecord | null> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/operations/runs/${encodeURIComponent(operationRunId)}/${action}`,
    {
      method: "POST",
      body: JSON.stringify({ actor: "frontend-demo", source: "operation_queue" }),
    },
  );
  const status = asString(payload.status);
  if (status === "invalid" || status === "not_found" || status === "failed" || status === "unsupported") {
    const reason = asString(payload.reason) || status || "unknown";
    throw new Error(`Operation ${action} failed: ${reason}`);
  }
  const operationRun = asObjectRecord(payload.operation_run);
  if (!Object.keys(operationRun).length) {
    const reason = asString(payload.reason) || status || "missing operation_run";
    throw new Error(`Operation ${action} failed: ${reason}`);
  }
  return deriveOperationRunRecord(operationRun);
}

export function cancelOperationRun(operationRunId: string): Promise<OperationRunRecord | null> {
  return postOperationRunControl(operationRunId, "cancel");
}

export function retryOperationRun(operationRunId: string): Promise<OperationRunRecord | null> {
  return postOperationRunControl(operationRunId, "retry");
}

export function resumeOperationRun(operationRunId: string): Promise<OperationRunRecord | null> {
  return postOperationRunControl(operationRunId, "resume");
}

export function dispatchOperationRun(operationRunId: string): Promise<OperationRunRecord | null> {
  return postOperationRunControl(operationRunId, "dispatch");
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

async function postWorkflowCommandControl(
  commandId: string,
  action: "cancel" | "retry" | "resume",
): Promise<WorkflowCommandRecord | null> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/workflow/commands/${encodeURIComponent(commandId)}/${action}`,
    {
      method: "POST",
      body: JSON.stringify({ actor: "frontend-demo", source: "operation_queue" }),
    },
  );
  const status = asString(payload.status);
  if (status === "invalid" || status === "not_found" || status === "failed" || status === "unsupported") {
    const reason = asString(payload.reason) || asString(payload.command_status) || status || "unknown";
    throw new Error(`Workflow command ${action} failed: ${reason}`);
  }
  const workflowCommand = asObjectRecord(payload.workflow_command);
  if (!Object.keys(workflowCommand).length) {
    const reason = asString(payload.reason) || asString(payload.command_status) || status || "missing workflow_command";
    throw new Error(`Workflow command ${action} failed: ${reason}`);
  }
  for (const field of ["control_state", "display_contract", "control_policy", "activity_spine_policy"]) {
    if (!Object.keys(asObjectRecord(workflowCommand[field])).length) {
      throw new Error(`Workflow command ${action} failed: missing ${field}`);
    }
  }
  return deriveWorkflowCommandRecord(workflowCommand);
}

export function cancelWorkflowCommand(commandId: string): Promise<WorkflowCommandRecord | null> {
  return postWorkflowCommandControl(commandId, "cancel");
}

export function retryWorkflowCommand(commandId: string): Promise<WorkflowCommandRecord | null> {
  return postWorkflowCommandControl(commandId, "retry");
}

export function resumeWorkflowCommand(commandId: string): Promise<WorkflowCommandRecord | null> {
  return postWorkflowCommandControl(commandId, "resume");
}

export async function listWorkflowActivities(filters: {
  commandId?: string;
  operationRunId?: string;
  workflowRunId?: string;
  limit?: number;
}): Promise<WorkflowActivityRecord[]> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/workflow/activities${buildApiQueryString({
      command_id: filters.commandId,
      operation_run_id: filters.operationRunId,
      workflow_run_id: filters.workflowRunId,
      limit: filters.limit,
    })}`,
  );
  return asArray(payload.workflow_activities)
    .map((item) => deriveWorkflowActivityRecord(asObjectRecord(item)))
    .filter((item) => item.activityRunId);
}

export async function listWorkflowActivityAttempts(filters: {
  commandId?: string;
  activityRunId?: string;
  workflowRunId?: string;
  limit?: number;
}): Promise<WorkflowActivityAttemptRecord[]> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/workflow/activity-attempts${buildApiQueryString({
      command_id: filters.commandId,
      activity_run_id: filters.activityRunId,
      workflow_run_id: filters.workflowRunId,
      limit: filters.limit,
    })}`,
  );
  return asArray(payload.workflow_activity_attempts)
    .map((item) => deriveWorkflowActivityAttemptRecord(asObjectRecord(item)))
    .filter((item) => item.attemptId);
}

export async function listWorkflowEntityDeltas(filters: {
  commandId?: string;
  activityRunId?: string;
  attemptId?: string;
  operationRunId?: string;
  workflowRunId?: string;
  limit?: number;
}): Promise<WorkflowEntityDeltaRecord[]> {
  const payload = await fetchJson<Record<string, unknown>>(
    `/api/workflow/entity-deltas${buildApiQueryString({
      command_id: filters.commandId,
      activity_run_id: filters.activityRunId,
      attempt_id: filters.attemptId,
      operation_run_id: filters.operationRunId,
      workflow_run_id: filters.workflowRunId,
      limit: filters.limit,
    })}`,
  );
  return asArray(payload.workflow_entity_deltas)
    .map((item) => deriveWorkflowEntityDeltaRecord(asObjectRecord(item)))
    .filter((item) => item.deltaId);
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

function normalizeDisplayKeywordKey(value: string): string {
  return normalizeKeywordToken(value)
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalizeDisplayKeyword(value: string): string {
  const normalized = normalizeDisplayKeywordKey(value);
  const canonicalMap: Record<string, string> = {
    coding: "Coding",
    "coding agent": "Coding",
    "coding agents": "Coding",
    programming: "Coding",
    math: "Math",
    mathematics: "Math",
    mathematical: "Math",
    text: "Text",
    language: "Text",
    nlp: "Text",
    audio: "Audio",
    speech: "Audio",
    voice: "Audio",
    vision: "Vision",
    visual: "Vision",
    multimodal: "Multimodal",
    multimodality: "Multimodal",
    reasoning: "Reasoning",
    reasoner: "Reasoning",
    rl: "RL",
    "reinforcement learning": "RL",
    eval: "Eval",
    evals: "Eval",
    evaluation: "Eval",
    "model evaluation": "Eval",
    "alignment evaluation": "Eval",
    "pre train": "Pre-train",
    pretraining: "Pre-train",
    "pre training": "Pre-train",
    "post train": "Post-train",
    posttraining: "Post-train",
    "post training": "Post-train",
    "world model": "World model",
    "world models": "World model",
    "world modeling": "World model",
    alignment: "Alignment",
    safety: "Safety",
    infra: "Infra",
    infrastructure: "Infra",
    veo: "Veo",
    "nano banana": "Nano Banana",
    nanobanana: "Nano Banana",
    gemini: "Gemini",
    chatgpt: "ChatGPT",
    claude: "Claude",
    o1: "o1",
  };
  if (canonicalMap[normalized]) {
    return canonicalMap[normalized];
  }
  if (!normalized) {
    return "";
  }
  if (/^[a-z0-9 ]+$/.test(normalized)) {
    return normalized
      .split(" ")
      .filter(Boolean)
      .map((part) => (part.length <= 2 && /\d/.test(part) ? part : part.charAt(0).toUpperCase() + part.slice(1)))
      .join(" ");
  }
  return value.trim();
}

function canonicalizeDisplayKeywords(values: string[]): string[] {
  const results: string[] = [];
  const seen: Set<string> = new Set();
  for (const value of values) {
    const canonical = canonicalizeDisplayKeyword(value);
    const key = normalizeDisplayKeywordKey(canonical);
    if (!canonical || seen.has(key)) {
      continue;
    }
    seen.add(key);
    results.push(canonical);
  }
  return results;
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

function extractPopulationCategories(payload: any): string[] {
  const categorySources = [
    asArray(payload.request_preview?.categories),
    asArray(payload.request?.categories),
  ];
  return dedupeStrings(
    categorySources
      .flat()
      .map((value) => asString(value).trim().toLowerCase())
      .filter(Boolean),
  );
}

function hasTechnicalPopulationIntent(queryText: string, payload: any): boolean {
  const requestPreview = (payload.request_preview as Record<string, unknown>) || {};
  const thematicValues = [
    ...asArray(requestPreview.keywords),
    ...asArray(requestPreview.must_have_keywords),
    ...asArray(requestPreview.must_have_facets),
    ...asArray(requestPreview.must_have_primary_role_buckets),
  ]
    .map((value) => asString(value))
    .filter(Boolean);
  if (thematicValues.length > 0) {
    return true;
  }
  const corpus = [
    queryText,
    pickFirstString((payload.request as Record<string, unknown>) || {}, ["query", "raw_user_request"]),
  ]
    .filter(Boolean)
    .join(" ");
  return containsPattern(corpus, [/方向/u, /focus/i, /topic/i, /working on/i]);
}

function inferPopulationLabel(queryText: string, payload: any): string {
  const corpus = [
    queryText,
    pickFirstString((payload.request as Record<string, unknown>) || {}, ["query", "raw_user_request"]),
    pickFirstString((payload.plan as Record<string, unknown>) || {}, ["target_population", "intent_summary"]),
  ]
    .filter(Boolean)
    .join(" ");
  const categories = new Set(extractPopulationCategories(payload));
  const technicalPopulationIntent = hasTechnicalPopulationIntent(queryText, payload);
  const backgroundLabel = containsPattern(corpus, [/华人/u, /\bChinese\b/i, /中文/u]) ? "华人" : "";
  let roleLabel = "";
  if (categories.has("investor")) {
    roleLabel = "投资人";
  } else if (
    categories.has("researcher")
    && categories.has("engineer")
  ) {
    roleLabel = "研究员/工程师";
  } else if (categories.has("researcher")) {
    roleLabel = "研究员";
  } else if (categories.has("engineer")) {
    roleLabel = "工程师";
  } else if (technicalPopulationIntent) {
    roleLabel = "研究员/工程师";
  } else if (categories.has("employee") || categories.has("former_employee")) {
    roleLabel = "员工/前员工";
  } else if (containsPattern(corpus, [/research engineer/i, /研究工程/u])) {
    roleLabel = "研究工程师";
  } else if (containsPattern(corpus, [/研究员/u, /researcher/i]) && containsPattern(corpus, [/工程师/u, /engineer/i])) {
    roleLabel = "研究员/工程师";
  } else if (containsPattern(corpus, [/工程师/u, /engineer/i])) {
    roleLabel = "工程师";
  } else if (containsPattern(corpus, [/研究员/u, /researcher/i])) {
    roleLabel = "研究员";
  } else {
    roleLabel = "研究员/工程师";
  }
  return `${backgroundLabel}${roleLabel}` || "目标候选人";
}

function inferProjectScopeLabel(queryText: string, payload: any): string {
  const scopeLabels = extractScopeLabels(queryText);
  if (scopeLabels.length > 0) {
    return scopeLabels.join("、");
  }
  const acquisitionStrategy = (payload.plan?.acquisition_strategy as Record<string, unknown>) || {};
  const effectiveExecutionSemantics = (payload.effective_execution_semantics as Record<string, unknown>) || {};
  const strategyType = pickFirstString(acquisitionStrategy, ["strategy_type"]);
  const targetScope =
    pickFirstString((payload.plan as Record<string, unknown>) || {}, ["target_scope"]) ||
    pickFirstString((payload.request_preview as Record<string, unknown>) || {}, ["target_scope"]);
  const effectiveAcquisitionMode = pickFirstString(effectiveExecutionSemantics, ["effective_acquisition_mode"]);
  const defaultResultsMode = pickFirstString(effectiveExecutionSemantics, ["default_results_mode"]);
  if (effectiveAcquisitionMode === "baseline_reuse_with_delta") {
    return "Baseline 全量 + 定向增量范围";
  }
  if (effectiveAcquisitionMode === "scoped_live_search") {
    return "目标公司定向搜索范围";
  }
  if (
    targetScope === "full_company_asset" ||
    effectiveAcquisitionMode === "full_local_asset_reuse" ||
    effectiveAcquisitionMode === "full_live_roster" ||
    defaultResultsMode === "asset_population"
  ) {
    return "目标公司全量范围";
  }
  if (strategyType === "full_company_roster" || targetScope === "full_company_asset") {
    return "目标公司全量范围";
  }
  if (strategyType === "scoped_search_roster") {
    return "目标公司定向搜索范围";
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
  if (containsPattern(corpus, [/\bcoding\b/i, /编程/u])) {
    keywords.push(/\bcoding\b/i.test(corpus) ? "Coding" : "编程");
  }
  if (containsPattern(corpus, [/\binfra\b/i, /infrastructure/i, /基础设施/u])) {
    keywords.push(/\binfra\b/i.test(corpus) ? "Infra" : "基础设施");
  }
  if (containsPattern(corpus, [/\bpre[\s-]?train/i, /预训练/u])) {
    keywords.push(/pre/i.test(corpus) ? "Pre-train" : "预训练");
  }
  if (containsPattern(corpus, [/\bpost[\s-]?train/i, /后训练/u])) {
    keywords.push(/post/i.test(corpus) ? "Post-train" : "后训练");
  }
  if (containsPattern(corpus, [/\breasoning\b/i, /推理/u])) {
    keywords.push(/\breasoning\b/i.test(corpus) ? "Reasoning" : "推理");
  }
  if (containsPattern(corpus, [/\bworld model/i, /\bworld models/i, /世界模型/u])) {
    keywords.push(/world/i.test(corpus) ? "World model" : "世界模型");
  }
  if (containsPattern(corpus, [/\balignment\b/i, /对齐/u])) {
    keywords.push(/\balignment\b/i.test(corpus) ? "Alignment" : "对齐");
  }
  if (containsPattern(corpus, [/\bsafety\b/i, /安全/u])) {
    keywords.push(/\bsafety\b/i.test(corpus) ? "Safety" : "安全");
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
  return canonicalizeDisplayKeywords(dedupeStrings([...keywords, ...fallbackKeywords])).slice(0, 6);
}

function normalizeKeywordToken(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

const GENERIC_RECALL_KEYWORD_TOKENS = new Set([
  "research",
  "researcher",
  "researchers",
  "engineer",
  "engineers",
  "engineering",
  "employee",
  "employees",
  "people",
  "member",
  "members",
  "team",
  "teams",
  "研究",
  "研究员",
  "工程师",
  "员工",
  "成员",
  "团队",
  "linkedin employee",
  "linkedin employees",
  "linkedin people",
  "linkedin member",
  "linkedin members",
]);

const ROLE_BUCKET_DISPLAY_KEYWORD_TOKENS = new Set([
  "infra_systems",
  "infra systems",
  "product_management",
  "product management",
  "engineering",
  "research",
  "founding",
  "recruiting",
  "ops",
]);

function filterDisplayIntentKeywords(values: string[]): string[] {
  const deduped = dedupeStrings(values);
  return deduped.filter((value) => {
    const token = normalizeDisplayKeywordKey(value);
    if (GENERIC_RECALL_KEYWORD_TOKENS.has(token) || ROLE_BUCKET_DISPLAY_KEYWORD_TOKENS.has(token)) {
      return false;
    }
    const parts = token.split(" ").filter(Boolean);
    const suffixes = parts.map((_, index) => parts.slice(index).join(" "));
    if (parts.length > 1 && suffixes.slice(1).some((suffix) => GENERIC_RECALL_KEYWORD_TOKENS.has(suffix))) {
      return false;
    }
    return true;
  });
}

function distinctMatchedKeywords(items: unknown[]): string[] {
  return canonicalizeDisplayKeywords(
    dedupeStrings(
      items
        .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
        .map((item) => firstNonEmptyString([pickFirstString(item, ["keyword"]), pickFirstString(item, ["matched_on"])]))
        .filter(Boolean),
    ),
  );
}

function normalizeSourceMatches(...sources: unknown[]): CandidateSourceMatch[] {
  const matches: CandidateSourceMatch[] = [];
  const seen = new Set<string>();
  for (const source of sources) {
    for (const item of asArray(source)) {
      const record = ((item && typeof item === "object" ? item : {}) as Record<string, unknown>);
      const matchedOn = pickFirstString(record, ["matched_on", "keyword"]);
      const field = pickFirstString(record, ["field"]);
      const sourceType = pickFirstString(record, ["source_type"]);
      const sourceQuery = pickFirstString(record, ["source_query", "query"]);
      const matchedKeywords = splitStructuredText(record.matched_keywords);
      if (!matchedOn && matchedKeywords.length === 0) {
        continue;
      }
      const key = [matchedOn, field, sourceType, sourceQuery].join("|").toLowerCase();
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      matches.push({
        ...record,
        ...(field ? { field } : {}),
        matched_on: matchedOn || matchedKeywords[0],
        ...(sourceType ? { source_type: sourceType } : {}),
        ...(sourceQuery ? { source_query: sourceQuery } : {}),
        ...(matchedKeywords.length > 0 ? { matched_keywords: matchedKeywords } : {}),
      });
    }
  }
  return matches;
}

function sourceMatchKeywords(sourceMatches: CandidateSourceMatch[]): string[] {
  return sourceMatches.flatMap((source) => [
    asString(source.matched_on),
    ...splitStructuredText(source.matched_keywords),
  ]).filter(Boolean);
}

function buildLayeredSegmentationOptions(candidates: Candidate[]): DashboardData["layers"] {
  const hasLayerMetadata = candidates.some((candidate) => typeof candidate.outreachLayer === "number");
  return [0, 1, 2, 3].map((layer) => ({
    id: `layer_${layer}`,
    label: `Layer ${layer}`,
    count: hasLayerMetadata ? candidates.filter((candidate) => candidate.outreachLayer === layer).length : 0,
  }));
}

function mapCandidateFacetOptions(source: unknown): DashboardData["layers"] {
  return asArray(source)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .map((item) => ({
      id: pickFirstString(item, ["id"]),
      label: pickFirstString(item, ["label"]),
      count: Number(item.count || 0) || 0,
    }))
    .filter((item) => item.id && item.label);
}

function mapCandidateFacetSummary(source: unknown): CandidateFacetSummary | undefined {
  const record = source && typeof source === "object" ? (source as Record<string, unknown>) : {};
  if (!record || Object.keys(record).length === 0) {
    return undefined;
  }
  return {
    schemaVersion: Number(record.schema_version || record.schemaVersion || 0) || undefined,
    candidateCount: Number(record.candidate_count || record.candidateCount || 0) || undefined,
    layers: mapCandidateFacetOptions(record.layers),
    recall: mapCandidateFacetOptions(record.recall),
    employment: mapCandidateFacetOptions(record.employment),
    locations: mapCandidateFacetOptions(record.locations),
    functions: mapCandidateFacetOptions(record.functions),
  };
}

function mapCandidateFacetSummaryScope(source: Record<string, unknown>, fallback = ""): string {
  const facetSummary =
    source.facet_summary && typeof source.facet_summary === "object" && !Array.isArray(source.facet_summary)
      ? (source.facet_summary as Record<string, unknown>)
      : {};
  return asString(
    source.facet_summary_scope ||
      facetSummary.count_scope ||
      facetSummary.facet_count_scope ||
      facetSummary.scope ||
      fallback,
  );
}

function isCanonicalFacetSummaryScope(scope: string | undefined): boolean {
  const normalized = asString(scope);
  return normalized === "global_full_population" || normalized === "exact_projection";
}

function candidateFacetSummaryCompletenessScore(summary: CandidateFacetSummary | undefined): number {
  if (!summary) {
    return 0;
  }
  const candidateCount = Math.max(0, Number(summary.candidateCount || 0) || 0);
  const sectionScore = [
    summary.layers,
    summary.recall,
    summary.employment,
    summary.locations,
    summary.functions,
  ].reduce((score, options) => {
    if (!options || options.length === 0) {
      return score;
    }
    const countTotal = options.reduce((sum, option) => sum + Math.max(0, Number(option.count || 0) || 0), 0);
    return score + 1_000 + Math.min(countTotal, Math.max(candidateCount, countTotal));
  }, 0);
  return candidateCount * 100_000 + sectionScore;
}

function candidateFacetLayerZeroCount(summary: CandidateFacetSummary | undefined): number {
  const layerZero = (summary?.layers || []).find((option) => option.id === "layer_0");
  return Math.max(0, Number(layerZero?.count || 0) || 0);
}

function candidateFacetSummaryMatchesCanonicalBoard(
  summary: CandidateFacetSummary | undefined,
  scope: string | undefined,
  boardRuntimeState: BoardRuntimeState | undefined,
): boolean {
  const normalizedScope = asString(scope);
  if (!summary || !isCanonicalFacetSummaryScope(normalizedScope)) {
    return false;
  }
  const expectedCount = Math.max(0, Number(boardRuntimeState?.expectedCandidateCount || 0) || 0);
  if (expectedCount <= 0) {
    return Math.max(0, Number(summary.candidateCount || 0) || 0) > 0;
  }
  if (Math.max(0, Number(summary.candidateCount || 0) || 0) < expectedCount) {
    return false;
  }
  if (
    normalizedScope === "global_full_population" &&
    (summary.layers || []).length > 0 &&
    candidateFacetLayerZeroCount(summary) !== expectedCount
  ) {
    return false;
  }
  if (boardRuntimeState) {
    const boardFacetCount = Math.max(0, Number(boardRuntimeState.facetSummaryCandidateCount || 0) || 0);
    if (
      boardRuntimeState.facetSummaryStatus === "complete" &&
      isCanonicalFacetSummaryScope(boardRuntimeState.facetSummaryScope) &&
      boardFacetCount > 0 &&
      boardFacetCount !== expectedCount
    ) {
      return false;
    }
  }
  return true;
}

function pickCanonicalCandidateFacetSummary(
  incoming: CandidateFacetSummary | undefined,
  current: CandidateFacetSummary | undefined,
): CandidateFacetSummary | undefined {
  if (!incoming) {
    return current;
  }
  if (!current) {
    return incoming;
  }
  return candidateFacetSummaryCompletenessScore(incoming) >= candidateFacetSummaryCompletenessScore(current)
    ? incoming
    : current;
}

function parseOutreachLayer(...values: unknown[]): number | null {
  for (const value of values) {
    if (value === null || value === undefined || value === "") {
      continue;
    }
    const parsed = typeof value === "number" ? value : Number(value);
    if (Number.isFinite(parsed)) {
      return Math.trunc(parsed);
    }
  }
  return null;
}

function extractDashboardIntentKeywords(payload: any, candidates: Candidate[], targetCompany: string): string[] {
  const requestPreview = (payload.request_preview as Record<string, unknown>) || {};
  const rawRequest = firstNonEmptyString([
    pickFirstString(payload?.job?.request || {}, ["raw_user_request", "query"]),
    pickFirstString(requestPreview, ["raw_user_request"]),
  ]);
  const normalizedRawRequest = normalizeKeywordToken(rawRequest).replace(/[^\p{L}\p{N}\s]+/gu, " ");
  const previewKeywordValues = asArray(requestPreview.keywords).map((value) => asString(value)).filter(Boolean);
  const previewKeywords = dedupeStrings(
    [
      ...previewKeywordValues,
      ...asArray(requestPreview.organization_keywords)
        .map((value) => asString(value))
        .filter((value) => {
          if (!value) {
            return false;
          }
          const normalizedValue = normalizeKeywordToken(value);
          if (previewKeywordValues.some((keyword) => normalizeKeywordToken(keyword) === normalizedValue)) {
            return true;
          }
          return normalizedRawRequest.includes(normalizedValue);
        }),
    ],
  );
  const inferredKeywords = inferKeywordLabels(rawRequest, payload);
  const blocked = new Set(
    [targetCompany, targetCompany.replace(/\s+/g, ""), pickFirstString(payload?.job?.request || {}, ["target_company"])]
      .filter(Boolean)
      .map((value) => normalizeDisplayKeywordKey(value)),
  );
  return canonicalizeDisplayKeywords(
    filterDisplayIntentKeywords(
      dedupeStrings([...previewKeywords, ...inferredKeywords])
        .filter((value) => !blocked.has(normalizeDisplayKeywordKey(value))),
    ),
  )
    .slice(0, 8);
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

function translateAcquisitionModeLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  const mappings: Record<string, string> = {
    full_company_roster: "全量 live roster",
    hybrid: "Baseline 复用 + 增量采集",
    scoped_search_roster: "定向搜索 roster",
    former_employee_search: "前员工定向搜索",
  };
  return mappings[normalized] || value || "待定";
}

function translateEffectiveAcquisitionModeLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  const mappings: Record<string, string> = {
    full_local_asset_reuse: "全量本地资产复用",
    baseline_reuse_with_delta: "Baseline 复用 + 缺口增量",
    baseline_reuse_ranked_retrieval: "Baseline 复用 + 排名检索",
    scoped_live_search: "定向 live search",
    full_live_roster: "全量 live roster",
    hybrid_live_acquisition: "Hybrid live acquisition",
  };
  return mappings[normalized] || "";
}

function translateLaneBehaviorLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  const mappings: Record<string, string> = {
    reuse_baseline: "复用本地 baseline",
    delta_acquisition: "只补缺口增量",
    live_acquisition: "直接实时采集",
    not_requested: "本次不请求",
  };
  return mappings[normalized] || value || "待定";
}

function translateDispatchStrategyLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  const mappings: Record<string, string> = {
    reuse_snapshot: "直接复用 snapshot",
    delta_from_snapshot: "基于 snapshot 补 delta",
    join_inflight: "复用进行中的 workflow",
    reuse_completed: "复用历史完成结果",
    new_job: "新建 workflow",
  };
  return mappings[normalized] || value || "待定";
}

function resolvePlanAcquisitionStrategyLabel(options: {
  effectiveExecutionSemantics?: Record<string, unknown>;
  assetReusePlan?: Record<string, unknown>;
  organizationExecutionProfile?: Record<string, unknown>;
  dispatchPreview?: Record<string, unknown>;
  queryText?: string;
  payload?: any;
}): string {
  const effectiveExecutionSemantics = options.effectiveExecutionSemantics || {};
  const assetReusePlan = options.assetReusePlan || {};
  const organizationExecutionProfile = options.organizationExecutionProfile || {};
  const dispatchPreview = options.dispatchPreview || {};
  const backendExecutionStrategyLabel = pickFirstString(effectiveExecutionSemantics, [
    "execution_strategy_label",
    "strategy_label",
  ]);
  if (backendExecutionStrategyLabel) {
    return backendExecutionStrategyLabel;
  }
  const effectiveAcquisitionMode = pickFirstString(effectiveExecutionSemantics, ["effective_acquisition_mode"]);
  const dispatchStrategy = pickFirstString(dispatchPreview, ["strategy"]);
  const plannerMode = pickFirstString(assetReusePlan, ["planner_mode"]);
  const baselinePopulationDefaultReuse = Boolean(assetReusePlan.baseline_population_default_reuse_sufficient);
  const fullLocalReuseResolved =
    effectiveAcquisitionMode === "full_local_asset_reuse" ||
    ((dispatchStrategy === "reuse_snapshot" ||
      dispatchStrategy === "reuse_completed" ||
      plannerMode === "reuse_snapshot_only")
      && Boolean(assetReusePlan.baseline_reuse_available)
      && !Boolean(assetReusePlan.requires_delta_acquisition)
      && (
        baselinePopulationDefaultReuse ||
        Boolean(assetReusePlan.baseline_full_company_coverage_proven) ||
        pickFirstString(organizationExecutionProfile, ["current_lane_default"]) === "reuse_baseline"
      ));
  if (fullLocalReuseResolved) {
    return "全量本地资产复用";
  }
  if (effectiveAcquisitionMode) {
    return (
      translateEffectiveAcquisitionModeLabel(effectiveAcquisitionMode) ||
      translateAcquisitionModeLabel(pickFirstString(organizationExecutionProfile, ["default_acquisition_mode"]))
    );
  }
  const acquisitionMode = pickFirstString(organizationExecutionProfile, ["default_acquisition_mode"]);
  return acquisitionMode
    ? translateAcquisitionModeLabel(acquisitionMode)
    : inferStrategyLabel(options.queryText || "", options.payload || {});
}

export function __testResolvePlanAcquisitionStrategyLabel(payload: any, queryText = ""): string {
  return resolvePlanAcquisitionStrategyLabel({
    effectiveExecutionSemantics: (payload?.effective_execution_semantics as Record<string, unknown>) || {},
    assetReusePlan: (payload?.asset_reuse_plan as Record<string, unknown>) || {},
    organizationExecutionProfile: (payload?.organization_execution_profile as Record<string, unknown>) || {},
    dispatchPreview: (payload?.dispatch_preview as Record<string, unknown>) || {},
    queryText,
    payload,
  });
}

export function __testMapPlanPayloadToDemoPlan(payload: any, queryText = ""): DemoPlan {
  return mapPlanPayloadToDemoPlan(payload, queryText, payload);
}

function translateOrganizationScaleBand(value: string): string {
  const normalized = value.trim().toLowerCase();
  const mappings: Record<string, string> = {
    small: "小型组织",
    medium: "中型组织",
    large: "大型组织",
  };
  return mappings[normalized] || value || "未识别";
}

function explainKeywords(payload: any): string[] {
  const requestPreview = (payload?.request_preview as Record<string, unknown>) || {};
  const keywords = asArray(requestPreview.keywords).map((value) => asString(value)).filter(Boolean);
  const organizationKeywords = asArray(requestPreview.organization_keywords).map((value) => asString(value)).filter(Boolean);
  return canonicalizeDisplayKeywords(dedupeStrings([...keywords, ...organizationKeywords])).slice(0, 8);
}

function extractSearchQueryBundleKeywords(values: unknown): string[] {
  const keywords: string[] = [];
  for (const item of asArray(values)) {
    if (typeof item === "string") {
      keywords.push(asString(item));
      continue;
    }
    const record = item && typeof item === "object" && !Array.isArray(item)
      ? (item as Record<string, unknown>)
      : {};
    keywords.push(
      pickFirstString(record, [
        "search_query",
        "query",
        "query_text",
        "keyword",
        "scope_keyword",
        "focus",
      ]),
    );
    keywords.push(...asArray(record.keywords).map((value) => asString(value)));
    const matchingRequest = (record.matching_family_request as Record<string, unknown>) || {};
    keywords.push(...asArray(matchingRequest.keywords).map((value) => asString(value)));
  }
  return keywords.filter(Boolean);
}

function mapProviderExecutionLanes(payload: any): ProviderExecutionLanePreview[] {
  const plan = (payload?.plan as Record<string, unknown>) || {};
  const acquisitionStrategy = (plan.acquisition_strategy as Record<string, unknown>) || {};
  const metadata = (payload?.metadata as Record<string, unknown>) || {};
  const manifest =
    (acquisitionStrategy.provider_execution_manifest as Record<string, unknown>) ||
    (plan.provider_execution_manifest as Record<string, unknown>) ||
    (payload?.provider_execution_manifest as Record<string, unknown>) ||
    (metadata.provider_execution_manifest as Record<string, unknown>) ||
    {};
  return asArray(manifest.lanes)
    .map((value) => {
      const record = value && typeof value === "object" && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : {};
      const companyFilters: Record<string, string[]> = {};
      const rawFilters = record.company_filters && typeof record.company_filters === "object" && !Array.isArray(record.company_filters)
        ? (record.company_filters as Record<string, unknown>)
        : {};
      for (const [key, values] of Object.entries(rawFilters)) {
        const normalizedValues = asArray(values).map((item) => asString(item)).filter(Boolean);
        if (normalizedValues.length > 0) {
          companyFilters[key] = normalizedValues;
        }
      }
      const lane: ProviderExecutionLanePreview = {
        laneId: pickFirstString(record, ["lane_id"]),
        employmentStatus: pickFirstString(record, ["employment_status"]),
        provider: pickFirstString(record, ["provider"]),
        operation: pickFirstString(record, ["operation"]),
        queryTexts: asArray(record.query_texts).map((item) => asString(item)).filter(Boolean),
        companyFilters,
        providerFacingQuery: Boolean(record.provider_facing_query),
        displayLabel: pickFirstString(record, ["display_label"]),
        reason: pickFirstString(record, ["reason"]),
      };
      return lane.provider || lane.operation || lane.queryTexts.length > 0 || Object.keys(lane.companyFilters).length > 0
        ? lane
        : null;
    })
    .filter((value): value is ProviderExecutionLanePreview => Boolean(value));
}

function extractProviderExecutionQueryKeywords(lanes: ProviderExecutionLanePreview[]): string[] {
  return canonicalizeDisplayKeywords(
    filterDisplayIntentKeywords(
      dedupeStrings(
        lanes
          .filter((lane) => lane.providerFacingQuery)
          .flatMap((lane) => lane.queryTexts),
      ),
    ),
  ).slice(0, 8);
}

function extractPlanKeywordLabels(
  payload: any,
  requestPreview: Record<string, unknown>,
  queryText: string,
  providerExecutionLanes: ProviderExecutionLanePreview[] = [],
): string[] {
  const plan = (payload?.plan as Record<string, unknown>) || {};
  const request = (payload?.request as Record<string, unknown>) || {};
  const acquisitionStrategy = (plan.acquisition_strategy as Record<string, unknown>) || {};
  const filterHints = (acquisitionStrategy.filter_hints as Record<string, unknown>) || {};
  const searchStrategy = (plan.search_strategy as Record<string, unknown>) || {};

  const manifestKeywords = extractProviderExecutionQueryKeywords(providerExecutionLanes);
  if (manifestKeywords.length > 0) {
    return manifestKeywords;
  }

  const providerQueryKeywords = canonicalizeDisplayKeywords(
    filterDisplayIntentKeywords(
      dedupeStrings([
        ...asArray(acquisitionStrategy.search_seed_queries).map((value) => asString(value)),
        ...asArray(acquisitionStrategy.search_queries).map((value) => asString(value)),
        ...extractSearchQueryBundleKeywords(acquisitionStrategy.search_query_bundles),
        ...extractSearchQueryBundleKeywords(searchStrategy.query_bundles),
      ]),
    ),
  );
  if (providerQueryKeywords.length > 0) {
    return providerQueryKeywords.slice(0, 8);
  }

  const requestKeywords = canonicalizeDisplayKeywords(
    filterDisplayIntentKeywords(
      dedupeStrings([
        ...asArray(requestPreview.keywords).map((value) => asString(value)),
        ...asArray(request.keywords).map((value) => asString(value)),
        ...asArray(filterHints.keywords).map((value) => asString(value)),
        ...asArray(requestPreview.must_have_keywords).map((value) => asString(value)),
        ...asArray(request.must_have_keywords).map((value) => asString(value)),
      ]),
    ),
  );
  if (requestKeywords.length > 0) {
    return requestKeywords.slice(0, 8);
  }

  const previewKeywords = explainKeywords({ ...payload, request_preview: requestPreview });
  return previewKeywords.length > 0 ? previewKeywords : inferKeywordLabels(queryText, payload);
}

function buildExecutionNotes(explainPayload: any): string[] {
  const notes: string[] = [];
  const organizationExecutionProfile = (explainPayload?.organization_execution_profile as Record<string, unknown>) || {};
  const assetReusePlan = (explainPayload?.asset_reuse_plan as Record<string, unknown>) || {};
  const effectiveExecutionSemantics = (explainPayload?.effective_execution_semantics as Record<string, unknown>) || {};
  const lanePreview = (explainPayload?.lane_preview as Record<string, unknown>) || {};
  const currentLane = ((lanePreview.current as Record<string, unknown>) || {});
  const formerLane = ((lanePreview.former as Record<string, unknown>) || {});
  const currentBehavior = translateLaneBehaviorLabel(pickFirstString(currentLane, ["planned_behavior"]));
  const formerBehavior = translateLaneBehaviorLabel(pickFirstString(formerLane, ["planned_behavior"]));
  if (currentBehavior) {
    notes.push(`Current lane: ${currentBehavior}`);
  }
  if (formerBehavior && pickFirstString(formerLane, ["planned_behavior"]) !== "not_requested") {
    notes.push(`Former lane: ${formerBehavior}`);
  }
  const baselineSnapshotId = pickFirstString(assetReusePlan, ["baseline_snapshot_id"]);
  if (baselineSnapshotId) {
    notes.push(`Baseline snapshot: ${baselineSnapshotId}`);
  }
  const plannerMode = pickFirstString(assetReusePlan, ["planner_mode"]);
  if (plannerMode) {
    notes.push(`Planner mode: ${plannerMode}`);
  }
  const effectiveAcquisitionMode = translateEffectiveAcquisitionModeLabel(
    pickFirstString(effectiveExecutionSemantics, ["effective_acquisition_mode"]),
  );
  if (effectiveAcquisitionMode) {
    notes.push(`本次执行: ${effectiveAcquisitionMode}`);
  }
  const orgScaleBand = pickFirstString(organizationExecutionProfile, ["org_scale_band"]);
  if (orgScaleBand) {
    notes.push(`组织规模: ${translateOrganizationScaleBand(orgScaleBand)}`);
  }
  return notes.slice(0, 4);
}

function describeExecutionModeHints(payload: any): string[] {
  const hints = (payload?.plan_review_gate?.execution_mode_hints as Record<string, unknown>) || {};
  const summaries: string[] = [];
  const shardCount = Number(hints.segmented_company_employee_shard_count || 0);
  if (shardCount > 0) {
    summaries.push(`当前 live roster 会拆成 ${shardCount} 个 shard。`);
  }
  if (hints.incremental_rerun_recommended) {
    summaries.push("推荐优先复用已有 baseline，只补最小增量。");
  }
  if (hints.adaptive_probe_required_before_live_roster) {
    summaries.push("如需 fresh roster，会先做 probe 再自动分片。");
  }
  return summaries.slice(0, 3);
}

function collectScopeHints(payload: any, requestPreview: Record<string, unknown>): string[] {
  const gateScope = ((payload?.plan_review_gate?.scope_disambiguation as Record<string, unknown>) || {});
  const requestScope = ((requestPreview.scope_disambiguation as Record<string, unknown>) || {});
  return dedupeStrings([
    ...asArray(gateScope.hints).map((value) => asString(value)).filter(Boolean),
    ...asArray(gateScope.sub_org_candidates).map((value) => asString(value)).filter(Boolean),
    ...asArray(requestScope.sub_org_candidates).map((value) => asString(value)).filter(Boolean),
    ...asArray(requestPreview.organization_keywords).map((value) => asString(value)).filter(Boolean),
  ]).slice(0, 6);
}

function asOptionalBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "on"].includes(normalized)) {
      return true;
    }
    if (["false", "0", "no", "off"].includes(normalized)) {
      return false;
    }
  }
  return undefined;
}

function mapTargetCompanyIdentity(payload: unknown): TargetCompanyIdentityPreview | undefined {
  const identity = (payload as Record<string, unknown>) || {};
  const linkedinCompanyUrl = pickFirstString(identity, ["linkedin_company_url"]);
  const linkedinSlug = pickFirstString(identity, ["linkedin_slug"]);
  const canonicalName = pickFirstString(identity, ["canonical_name"]);
  const requestedName = pickFirstString(identity, ["requested_name"]);
  const companyKey = pickFirstString(identity, ["company_key"]);
  if (!linkedinCompanyUrl && !linkedinSlug && !canonicalName && !requestedName && !companyKey) {
    return undefined;
  }
  return {
    requestedName,
    canonicalName,
    companyKey,
    linkedinSlug,
    linkedinCompanyUrl,
    domain: pickFirstString(identity, ["domain"]),
    resolver: pickFirstString(identity, ["resolver"]),
    confidence: pickFirstString(identity, ["confidence"]),
    localAssetAvailable: Boolean(identity.local_asset_available),
  };
}

function mapPlanReviewGate(payload: any, requestPreview: Record<string, unknown>): PlanReviewGate {
  const gate = (payload?.plan_review_gate as Record<string, unknown>) || {};
  return {
    status: pickFirstString(gate, ["status"]) || "ready",
    requiredBeforeExecution: Boolean(gate.required_before_execution),
    riskLevel: pickFirstString(gate, ["risk_level"]) || "low",
    reasons: asArray(gate.reasons).map((value) => asString(value)).filter(Boolean),
    confirmationItems: asArray(gate.confirmation_items).map((value) => asString(value)).filter(Boolean),
    editableFields: asArray(gate.editable_fields)
      .map((value) => asString(value))
      .filter(Boolean) as PlanReviewEditableField[],
    suggestedActions: asArray(gate.suggested_actions).map((value) => asString(value)).filter(Boolean),
    scopeHints: collectScopeHints(payload, requestPreview),
    executionModeHints: describeExecutionModeHints(payload),
  };
}

function mapPlanReviewDecisionDefaults(
  payload: any,
  requestPreview: Record<string, unknown>,
  reviewGate: PlanReviewGate,
): PlanReviewDecision {
  const intentAxes = ((requestPreview.intent_axes as Record<string, unknown>) || {});
  const scopeBoundary = ((intentAxes.scope_boundary as Record<string, unknown>) || {});
  const acquisitionLanePolicy = ((intentAxes.acquisition_lane_policy as Record<string, unknown>) || {});
  const fallbackPolicy = ((intentAxes.fallback_policy as Record<string, unknown>) || {});
  const executionHints = ((payload?.plan_review_gate?.execution_mode_hints as Record<string, unknown>) || {});
  const recommendedPatch = ((executionHints.recommended_decision_patch as Record<string, unknown>) || {});
  const defaultScope = asArray(scopeBoundary.confirmed_company_scope).map((value) => asString(value)).filter(Boolean);
  const targetCompanyIdentity = mapTargetCompanyIdentity(requestPreview.target_company_identity);
  const persistedManualCompanyLinkedinUrl =
    targetCompanyIdentity?.resolver === "manual_review_override"
      ? targetCompanyIdentity.linkedinCompanyUrl || ""
      : "";

  return {
    confirmedCompanyScope: defaultScope,
    targetCompanyLinkedinUrl: persistedManualCompanyLinkedinUrl,
    extraSourceFamilies: [],
    precisionRecallBias: pickFirstString(fallbackPolicy, ["precision_recall_bias"]),
    acquisitionStrategyOverride:
      pickFirstString(acquisitionLanePolicy, ["acquisition_strategy_override"]) ||
      pickFirstString(recommendedPatch, ["acquisition_strategy_override"]),
    useCompanyEmployeesLane:
      asOptionalBoolean(acquisitionLanePolicy.use_company_employees_lane) ??
      asOptionalBoolean(recommendedPatch.use_company_employees_lane),
    keywordPriorityOnly:
      asOptionalBoolean(acquisitionLanePolicy.keyword_priority_only) ??
      asOptionalBoolean(recommendedPatch.keyword_priority_only),
    formerKeywordQueriesOnly:
      asOptionalBoolean(acquisitionLanePolicy.former_keyword_queries_only) ??
      asOptionalBoolean(recommendedPatch.former_keyword_queries_only),
    providerPeopleSearchQueryStrategy:
      pickFirstString(fallbackPolicy, ["provider_people_search_query_strategy"]) || "all_queries_union",
    providerPeopleSearchMaxQueries:
      typeof fallbackPolicy.provider_people_search_max_queries === "number"
        ? Number(fallbackPolicy.provider_people_search_max_queries)
        : null,
    largeOrgKeywordProbeMode:
      asOptionalBoolean(acquisitionLanePolicy.large_org_keyword_probe_mode) ??
      asOptionalBoolean(recommendedPatch.large_org_keyword_probe_mode),
    forceFreshRun: asOptionalBoolean(fallbackPolicy.force_fresh_run),
    reuseExistingRoster:
      asOptionalBoolean(fallbackPolicy.reuse_existing_roster) ??
      asOptionalBoolean(recommendedPatch.reuse_existing_roster),
    runFormerSearchSeed:
      asOptionalBoolean(fallbackPolicy.run_former_search_seed) ??
      asOptionalBoolean(recommendedPatch.run_former_search_seed),
  };
}

export function planReviewDecisionToApiPayload(
  decision: PlanReviewDecision,
  editableFields: PlanReviewEditableField[],
): Record<string, unknown> {
  const allowed = new Set(editableFields);
  const payload: Record<string, unknown> = {};
  if (allowed.has("company_scope") && decision.confirmedCompanyScope.length > 0) {
    payload.confirmed_company_scope = decision.confirmedCompanyScope;
  }
  if (allowed.has("target_company_linkedin_url") && decision.targetCompanyLinkedinUrl?.trim()) {
    payload.target_company_linkedin_url = decision.targetCompanyLinkedinUrl.trim();
  }
  if (allowed.has("extra_source_families") && decision.extraSourceFamilies.length > 0) {
    payload.extra_source_families = decision.extraSourceFamilies;
  }
  if (allowed.has("precision_recall_bias") && decision.precisionRecallBias) {
    payload.precision_recall_bias = decision.precisionRecallBias;
  }
  if (allowed.has("acquisition_strategy_override") && decision.acquisitionStrategyOverride) {
    payload.acquisition_strategy_override = decision.acquisitionStrategyOverride;
  }
  if (allowed.has("use_company_employees_lane") && decision.useCompanyEmployeesLane !== undefined) {
    payload.use_company_employees_lane = decision.useCompanyEmployeesLane;
  }
  if (allowed.has("keyword_priority_only") && decision.keywordPriorityOnly !== undefined) {
    payload.keyword_priority_only = decision.keywordPriorityOnly;
  }
  if (allowed.has("former_keyword_queries_only") && decision.formerKeywordQueriesOnly !== undefined) {
    payload.former_keyword_queries_only = decision.formerKeywordQueriesOnly;
  }
  if (
    allowed.has("provider_people_search_query_strategy")
    && decision.providerPeopleSearchQueryStrategy
  ) {
    payload.provider_people_search_query_strategy = decision.providerPeopleSearchQueryStrategy;
  }
  if (
    allowed.has("provider_people_search_max_queries")
    && typeof decision.providerPeopleSearchMaxQueries === "number"
    && Number.isFinite(decision.providerPeopleSearchMaxQueries)
  ) {
    payload.provider_people_search_max_queries = decision.providerPeopleSearchMaxQueries;
  }
  if (allowed.has("large_org_keyword_probe_mode") && decision.largeOrgKeywordProbeMode !== undefined) {
    payload.large_org_keyword_probe_mode = decision.largeOrgKeywordProbeMode;
  }
  if (allowed.has("force_fresh_run") && decision.forceFreshRun !== undefined) {
    payload.force_fresh_run = decision.forceFreshRun;
  }
  if (allowed.has("reuse_existing_roster") && decision.reuseExistingRoster !== undefined) {
    payload.reuse_existing_roster = decision.reuseExistingRoster;
  }
  if (allowed.has("run_former_search_seed") && decision.runFormerSearchSeed !== undefined) {
    payload.run_former_search_seed = decision.runFormerSearchSeed;
  }
  return payload;
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

function mapPlanPayloadToDemoPlan(payload: any, queryText: string, explainPayload?: any): DemoPlan {
  const explain = explainPayload || {};
  const metadata = (payload.metadata as Record<string, unknown>) || {};
  const acquisitionStrategy = (payload.plan?.acquisition_strategy as Record<string, unknown>) || {};
  const searchStrategyLabels = mapSearchBundlesToLabels(payload);
  const fallbackSearchChannels = asArray(acquisitionStrategy.search_channel_order)
    .map((value) => asString(value))
    .filter(Boolean);
  const lanePreview =
    (explain.lane_preview as Record<string, unknown>) ||
    (payload.lane_preview as Record<string, unknown>) ||
    (metadata.lane_preview as Record<string, unknown>) ||
    {};
  const currentLane = ((lanePreview.current as Record<string, unknown>) || {});
  const formerLane = ((lanePreview.former as Record<string, unknown>) || {});
  const explainSearchChannels = [
    translateLaneBehaviorLabel(pickFirstString(currentLane, ["planned_behavior"])),
    translateLaneBehaviorLabel(pickFirstString(formerLane, ["planned_behavior"])),
  ].filter((value) => value && value !== "本次不请求");
  const configuredSearchStrategy =
    explainSearchChannels.length > 0
      ? explainSearchChannels
      : searchStrategyLabels.length > 0
      ? searchStrategyLabels
      : fallbackSearchChannels.length > 0
        ? fallbackSearchChannels
        : asArray(payload.plan?.intent_brief?.default_execution_strategy).map((value) => asString(value)).filter(Boolean);
  const requestPreview = (
    (explain.request_preview as Record<string, unknown>) ||
    (payload.request_preview as Record<string, unknown>) ||
    (metadata.request_preview as Record<string, unknown>) ||
    {}
  );
  const organizationExecutionProfile =
    (explain.organization_execution_profile as Record<string, unknown>) ||
    (payload.organization_execution_profile as Record<string, unknown>) ||
    (metadata.organization_execution_profile as Record<string, unknown>) ||
    (payload.plan?.organization_execution_profile as Record<string, unknown>) ||
    (acquisitionStrategy.organization_execution_profile as Record<string, unknown>) ||
    {};
  const assetReusePlan =
    (explain.asset_reuse_plan as Record<string, unknown>) ||
    (payload.asset_reuse_plan as Record<string, unknown>) ||
    (metadata.asset_reuse_plan as Record<string, unknown>) ||
    (payload.plan?.asset_reuse_plan as Record<string, unknown>) ||
    {};
  const effectiveExecutionSemantics =
    (explain.effective_execution_semantics as Record<string, unknown>) ||
    (payload.effective_execution_semantics as Record<string, unknown>) ||
    (metadata.effective_execution_semantics as Record<string, unknown>) ||
    {};
  const dispatchPreview =
    (explain.dispatch_preview as Record<string, unknown>) ||
    (payload.dispatch_preview as Record<string, unknown>) ||
    (metadata.dispatch_preview as Record<string, unknown>) ||
    {};
  const planTargetCompany =
    pickFirstString(payload.plan || {}, ["target_company"]) ||
    pickFirstString(requestPreview, ["target_company"]) ||
    pickFirstString(payload.request || {}, ["target_company"]) ||
    "待确认公司";
  const providerExecutionLanes = mapProviderExecutionLanes({ ...payload, ...explain });
  const planKeywords = extractPlanKeywordLabels(
    { ...payload, ...explain, request_preview: requestPreview },
    requestPreview,
    queryText,
    providerExecutionLanes,
  );
  const acquisitionMode = pickFirstString(organizationExecutionProfile, ["default_acquisition_mode"]);
  const dispatchStrategy = pickFirstString(dispatchPreview, ["strategy"]);
  const currentLaneBehavior = pickFirstString(currentLane, ["planned_behavior"]);
  const formerLaneBehavior = pickFirstString(formerLane, ["planned_behavior"]);
  const reviewGate = mapPlanReviewGate(payload, requestPreview);
  const targetCompanyIdentity = mapTargetCompanyIdentity(requestPreview.target_company_identity);
  const plannerMode = pickFirstString(assetReusePlan, ["planner_mode"]);
  const resolvedAcquisitionStrategy = resolvePlanAcquisitionStrategyLabel({
    effectiveExecutionSemantics,
    assetReusePlan,
    organizationExecutionProfile,
    dispatchPreview,
    queryText,
    payload,
  });
  return {
    planId: String(payload.plan_review_session?.review_id || crypto.randomUUID()),
    rawUserRequest: payload.request?.raw_user_request || queryText,
    targetCompany: planTargetCompany,
    targetPopulation: inferPopulationLabel(queryText, { ...payload, request_preview: requestPreview }),
    projectScope: inferProjectScopeLabel(queryText, { ...payload, request_preview: requestPreview }),
    keywords: canonicalizeDisplayKeywords(planKeywords.length > 0 ? planKeywords : inferKeywordLabels(queryText, payload)),
    acquisitionStrategy: resolvedAcquisitionStrategy,
    searchStrategy: configuredSearchStrategy.map((value) => translateSearchChannelLabel(value)),
    estimatedCostLevel: payload.plan_review_gate?.required_before_execution
      ? "high"
      : (
        Boolean(assetReusePlan.requires_delta_acquisition)
        || dispatchStrategy === "delta_from_snapshot"
          ? "medium"
          : "low"
      ),
    reviewRequired: Boolean(payload.plan_review_gate?.required_before_execution),
    status: payload.plan_review_session?.status === "pending" ? "pending_review" : "draft",
    organizationScaleBand: translateOrganizationScaleBand(pickFirstString(organizationExecutionProfile, ["org_scale_band"])),
    defaultAcquisitionMode: acquisitionMode,
    plannerMode: pickFirstString(assetReusePlan, ["planner_mode"]),
    dispatchStrategy: translateDispatchStrategyLabel(dispatchStrategy),
    currentLaneBehavior: translateLaneBehaviorLabel(currentLaneBehavior),
    formerLaneBehavior: translateLaneBehaviorLabel(formerLaneBehavior),
    baselineSnapshotId: pickFirstString(assetReusePlan, ["baseline_snapshot_id"]),
    requiresDeltaAcquisition: Boolean(assetReusePlan.requires_delta_acquisition),
    executionNotes: buildExecutionNotes(explain),
    targetCompanyIdentity,
    providerExecutionLanes,
    reviewGate,
    reviewDecisionDefaults: mapPlanReviewDecisionDefaults(payload, requestPreview, reviewGate),
  };
}

export async function getWorkflowExplain(queryText: string): Promise<any> {
  return fetchJson<any>("/api/workflows/explain", {
    method: "POST",
    body: JSON.stringify({
      raw_user_request: queryText,
      planning_mode: "model_assisted",
      ...DEFAULT_RECALL_LIMITS,
    }),
  }, PLAN_API_TIMEOUT_MS);
}

export interface FrontendHistoryRecoveryEnvelope {
  historyId: string;
  queryText: string;
  reviewId: string;
  jobId: string;
  phase: WorkflowPhase;
  plan: DemoPlan | null;
  errorMessage?: string;
  targetCompany?: string;
  createdAt?: string;
  updatedAt?: string;
  metadata?: Record<string, unknown>;
  raw: any;
}

function normalizeRecoveredPhase(value: string): WorkflowPhase {
  if (value === "plan" || value === "running" || value === "results") {
    return value;
  }
  return "idle";
}

function mapFrontendHistoryRecoveryPayload(
  recovery: Record<string, unknown>,
  rawPayload: unknown,
): FrontendHistoryRecoveryEnvelope {
  const recoveryRequest = (recovery.request as Record<string, unknown>) || {};
  const recoveryJob = (recovery.job as Record<string, unknown>) || {};
  const metadata =
    recovery.metadata && typeof recovery.metadata === "object" && !Array.isArray(recovery.metadata)
      ? (recovery.metadata as Record<string, unknown>)
      : {};
  const queryText =
    pickFirstString(recovery, ["query_text"]) ||
    pickFirstString(recoveryRequest, ["raw_user_request"]);
  const reviewId =
    String(recovery.review_id || (recovery.plan_review_session as Record<string, unknown> | undefined)?.review_id || "");
  const jobId =
    pickFirstString(recovery, ["job_id"]) ||
    pickFirstString(recoveryJob, ["job_id"]);
  const mappedPlanPayload = {
    request: recoveryRequest,
    request_preview:
      (recovery.request_preview as Record<string, unknown>) ||
      (metadata.request_preview as Record<string, unknown>) ||
      {},
    plan: (recovery.plan as Record<string, unknown>) || {},
    plan_review_gate: (recovery.plan_review_gate as Record<string, unknown>) || {},
    plan_review_session: (recovery.plan_review_session as Record<string, unknown>) || {},
    dispatch_preview: (metadata.dispatch_preview as Record<string, unknown>) || {},
    organization_execution_profile: (metadata.organization_execution_profile as Record<string, unknown>) || {},
    asset_reuse_plan: (metadata.asset_reuse_plan as Record<string, unknown>) || {},
    lane_preview: (metadata.lane_preview as Record<string, unknown>) || {},
    effective_execution_semantics: (metadata.effective_execution_semantics as Record<string, unknown>) || {},
    metadata,
  };
  const hasPlan = Object.keys(mappedPlanPayload.plan).length > 0;
  return {
    historyId: pickFirstString(recovery, ["history_id"]),
    queryText,
    reviewId,
    jobId,
    phase: normalizeRecoveredPhase(pickFirstString(recovery, ["phase"])),
    plan: hasPlan ? mapPlanPayloadToDemoPlan(mappedPlanPayload, queryText) : null,
    errorMessage: pickFirstString(recovery, ["error_message"]),
    targetCompany: pickFirstString(recovery, ["target_company"]),
    createdAt: pickFirstString(recovery, ["created_at"]),
    updatedAt: pickFirstString(recovery, ["updated_at"]),
    metadata,
    raw: rawPayload,
  };
}

export async function submitPlanEnvelope(
  queryText: string,
  historyId = "",
): Promise<{ plan: DemoPlan | null; reviewId: string; historyId: string; status: string; raw: any; explain: any }> {
  const payload = await fetchJson<any>("/api/plan/submit", {
    method: "POST",
    body: JSON.stringify({
      raw_user_request: queryText,
      history_id: historyId || undefined,
      planning_mode: "model_assisted",
      ...DEFAULT_RECALL_LIMITS,
    }),
  }, DEFAULT_API_TIMEOUT_MS);
  const hasPlan =
    payload?.plan &&
    typeof payload.plan === "object" &&
    !Array.isArray(payload.plan) &&
    Object.keys(payload.plan).length > 0;
  return {
    plan: hasPlan ? mapPlanPayloadToDemoPlan(payload, queryText, payload) : null,
    reviewId: String(payload.plan_review_session?.review_id || ""),
    historyId: String(payload.history_id || historyId || ""),
    status: String(payload.status || ""),
    raw: payload,
    explain: payload,
  };
}

export async function approvePlanReview(reviewId: string, decision?: PlanReviewDecision, editableFields: PlanReviewEditableField[] = []): Promise<any> {
  return fetchJson<any>("/api/plan/review", {
    method: "POST",
    body: JSON.stringify({
      review_id: Number(reviewId),
      action: "approved",
      reviewer: "frontend-demo",
      decision: decision ? planReviewDecisionToApiPayload(decision, editableFields) : {},
    }),
  });
}

export async function startWorkflowRun(reviewId: string, historyId = ""): Promise<{ jobId: string; raw: any }> {
  const payload = await fetchJson<any>("/api/workflows", {
    method: "POST",
    body: JSON.stringify({
      plan_review_id: Number(reviewId),
      history_id: historyId || undefined,
    }),
  }, WORKFLOW_START_TIMEOUT_MS);
  return {
    jobId: String(payload.job_id || ""),
    raw: payload,
  };
}

export async function getFrontendHistoryRecovery(historyId: string): Promise<FrontendHistoryRecoveryEnvelope> {
  const payload = await fetchJson<any>(`/api/frontend-history/${encodeURIComponent(historyId)}`);
  const recovery = (payload?.recovery as Record<string, unknown>) || {};
  const envelope = mapFrontendHistoryRecoveryPayload(recovery, payload);
  return {
    ...envelope,
    historyId: envelope.historyId || historyId,
  };
}

export async function listFrontendHistory(limit = 24): Promise<FrontendHistoryRecoveryEnvelope[]> {
  const cacheKey = String(Math.max(1, limit || 24));
  const cached = readFreshCacheValue(frontendHistoryCache, cacheKey, LIST_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = frontendHistoryPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(`/api/frontend-history${buildApiQueryString({ limit: cacheKey })}`)
      .then((payload) => {
        const rows = Array.isArray(payload.history) ? payload.history : [];
        return writeCacheValue(
          frontendHistoryCache,
          cacheKey,
          rows
            .map((item: unknown) =>
              mapFrontendHistoryRecoveryPayload(
                (item && typeof item === "object" ? item : {}) as Record<string, unknown>,
                payload,
              ),
            )
            .filter((item: FrontendHistoryRecoveryEnvelope) => item.historyId),
        );
      })
      .finally(() => {
        frontendHistoryPromiseCache.delete(cacheKey);
      });
    frontendHistoryPromiseCache.set(cacheKey, fetchPromise);
  }
  return fetchPromise;
}

export async function deleteFrontendHistory(historyId: string): Promise<{ status: string; historyId: string }> {
  const payload = await fetchJson<any>(`/api/frontend-history/${encodeURIComponent(historyId)}`, {
    method: "DELETE",
  });
  frontendHistoryCache.clear();
  frontendHistoryPromiseCache.clear();
  return {
    status: String(payload.status || ""),
    historyId: String(payload.history_id || historyId),
  };
}

export async function continueWorkflowStage2(jobId: string): Promise<any> {
  return fetchJson<any>(`/api/workflows/${encodeURIComponent(jobId)}/continue-stage2`, {
    method: "POST",
    body: JSON.stringify({}),
  }, WORKFLOW_START_TIMEOUT_MS);
}

type NaiveTimestampSemantics = "utc" | "china_local";

function parseTimestampMs(value: string, semantics: NaiveTimestampSemantics = "utc"): number | null {
  const normalized = String(value || "").trim().replace("T", " ");
  if (!normalized) {
    return null;
  }
  const naiveMatch = normalized.match(
    /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/,
  );
  if (naiveMatch) {
    const [, year, month, day, hour, minute, second] = naiveMatch;
    const utcMs = Date.UTC(
      Number(year),
      Number(month) - 1,
      Number(day),
      Number(hour),
      Number(minute),
      Number(second),
    );
    return semantics === "china_local" ? utcMs - CHINA_TIME_OFFSET_MS : utcMs;
  }
  const parsed = new Date(value).getTime();
  return Number.isNaN(parsed) ? null : parsed;
}

const CHINA_TIME_OFFSET_MS = 8 * 60 * 60 * 1000;
const NAIVE_TIMESTAMP_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2}))?$/;

function formatTimestampMs(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "";
  }
  return new Date(value).toISOString().replace(".000Z", "Z");
}

function normalizeStageSummaryTimestamp(value: string): string {
  const normalized = String(value || "").trim().replace("T", " ");
  if (!normalized) {
    return "";
  }
  const parsed = parseTimestampMs(value, "china_local");
  return parsed === null ? normalized : formatTimestampMs(parsed);
}

function normalizeBackendProgressTimestamp(value: string): string {
  const normalized = String(value || "").trim().replace("T", " ");
  if (!normalized) {
    return "";
  }
  const parsed = parseTimestampMs(value, "utc");
  if (parsed === null) {
    return normalized;
  }
  return formatTimestampMs(parsed);
}

function mapProgressPayloadToRunStatus(payload: any): RunStatusData {
  const milestones = asArray(payload.progress?.milestones);
  const progressEvents = asArray(payload.progress?.events);
  const workerSummary = (payload.progress?.worker_summary as Record<string, unknown>) || {};
  const laneSummaries = asArray(workerSummary.by_lane).map((item) => (item as Record<string, unknown>) || {});
  const rawOverallStatus = pickFirstString(payload, ["status"]);
  const statusContract = resolveWorkflowStatus(rawOverallStatus);
  const workerStatusCounts = (workerSummary.by_status as Record<string, number>) || {};
  const activeBackgroundWorkerCount = [
    "running",
    "queued",
    "waiting_remote_search",
    "waiting_remote_harvest",
    "blocked",
  ].reduce((sum, status) => sum + Number(workerStatusCounts[status] || 0), 0);
  const hasPostCompletionWork = statusContract.status === "completed" && activeBackgroundWorkerCount > 0;
  const overallStatus: RunStatusData["status"] =
    hasPostCompletionWork ? "running" : statusContract.status;
  const effectiveStatusContract = resolveWorkflowStatus(overallStatus);
  const postCompletionMessage = "结果已可浏览，后台仍在补全 LinkedIn profile 与候选人详情。";
  const completedAtFallback = normalizeBackendProgressTimestamp(pickFirstString(payload, ["updated_at"]));

  const normalizeEventStatus = (
    status: string,
    stage: string,
  ): RunStatusData["timeline"][number]["status"] => {
    const sourceStatus = String(status || "").trim().toLowerCase();
    const normalizedStatus =
      overallStatus === "completed" && sourceStatus === "running" && stage !== "completed"
        ? "completed"
        : sourceStatus;
    if (
      effectiveStatusContract.terminal &&
      (normalizedStatus === "pending" ||
        normalizedStatus === "queued" ||
        normalizedStatus === "running" ||
        normalizedStatus === "blocked")
    ) {
      return statusContract.status;
    }
    if (normalizedStatus === "pending") {
      return "pending";
    }
    if (normalizedStatus === "succeeded") {
      return "completed";
    }
    const eventStatusContract = resolveWorkflowStatus(normalizedStatus);
    if (eventStatusContract.reason !== "unknown_domain_status" && eventStatusContract.reason !== "missing_domain_status") {
      return eventStatusContract.status;
    }
    return effectiveStatusContract.terminal ? effectiveStatusContract.status : "failed";
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

  const stageSummaryRoot = (payload.workflow_stage_summaries as Record<string, unknown>) || {};
  const stageSummaryMap = ((stageSummaryRoot.summaries as Record<string, unknown>) || {}) as Record<string, Record<string, unknown>>;
  const executionPhaseContract = mapExecutionPhaseContract(
    ((payload.execution_phase_contract as Record<string, unknown>) ||
      (payload.progress?.execution_phase_contract as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const excelIntakeProgress = mapExcelIntakeProgress(
    ((payload.excel_intake_progress as Record<string, unknown>) ||
      (payload.progress?.excel_intake_progress as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const stageTitleOverride = (stageId: string): string =>
    executionPhaseContract?.stageTitleOverrides?.[stageId] || "";
  const stageDetailOverride = (stageId: string): string =>
    executionPhaseContract?.stageDetailOverrides?.[stageId] || "";
  const canonicalWorkflowStages = [
    {
      id: "linkedin_stage_1",
      title: stageTitleOverride("linkedin_stage_1") || pickFirstString(stageSummaryMap.linkedin_stage_1 || {}, ["title"]) || "LinkedIn Stage 1",
    },
    {
      id: "stage_1_preview",
      title: stageTitleOverride("stage_1_preview") || pickFirstString(stageSummaryMap.stage_1_preview || {}, ["title"]) || "Stage 1 Preview",
    },
    {
      id: "public_web_stage_2",
      title: stageTitleOverride("public_web_stage_2") || pickFirstString(stageSummaryMap.public_web_stage_2 || {}, ["title"]) || "Public Web Stage 2",
    },
    {
      id: "stage_2_final",
      title: stageTitleOverride("stage_2_final") || pickFirstString(stageSummaryMap.stage_2_final || {}, ["title"]) || "Final Results",
    },
  ];
  const milestoneMap = milestones.reduce<Record<string, Record<string, unknown>>>((accumulator, item) => {
    const record = (item as Record<string, unknown>) || {};
    const stage = pickFirstString(record, ["stage"]);
    if (stage) {
      accumulator[stage] = record;
    }
    return accumulator;
  }, {});
  const acquiringMilestone = milestoneMap.acquiring || {};
  const retrievingMilestone = milestoneMap.retrieving || {};
  const acquiringLatestPayload =
    ((acquiringMilestone.latest_payload as Record<string, unknown>) || {}) as Record<string, unknown>;
  const retrievingLatestPayload =
    ((retrievingMilestone.latest_payload as Record<string, unknown>) || {}) as Record<string, unknown>;
  const stage1PreviewSummary = stageSummaryMap.stage_1_preview || {};
  const stage2FinalSummary = stageSummaryMap.stage_2_final || {};
  const stage1PreviewCandidateSource =
    ((stage1PreviewSummary.candidate_source as Record<string, unknown>) || {}) as Record<string, unknown>;
  const stage2FinalCandidateSource =
    ((stage2FinalSummary.candidate_source as Record<string, unknown>) || {}) as Record<string, unknown>;
  const retrievingCandidateSource =
    ((retrievingLatestPayload.candidate_source as Record<string, unknown>) || {}) as Record<string, unknown>;
  const acquiringSync =
    ((acquiringLatestPayload.sync as Record<string, unknown>) || {}) as Record<string, unknown>;
  const linkedinStage1Progress = mapLinkedinStage1Progress(
    ((payload.linkedin_stage_1_progress as Record<string, unknown>) ||
      (payload.progress?.linkedin_stage_1_progress as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const resultViewLifecycle = mapResultViewLifecycle(
    ((payload.result_view_lifecycle as Record<string, unknown>) ||
      (payload.progress?.result_view_lifecycle as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const boardRuntimeState = mapBoardRuntimeState(
    ((payload.board_runtime_state as Record<string, unknown>) ||
      (payload.progress?.board_runtime_state as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const lifecycleExpectedCount = resultViewLifecycle?.expectedCandidateCount || 0;
  const lifecycleServedCount = resultViewLifecycle?.servedCandidateCount || 0;
  const boardExpectedCount = boardRuntimeState?.expectedCandidateCount || 0;
  const stage1RequiredCount = linkedinStage1Progress?.profileFetchRequiredCount || 0;
  const baselineCount = resultViewLifecycle?.baselineCandidateCount || 0;
  const stage1ExpectedCount = baselineCount > 0 && stage1RequiredCount > 0 ? baselineCount + stage1RequiredCount : 0;
  const candidateCountMetric = boardRuntimeState
    ? Math.max(0, boardExpectedCount)
    : Math.max(
        0,
        lifecycleExpectedCount,
        lifecycleServedCount,
        stage1ExpectedCount,
        asNumber(payload.progress?.counters?.candidate_count) ?? 0,
        asNumber(payload.progress?.counters?.results_count) ?? 0,
        asNumber(stage2FinalCandidateSource.candidate_count) ?? 0,
        asNumber(stage1PreviewCandidateSource.candidate_count) ?? 0,
        asNumber(retrievingCandidateSource.candidate_count) ?? 0,
        asNumber(acquiringSync.candidate_count) ?? 0,
        linkedinStage1Progress?.dedupedCandidateCount || 0,
        stage1RequiredCount,
        linkedinStage1Progress?.profileFetchedCount || 0,
      );
  const manualReviewCountMetric =
    asNumber(payload.manual_review_count) ??
    asNumber(payload.progress?.counters?.manual_review_count) ??
    0;
  const linkedinProfileFetchRequiredCount = linkedinStage1Progress?.profileFetchRequiredCount || 0;
  const linkedinProfileFetchedCount = linkedinStage1Progress?.profileFetchedCount || 0;
  const linkedinStage1ProviderWorkVisible = Boolean(
    linkedinStage1Progress &&
      Math.max(
        linkedinStage1Progress.currentSearchReturnedCount,
        linkedinStage1Progress.formerSearchReturnedCount,
        linkedinStage1Progress.allSearchReturnedCount,
        linkedinStage1Progress.dedupedCandidateCount,
        linkedinProfileFetchRequiredCount,
        linkedinProfileFetchedCount,
      ) > 0,
  );
  const exposeManualReviewMetric = Boolean(
    manualReviewCountMetric > 0 &&
      !boardRuntimeState &&
      !linkedinStage1ProviderWorkVisible,
  );
  const publicManualReviewCountMetric = exposeManualReviewMetric ? manualReviewCountMetric : 0;
  const acquisitionMetrics: RunStatusData["metrics"] = linkedinStage1ProviderWorkVisible && linkedinStage1Progress
    ? [
        { label: "新发现在职候选人", value: String(linkedinStage1Progress.currentSearchReturnedCount) },
        { label: "新发现离职候选人", value: String(linkedinStage1Progress.formerSearchReturnedCount) },
        { label: "经去重得到", value: String(linkedinStage1Progress.dedupedCandidateCount) },
        {
          label: "需补取 LinkedIn Profile",
          value: String(linkedinStage1Progress.profileFetchRequiredCount),
        },
        { label: "已取回 LinkedIn Profile", value: String(linkedinStage1Progress.profileFetchedCount) },
      ]
    : [];
  const linkedinProfileWorkPending =
    linkedinStage1ProviderWorkVisible &&
    linkedinProfileFetchRequiredCount > 0 &&
    linkedinProfileFetchedCount < linkedinProfileFetchRequiredCount;

  const summaryTime = (stageId: string, ...fieldNames: string[]): string =>
    normalizeStageSummaryTimestamp(pickFirstString(stageSummaryMap[stageId] || {}, fieldNames));
  const backendTime = (value: string): string => normalizeBackendProgressTimestamp(value);

  const currentCanonicalStageId = (): string => {
    const contractStageId = executionPhaseContract?.activeStageId || "";
    if (contractStageId && canonicalWorkflowStages.some((stage) => stage.id === contractStageId)) {
      return contractStageId;
    }
    if (rawOverallStatus === "completed") {
      return "stage_2_final";
    }
    if (overallStatus === "failed" || overallStatus === "cancelled") {
      const latestStartedStage = [...canonicalWorkflowStages].reverse().find((stage) => {
        const summary = stageSummaryMap[stage.id] || {};
        return Boolean(
          pickFirstString(summary, ["status"]) ||
            pickFirstString(summary, ["started_at"]) ||
            pickFirstString(summary, ["completed_at"]) ||
            pickFirstString(summary, ["saved_at"]),
        );
      });
      if (latestStartedStage) {
        return latestStartedStage.id;
      }
    }
    if (pickFirstString(payload, ["awaiting_user_action"]) === "continue_stage2") {
      return "stage_1_preview";
    }
    if (linkedinProfileWorkPending) {
      return "linkedin_stage_1";
    }
    const currentStage = pickFirstString(payload, ["stage"]) || pickFirstString(payload.progress, ["current_stage"]);
    if (currentStage === "acquiring") {
      if (pickFirstString(stageSummaryMap.public_web_stage_2 || {}, ["status"]) === "completed") {
        return "stage_2_final";
      }
      if (pickFirstString(stageSummaryMap.stage_1_preview || {}, ["status"]) === "completed") {
        return "public_web_stage_2";
      }
      return "linkedin_stage_1";
    }
    if (currentStage === "retrieving") {
      if (pickFirstString(stageSummaryMap.public_web_stage_2 || {}, ["status"]) === "completed") {
        return "stage_2_final";
      }
      if (pickFirstString(stageSummaryMap.stage_1_preview || {}, ["status"]) === "completed") {
        return "public_web_stage_2";
      }
      return "stage_1_preview";
    }
    return "linkedin_stage_1";
  };

  const currentCanonicalStage = currentCanonicalStageId();
  const currentCanonicalStageIndex = canonicalWorkflowStages.findIndex((stage) => stage.id === currentCanonicalStage);
  const linkedinStageCompletedAt =
    summaryTime("linkedin_stage_1", "completed_at") ||
    backendTime(pickFirstString(acquiringMilestone, ["completed_at"]));
  const stage1PreviewCompletedAt =
    summaryTime("stage_1_preview", "completed_at") ||
    backendTime(pickFirstString(acquiringMilestone, ["completed_at"]));
  const publicWebStageCompletedAt =
    summaryTime("public_web_stage_2", "completed_at") ||
    backendTime(pickFirstString(retrievingMilestone, ["started_at"]));
  const stage2FinalCompletedAt =
    summaryTime("stage_2_final", "completed_at") ||
    backendTime(pickFirstString(retrievingMilestone, ["completed_at"])) ||
    backendTime(pickFirstString(payload, ["updated_at"]));
  const canonicalStageTimingMap: Record<string, { startedAt: string; completedAt: string }> = {
    linkedin_stage_1: {
      startedAt:
        summaryTime("linkedin_stage_1", "started_at") ||
        backendTime(pickFirstString(payload, ["started_at"]) || pickFirstString(acquiringMilestone, ["started_at"])),
      completedAt: linkedinStageCompletedAt,
    },
    stage_1_preview: {
      startedAt:
        summaryTime("stage_1_preview", "started_at") ||
        linkedinStageCompletedAt ||
        backendTime(pickFirstString(acquiringMilestone, ["completed_at"])),
      completedAt: stage1PreviewCompletedAt,
    },
    public_web_stage_2: {
      startedAt:
        summaryTime("public_web_stage_2", "started_at") ||
        stage1PreviewCompletedAt ||
        backendTime(pickFirstString(retrievingMilestone, ["started_at"])),
      completedAt: publicWebStageCompletedAt,
    },
    stage_2_final: {
      startedAt:
        summaryTime("stage_2_final", "started_at") ||
        publicWebStageCompletedAt ||
        stage1PreviewCompletedAt ||
        backendTime(pickFirstString(retrievingMilestone, ["started_at"])),
      completedAt: stage2FinalCompletedAt,
    },
  };
  let previousCompletedAtMs: number | null = null;
  for (const stage of canonicalWorkflowStages) {
    const timing = canonicalStageTimingMap[stage.id];
    if (!timing) {
      continue;
    }
    let startedAtMs = parseTimestampMs(timing.startedAt, "utc");
    let completedAtMs = parseTimestampMs(timing.completedAt, "utc");
    if (previousCompletedAtMs !== null) {
      if (startedAtMs === null || startedAtMs < previousCompletedAtMs) {
        startedAtMs = previousCompletedAtMs;
      }
      if (completedAtMs !== null && completedAtMs < startedAtMs) {
        completedAtMs = startedAtMs;
      }
    } else if (startedAtMs !== null && completedAtMs !== null && completedAtMs < startedAtMs) {
      completedAtMs = startedAtMs;
    }
    timing.startedAt = formatTimestampMs(startedAtMs) || timing.startedAt;
    timing.completedAt = formatTimestampMs(completedAtMs) || timing.completedAt;
    if (completedAtMs !== null) {
      previousCompletedAtMs = completedAtMs;
    }
  }

  const stageSummaryDetail = (stageId: string, summary: Record<string, unknown>): string => {
    const overrideDetail = stageDetailOverride(stageId);
    if (overrideDetail) {
      return overrideDetail;
    }
    if (stageId === "linkedin_stage_1") {
      if (executionPhaseContract?.activeStageId === "linkedin_stage_1" && executionPhaseContract.activePhaseDetail) {
        return executionPhaseContract.activePhaseDetail;
      }
      if (linkedinProfileWorkPending) {
        return `正在取回 LinkedIn Profile ${linkedinProfileFetchedCount}/${linkedinProfileFetchRequiredCount}。`;
      }
      const explicitText = pickFirstString(summary, ["text"]);
      if (explicitText) {
        return explicitText;
      }
      if (pickFirstString(summary, ["status"]) === "completed") {
        return "LinkedIn Stage 1 completed.";
      }
      return "正在获取 LinkedIn roster 与 profile 数据。";
    }
    if (stageId === "stage_1_preview") {
      const returnedMatches = Number(summary.returned_matches || 0);
      const manualReviewCount = publicManualReviewCountMetric;
      if (returnedMatches > 0) {
        return `Stage 1 preview 已生成，当前返回 ${returnedMatches} 位候选人，待审核 ${manualReviewCount} 条。`;
      }
      return pickFirstString(summary, ["text"]) || "正在根据 LinkedIn Stage 1 数据生成 preview。";
    }
    if (stageId === "public_web_stage_2") {
      return pickFirstString(summary, ["text"]) || "正在补充公开网页、论文与外部证据。";
    }
    if (stageId === "stage_2_final") {
      if (hasPostCompletionWork) {
        return postCompletionMessage;
      }
      const explicitText = pickFirstString(summary, ["text"]);
      if (explicitText && (pickFirstString(summary, ["status"]) === "completed" || overallStatus === "completed")) {
        return explicitText;
      }
      if (pickFirstString(summary, ["status"]) === "completed" || overallStatus === "completed") {
        return "Workflow completed.";
      }
      const inFlightDetail =
        pickFirstString(payload, ["current_message"]) ||
        pickFirstString(payload.progress?.latest_event || {}, ["detail"]);
      if (inFlightDetail) {
        return inFlightDetail;
      }
      if (explicitText) {
        return explicitText;
      }
      return "正在整理最终结果与候选人看板。";
    }
    return pickFirstString(summary, ["text"]) || "Workflow in progress.";
  };

  const summarizedTimeline =
    Object.keys(stageSummaryMap).length > 0 || pickFirstString(payload, ["stage"]) || pickFirstString(payload.progress, ["current_stage"])
      ? canonicalWorkflowStages.map((stage, index) => {
          const summary = stageSummaryMap[stage.id] || {};
          const explicitStatus = pickFirstString(summary, ["status"]);
          let status = "";
          if (overallStatus === "running" && linkedinProfileWorkPending && stage.id === "linkedin_stage_1") {
            status = "running";
          } else if (overallStatus === "running" && linkedinProfileWorkPending && stage.id !== "linkedin_stage_1") {
            status = "pending";
          } else if (hasPostCompletionWork && stage.id === "stage_2_final") {
            status = "running";
          } else if (explicitStatus) {
            status = normalizeEventStatus(explicitStatus, stage.id);
          } else if (overallStatus === "completed" && stage.id === "stage_2_final") {
            status = "completed";
          } else if (index < currentCanonicalStageIndex) {
            status = "completed";
          } else if (index === currentCanonicalStageIndex) {
            status =
              overallStatus === "queued"
                ? "queued"
                : overallStatus === "failed" || overallStatus === "cancelled"
                  ? overallStatus
                  : "running";
          } else {
            status = "pending";
          }
          return {
            id: stage.id,
            stage: stage.id,
            title: stage.title,
            detail: stageSummaryDetail(stage.id, summary),
            status,
            startedAt:
              status === "pending" || status === "queued"
                ? ""
                : canonicalStageTimingMap[stage.id]?.startedAt || "",
            completedAt:
              status === "pending" || status === "queued" || status === "running"
                ? ""
                : normalizeCompletedAt(status, canonicalStageTimingMap[stage.id]?.completedAt || ""),
            sourceTags: [],
          };
        })
      : [];

  return {
    jobId: String(payload.job_id || ""),
    status: overallStatus,
    currentStage:
      executionPhaseContract?.activePhaseLabel ||
      canonicalWorkflowStages.find((stage) => stage.id === currentCanonicalStage)?.title ||
      payload.stage ||
      payload.progress?.current_stage ||
      "Workflow",
    startedAt: backendTime(payload.started_at || payload.updated_at || "") || "unknown",
    currentMessage:
      executionPhaseContract?.activePhaseDetail ||
      (hasPostCompletionWork ? postCompletionMessage : pickFirstString(payload, ["current_message"])),
    awaitingUserAction: pickFirstString(payload, ["awaiting_user_action"]),
    metrics: [
      ...acquisitionMetrics,
      { label: "总候选人数量", value: String(candidateCountMetric) },
      ...(exposeManualReviewMetric
        ? [{ label: "需人工审核候选人", value: String(publicManualReviewCountMetric) }]
        : []),
    ],
    linkedinStage1Progress,
    resultViewLifecycle,
    boardRuntimeState,
    executionPhaseContract,
    excelIntakeProgress,
    timeline:
      summarizedTimeline.length > 0
        ? summarizedTimeline
        : progressEvents.length > 0
        ? progressEvents.map((item, index) => {
            const event = item as Record<string, unknown>;
            const stage = pickFirstString(event, ["stage"]) || `Stage ${index + 1}`;
            const status = normalizeEventStatus(pickFirstString(event, ["status"]), stage);
            return {
              id: pickFirstString(event, ["id"]) || String(index + 1),
              stage,
              title: pickFirstString(event, ["title"]) || stage,
              detail: pickFirstString(event, ["detail"]) || pickFirstString(payload, ["current_message"]) || "Workflow in progress.",
              status,
              startedAt: backendTime(pickFirstString(event, ["started_at"]) || ""),
              completedAt: normalizeCompletedAt(status, backendTime(pickFirstString(event, ["completed_at"]) || "")),
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
            const status = normalizeEventStatus(pickFirstString(milestone, ["status"]), stage);
            return {
              id: String(index + 1),
              stage,
              title: stage,
              detail: pickFirstString(milestone, ["latest_detail"]) || pickFirstString(payload, ["current_message"]) || "Workflow in progress.",
              status,
              startedAt: backendTime(pickFirstString(milestone, ["started_at"]) || ""),
              completedAt: normalizeCompletedAt(status, backendTime(pickFirstString(milestone, ["completed_at"]) || "")),
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

export function __testMapProgressPayloadToRunStatus(payload: any): RunStatusData {
  return mapProgressPayloadToRunStatus(payload);
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
        status: normalizeWorkflowStatus(fallbackPayload.status),
        currentStage: fallbackPayload.stage || "Workflow",
        startedAt: fallbackPayload.created_at || fallbackPayload.updated_at || "unknown",
        currentMessage: fallbackPayload.summary?.message || "",
        awaitingUserAction: fallbackPayload.summary?.awaiting_user_action || "",
      metrics: [
        { label: "总候选人数量", value: String(fallbackPayload.summary?.candidate_count || 0) },
        { label: "需人工审核候选人", value: String(fallbackPayload.summary?.manual_review_queue_count || 0) },
      ],
        timeline: [],
        workers: [],
      };
    }
  }

function mapJobResultsToDashboard(payload: any): DashboardData {
  const rankedResultRecords = asArray(payload.results)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .filter((item) => Object.keys(item).length > 0);
  const assetPopulationPayload = (payload.asset_population as Record<string, unknown>) || {};
  const assetPopulationRecords = asArray(assetPopulationPayload.candidates)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .filter((item) => Object.keys(item).length > 0);
  const rawEffectiveExecutionSemantics = (payload.effective_execution_semantics as Record<string, unknown>) || {};
  const rankedResultCount =
    typeof payload.ranked_result_count === "number"
      ? Number(payload.ranked_result_count || 0)
      : rankedResultRecords.length;
  const assetPopulationCount =
    Number(assetPopulationPayload.candidate_count || 0) || assetPopulationRecords.length;
  const preferredResultsMode = pickFirstString(rawEffectiveExecutionSemantics, ["default_results_mode"]);
  const requestTargetScope =
    pickFirstString(payload.request_preview || {}, ["target_scope"]) ||
    pickFirstString(payload.job?.request || {}, ["target_scope"]);
  const assetPopulationAvailable = Boolean(assetPopulationPayload.available) && (
    assetPopulationCount > 0 ||
    preferredResultsMode === "asset_population" ||
    requestTargetScope === "full_company_asset"
  );
  const useAssetPopulation =
    assetPopulationAvailable &&
    (
      preferredResultsMode !== "ranked_results" ||
      Boolean(assetPopulationPayload.default_selected) ||
      rankedResultCount === 0 ||
      requestTargetScope === "full_company_asset"
    );
  const activeRecords = useAssetPopulation ? assetPopulationRecords : rankedResultRecords;
  const candidates = activeRecords.map((record) => deriveCandidate(record));
  const groups = Array.from(new Set(["All", ...candidates.map((candidate) => candidate.team || "Unknown")]));
  const targetCompany = firstNonEmptyString([
    pickFirstString(payload.request_preview || {}, ["target_company"]),
    pickFirstString(payload.job?.request || {}, ["target_company"]),
    candidates[0]?.currentCompany || "",
  ]);
  const intentKeywords = extractDashboardIntentKeywords(payload, candidates, targetCompany);
  const rawManualReviewCount =
    typeof payload.manual_review_count === "number"
      ? Number(payload.manual_review_count || 0)
      : asArray(payload.manual_review_items).length;
  const profileFetchProgress = mapProfileFetchProgress(
    (assetPopulationPayload.profile_fetch_progress as Record<string, unknown>) ||
      (payload.profile_fetch_progress as Record<string, unknown>) ||
      {},
  );
  const linkedinStage1Progress = mapLinkedinStage1Progress(
    ((payload.linkedin_stage_1_progress as Record<string, unknown>) ||
      (assetPopulationPayload.linkedin_stage_1_progress as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const resultViewLifecycle = mapResultViewLifecycle(
    ((payload.result_view_lifecycle as Record<string, unknown>) ||
      (assetPopulationPayload.result_view_lifecycle as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const projectionId = firstNonEmptyString([
    pickFirstString(payload, ["projection_id", "serving_projection_id"]),
    pickFirstString(assetPopulationPayload, ["projection_id", "serving_projection_id"]),
    resultViewLifecycle?.servingProjectionId || "",
  ]);
  const boardRuntimeState = mapBoardRuntimeState(
    ((payload.board_runtime_state as Record<string, unknown>) ||
      (assetPopulationPayload.board_runtime_state as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const linkedinProfileRequiredCount = linkedinStage1Progress?.profileFetchRequiredCount || 0;
  const linkedinProfileFetchedCount = linkedinStage1Progress?.profileFetchedCount || 0;
  const linkedinStage1ProviderWorkVisible = Boolean(
    linkedinStage1Progress &&
      Math.max(
        linkedinStage1Progress.currentSearchReturnedCount,
        linkedinStage1Progress.formerSearchReturnedCount,
        linkedinStage1Progress.allSearchReturnedCount,
        linkedinStage1Progress.dedupedCandidateCount,
        linkedinProfileRequiredCount,
        linkedinProfileFetchedCount,
      ) > 0,
  );
  const manualReviewCount =
    rawManualReviewCount > 0 && !boardRuntimeState && !linkedinStage1ProviderWorkVisible
      ? rawManualReviewCount
      : 0;
  const executionPhaseContract = mapExecutionPhaseContract(
    ((payload.execution_phase_contract as Record<string, unknown>) ||
      (assetPopulationPayload.execution_phase_contract as Record<string, unknown>) ||
      {}) as Record<string, unknown>,
  );
  const effectiveExecutionSemantics = mapEffectiveExecutionSemantics(
    (payload.effective_execution_semantics as Record<string, unknown>) || {},
  );
  const rawCandidateFacetSummary = mapCandidateFacetSummary(assetPopulationPayload.facet_summary);
  const rawCandidateFacetSummaryScope = mapCandidateFacetSummaryScope(
    assetPopulationPayload,
    asString(payload.facet_summary_scope),
  );
  const canonicalBoardExpectedCount = boardRuntimeState?.expectedCandidateCount || 0;
  const canonicalAssetPopulationCount = boardRuntimeState ? canonicalBoardExpectedCount : assetPopulationCount;
  const boardFacetContractActive = Boolean(boardRuntimeState);
  const candidateFacetSummary =
    !boardFacetContractActive ||
    candidateFacetSummaryMatchesCanonicalBoard(rawCandidateFacetSummary, rawCandidateFacetSummaryScope, boardRuntimeState)
      ? rawCandidateFacetSummary
      : undefined;
  const candidateFacetSummaryScope = candidateFacetSummary ? rawCandidateFacetSummaryScope : "";
  const canonicalTotalCandidates = useAssetPopulation
    ? canonicalAssetPopulationCount
    : rankedResultCount;
  return {
    projectionId,
    title: payload.job?.request?.raw_user_request || payload.job?.request?.query || "Sourcing results",
    snapshotId:
      pickFirstString(assetPopulationPayload, ["snapshot_id"]) ||
      payload.job?.summary?.snapshot_id ||
      payload.job?.job_id ||
      "live",
    queryLabel: payload.job?.request?.raw_user_request || payload.job?.request?.query || "Query",
    targetCompany,
    intentKeywords,
    resultMode: useAssetPopulation ? "asset_population" : "ranked_results",
    resultModeLabel: useAssetPopulation ? "公司级资产视图" : "检索排序结果",
    rankedCandidateCount: rankedResultCount,
    assetPopulationCount: canonicalAssetPopulationCount,
    totalCandidates: canonicalTotalCandidates,
    totalEvidence: activeRecords.reduce((sum, record) => sum + asArray(record.evidence).length, 0),
    manualReviewCount,
    layers: candidateFacetSummary?.layers?.length
      ? candidateFacetSummary.layers
      : buildLayeredSegmentationOptions(candidates),
    groups,
    candidates,
    profileFetchProgress,
    linkedinStage1Progress,
    resultViewLifecycle,
    boardRuntimeState,
    executionPhaseContract,
    effectiveExecutionSemantics,
    candidateFacetSummary,
    candidateFacetSummaryScope,
  };
}

function mapProfileFetchProgress(source: Record<string, unknown>): ProfileFetchProgress | undefined {
  if (!source || Object.keys(source).length === 0) {
    return undefined;
  }
  const statusCountsSource = (source.status_counts as Record<string, unknown>) || {};
  const statusCounts = Object.fromEntries(
    Object.entries(statusCountsSource).map(([key, value]) => [key, Number(value || 0) || 0]),
  );
  return {
    totalUrlCount: Number(source.total_url_count || 0) || 0,
    fetchedUrlCount: Number(source.fetched_url_count || 0) || 0,
    queuedUrlCount: Number(source.queued_url_count || 0) || 0,
    failedRetryableUrlCount: Number(source.failed_retryable_url_count || 0) || 0,
    unrecoverableUrlCount: Number(source.unrecoverable_url_count || 0) || 0,
    missingRegistryUrlCount: Number(source.missing_registry_url_count || 0) || 0,
    deferredUrlCount: Number(source.deferred_url_count || 0) || 0,
    pendingUrlCount: Number(source.pending_url_count || 0) || 0,
    statusCounts,
  };
}

function mapLinkedinStage1Progress(source: Record<string, unknown>): LinkedinStage1Progress | undefined {
  if (!source || Object.keys(source).length === 0) {
    return undefined;
  }
  const statusCountsSource = (source.status_counts as Record<string, unknown>) || {};
  const statusCounts = Object.fromEntries(
    Object.entries(statusCountsSource).map(([key, value]) => [key, Number(value || 0) || 0]),
  );
  return {
    currentSearchReturnedCount: Number(source.current_search_returned_count || 0) || 0,
    formerSearchReturnedCount: Number(source.former_search_returned_count || 0) || 0,
    allSearchReturnedCount: Number(source.all_search_returned_count || 0) || 0,
    dedupedCandidateCount: Number(source.deduped_candidate_count || 0) || 0,
    dedupedProfileUrlCount: Number(source.deduped_profile_url_count || 0) || 0,
    profileFetchRequiredCount: Number(source.profile_fetch_required_count || 0) || 0,
    profileFetchedCount: Number(source.profile_fetched_count || source.fetched_profile_count || 0) || 0,
    profileQueuedCount: Number(source.profile_queued_count || 0) || 0,
    profileFailedRetryableCount: Number(source.profile_failed_retryable_count || 0) || 0,
    profileUnrecoverableCount: Number(source.profile_unrecoverable_count || 0) || 0,
    profilePendingCount: Number(source.profile_pending_count || 0) || 0,
    statusCounts,
  };
}

function mapResultViewLifecycle(source: Record<string, unknown>): ResultViewLifecycle | undefined {
  if (!source || Object.keys(source).length === 0) {
    return undefined;
  }
  return {
    state: pickFirstString(source, ["state"]),
    baselineSnapshotId: pickFirstString(source, ["baseline_snapshot_id"]),
    currentSnapshotId: pickFirstString(source, ["current_snapshot_id"]),
    servedSnapshotId: pickFirstString(source, ["served_snapshot_id"]),
    baselineCandidateCount: Number(source.baseline_candidate_count || 0) || 0,
    servedCandidateCount: Number(source.served_candidate_count || 0) || 0,
    expectedCandidateCount: Number(source.expected_candidate_count || 0) || 0,
    deltaProfileProgressApplicable: source.delta_profile_progress_applicable !== false,
    deltaProfileProgressReason: pickFirstString(source, ["delta_profile_progress_reason"]),
    deltaProfileRequiredCount: Number(source.delta_profile_required_count || 0) || 0,
    deltaProfileFetchedCount: Number(source.delta_profile_fetched_count || 0) || 0,
    deltaProfileMaterializedCount: asNumber(source.delta_profile_materialized_count) ?? undefined,
    deltaProfileBoardVisibleCount: asNumber(source.delta_profile_board_visible_count) ?? undefined,
    deltaProfilePendingCount: Number(source.delta_profile_pending_count || 0) || 0,
    deltaProfileQueuedCount: Number(source.delta_profile_queued_count || 0) || 0,
    deltaProfileRetryableCount: Number(source.delta_profile_retryable_count || 0) || 0,
    servingProjectionId: pickFirstString(source, ["serving_projection_id"]),
    servingProjectionPhase: pickFirstString(source, ["serving_projection_phase"]),
    backgroundSnapshotMaterializationStatus: pickFirstString(source, ["background_snapshot_materialization_status"]),
    outreachLayeringStatus: pickFirstString(source, ["outreach_layering_status"]),
  };
}

function mapBoardRuntimeState(source: Record<string, unknown>): BoardRuntimeState | undefined {
  if (!source || Object.keys(source).length === 0) {
    return undefined;
  }
  const filterContractSource = (source.filter_contract as Record<string, unknown>) || {};
  const resultMode = pickFirstString(source, ["result_mode"]) === "asset_population" ? "asset_population" : "ranked_results";
  const syncNoteLines = asArray(source.sync_note_lines)
    .map((item) => (item && typeof item === "object" ? (item as Record<string, unknown>) : {}))
    .map((item) => ({
      id: pickFirstString(item, ["id"]),
      text: pickFirstString(item, ["text"]),
    }))
    .filter((item) => item.id && item.text);
  return {
    schemaVersion: Number(source.schema_version || 1) || 1,
    jobId: pickFirstString(source, ["job_id"]),
    resultMode,
    phase: pickFirstString(source, ["phase"]),
    publicationStatus: pickFirstString(source, ["publication_status"]),
    expectedCandidateCount: Number(source.expected_candidate_count || 0) || 0,
    servedCandidateCount: Number(source.served_candidate_count || 0) || 0,
    publishedCandidateCount: Number(source.published_candidate_count || 0) || 0,
    displayReadyCandidateCount: Number(source.display_ready_candidate_count || 0) || 0,
    previewCandidateCount: Number(source.preview_candidate_count || 0) || 0,
    profileDetailCandidateCount: Number(source.profile_detail_candidate_count || 0) || 0,
    explicitProfileCaptureCandidateCount: nonNegativeInteger(source.explicit_profile_capture_candidate_count),
    needsProfileCompletionCandidateCount: nonNegativeInteger(source.needs_profile_completion_candidate_count),
    lowProfileRichnessCandidateCount: nonNegativeInteger(source.low_profile_richness_candidate_count),
    cardMaterializationQualityFieldsAvailable: Boolean(source.card_materialization_quality_fields_available),
    rowHydrationTargetCount: Number(source.row_hydration_target_count || 0) || 0,
    candidateDiscoveryCount: Number(source.candidate_discovery_count || 0) || 0,
    profileFetchRequiredCount: Number(source.profile_fetch_required_count || 0) || 0,
    profileFetchedCount: Number(source.profile_fetched_count || 0) || 0,
    baselineCandidateCount: Number(source.baseline_candidate_count || 0) || 0,
    deltaProfileRequiredCount: Number(source.delta_profile_required_count || 0) || 0,
    deltaProfileFetchedCount: Number(source.delta_profile_fetched_count || 0) || 0,
    deltaProfileMaterializedCount: Number(source.delta_profile_materialized_count || 0) || 0,
    deltaProfileBoardVisibleCount: Number(source.delta_profile_board_visible_count || 0) || 0,
    deltaProfileDenominatorPromoted: Boolean(source.delta_profile_denominator_promoted),
    rowPublicationSequence: Number(source.row_publication_sequence || 0) || 0,
    rowPublicationTier: pickFirstString(source, ["row_publication_tier"]),
    rowPublicationRevision: pickFirstString(source, ["row_publication_revision"]),
    rowPublicationWatermark: pickFirstString(source, ["row_publication_watermark"]),
    rowPublicationUpdatedAt: pickFirstString(source, ["row_publication_updated_at"]),
    facetSummaryStatus: pickFirstString(source, ["facet_summary_status"]),
    facetSummaryScope: pickFirstString(source, ["facet_summary_scope"]),
    facetSummaryCandidateCount: Number(source.facet_summary_candidate_count || 0) || 0,
    layeringStatus: pickFirstString(source, ["layering_status"]),
    filterContract: Object.keys(filterContractSource).length
      ? {
          source: pickFirstString(filterContractSource, ["source"]),
          facetCountScope: pickFirstString(filterContractSource, ["facet_count_scope"]),
          rowFilterScope: pickFirstString(filterContractSource, ["row_filter_scope"]),
          backendFilteredPagingSupported: Boolean(filterContractSource.backend_filtered_paging_supported),
        }
      : undefined,
    syncStatusText: pickFirstString(source, ["sync_status_text"]),
    syncNoteLines,
    candidateDiscoveryStatusText: pickFirstString(source, ["candidate_discovery_status_text"]),
    profileFetchStatusText: pickFirstString(source, ["profile_fetch_status_text"]),
    cardMaterializationStatusText: pickFirstString(source, ["card_materialization_status_text"]),
    noteText: pickFirstString(source, ["note_text"]),
  };
}

function mapCandidatePageFilterContract(source: unknown): DashboardCandidatePage["filterContract"] | undefined {
  const record = source && typeof source === "object" ? (source as Record<string, unknown>) : {};
  if (Object.keys(record).length === 0) {
    return undefined;
  }
  return {
    source: pickFirstString(record, ["source"]),
    facetCountScope: pickFirstString(record, ["facet_count_scope"]),
    rowFilterScope: pickFirstString(record, ["row_filter_scope"]),
    backendFilteredPagingSupported: Boolean(record.backend_filtered_paging_supported),
    filterSignature: pickFirstString(record, ["filter_signature"]),
    filterActive: Boolean(record.filter_active),
  };
}

function mapStringRecord(source: unknown): Record<string, string> {
  const record = source && typeof source === "object" ? (source as Record<string, unknown>) : {};
  return Object.fromEntries(
    Object.entries(record)
      .map(([key, value]) => [key, asString(value).trim()])
      .filter(([, value]) => value.length > 0),
  );
}

function mapExecutionPhaseContract(source: Record<string, unknown>): ExecutionPhaseContract | undefined {
  if (!source || Object.keys(source).length === 0) {
    return undefined;
  }
  return {
    activePhaseId: pickFirstString(source, ["active_phase_id"]),
    activeStageId: pickFirstString(source, ["active_stage_id"]),
    activePhaseLabel: pickFirstString(source, ["active_phase_label"]),
    activePhaseDetail: pickFirstString(source, ["active_phase_detail"]),
    publicWebStageApplicable: Boolean(source.public_web_stage_applicable),
    localAssetMaterializationApplicable: Boolean(source.local_asset_materialization_applicable),
    profileWorkPending: Boolean(source.profile_work_pending),
    stageTitleOverrides: mapStringRecord(source.stage_title_overrides),
    stageDetailOverrides: mapStringRecord(source.stage_detail_overrides),
  };
}

function mapExcelIntakeProgress(source: Record<string, unknown>): ExcelIntakeProgress | undefined {
  if (!source || Object.keys(source).length === 0) {
    return undefined;
  }
  const statusCountsSource = (source.status_counts as Record<string, unknown>) || {};
  const statusCounts = Object.fromEntries(
    Object.entries(statusCountsSource).map(([key, value]) => [key, Number(value || 0) || 0]),
  );
  return {
    workflowKind: pickFirstString(source, ["workflow_kind"]),
    targetCompany: pickFirstString(source, ["target_company"]),
    inputFilename: pickFirstString(source, ["input_filename"]),
    totalRowCount: Number(source.total_row_count || 0) || 0,
    matchedRowCount: Number(source.matched_row_count || 0) || 0,
    targetCandidateCount: Number(source.target_candidate_count || 0) || 0,
    manualReviewRowCount: Number(source.manual_review_row_count || 0) || 0,
    unresolvedRowCount: Number(source.unresolved_row_count || 0) || 0,
    invalidRowCount: Number(source.invalid_row_count || 0) || 0,
    reviewRowCount: Number(source.review_row_count || 0) || 0,
    statusCounts,
    rowManifestAvailable: Boolean(source.row_manifest_available),
    rowManifestTruncated: Boolean(source.row_manifest_truncated),
  };
}

function mapEffectiveExecutionSemantics(source: Record<string, unknown>): EffectiveExecutionSemantics | undefined {
  if (!source || Object.keys(source).length === 0) {
    return undefined;
  }
  return {
    effectiveAcquisitionMode: pickFirstString(source, ["effective_acquisition_mode"]),
    defaultResultsMode: pickFirstString(source, ["default_results_mode"]),
    executionStrategyLabel: pickFirstString(source, ["execution_strategy_label", "strategy_label"]),
    fullLocalAssetReuse: Boolean(source.full_local_asset_reuse),
    requiresDeltaAcquisition: Boolean(source.requires_delta_acquisition),
    assetPopulationSupported: Boolean(source.asset_population_supported),
  };
}

function mergeUniqueCandidates(existing: Candidate[], incoming: Candidate[]): Candidate[] {
  if (incoming.length === 0) {
    return existing;
  }
  const merged = new Map<string, Candidate>();
  existing.forEach((candidate) => {
    merged.set(candidate.id, candidate);
  });
  incoming.forEach((candidate) => {
    merged.set(candidate.id, candidate);
  });
  return Array.from(merged.values());
}

function mergeDashboardCandidatesInPageOrder(
  existing: Candidate[],
  incoming: Candidate[],
  options?: {
    offset?: number;
  },
): Candidate[] {
  if (incoming.length === 0) {
    return existing;
  }
  const normalizedOffset = Math.max(Number(options?.offset || 0), 0);
  const incomingIds = new Set(incoming.map((candidate) => candidate.id));
  if (normalizedOffset === 0) {
    return [
      ...incoming,
      ...existing.filter((candidate) => !incomingIds.has(candidate.id)),
    ];
  }
  if (normalizedOffset >= existing.length) {
    return mergeUniqueCandidates(existing, incoming);
  }
  const prefix = existing
    .slice(0, normalizedOffset)
    .filter((candidate) => !incomingIds.has(candidate.id));
  const suffix = existing
    .slice(normalizedOffset)
    .filter((candidate) => !incomingIds.has(candidate.id));
  return [...prefix, ...incoming, ...suffix];
}

export function storeDashboardCache(jobId: string, dashboard: DashboardData): DashboardData {
  return writeCacheValue(dashboardCache, jobId, dashboard);
}

export function mergeDashboardRuntimeProgress(dashboard: DashboardData, runStatus: RunStatusData): DashboardData {
  const linkedinStage1Progress = pickFresherLinkedinProgress(
    runStatus.linkedinStage1Progress,
    dashboard.linkedinStage1Progress,
  );
  const resultViewLifecycle = pickFresherResultViewLifecycle(
    runStatus.resultViewLifecycle,
    dashboard.resultViewLifecycle,
  );
  const boardRuntimeState = pickFresherBoardRuntimeState(
    runStatus.boardRuntimeState,
    dashboard.boardRuntimeState,
  );
  const nextTotalCandidates = boardRuntimeState
    ? Math.max(0, boardRuntimeState.expectedCandidateCount || 0)
    : Math.max(
        dashboard.totalCandidates || 0,
        resultViewLifecycle?.expectedCandidateCount || 0,
        resultViewLifecycle?.servedCandidateCount || 0,
        dashboard.candidates.length,
      );
  const keepExistingFacetSummary =
    !boardRuntimeState ||
    candidateFacetSummaryMatchesCanonicalBoard(
      dashboard.candidateFacetSummary,
      dashboard.candidateFacetSummaryScope,
      boardRuntimeState,
    );
  if (
    linkedinStage1Progress === dashboard.linkedinStage1Progress &&
    resultViewLifecycle === dashboard.resultViewLifecycle &&
    boardRuntimeState === dashboard.boardRuntimeState &&
    nextTotalCandidates === dashboard.totalCandidates &&
    runStatus.executionPhaseContract === dashboard.executionPhaseContract &&
    keepExistingFacetSummary
  ) {
    return dashboard;
  }
  return {
    ...dashboard,
    totalCandidates: nextTotalCandidates,
    linkedinStage1Progress,
    resultViewLifecycle,
    boardRuntimeState,
    executionPhaseContract: runStatus.executionPhaseContract || dashboard.executionPhaseContract,
    candidateFacetSummary: keepExistingFacetSummary ? dashboard.candidateFacetSummary : undefined,
    candidateFacetSummaryScope: keepExistingFacetSummary ? dashboard.candidateFacetSummaryScope : "",
  };
}

export function mergeDashboardBoardPatchRuntime(
  dashboard: DashboardData,
  patchLog: Pick<BoardVisiblePatchLog, "boardRuntimeState" | "resultViewLifecycle">,
): DashboardData {
  if (!patchLog.boardRuntimeState && !patchLog.resultViewLifecycle) {
    return dashboard;
  }
  return mergeDashboardRuntimeProgress(dashboard, {
    boardRuntimeState: patchLog.boardRuntimeState,
    resultViewLifecycle: patchLog.resultViewLifecycle,
  } as RunStatusData);
}

function linkedinProgressFreshnessScore(progress: LinkedinStage1Progress | undefined): number {
  if (!progress) {
    return 0;
  }
  return (
    Math.max(0, progress.profileFetchRequiredCount || 0) * 1_000_000 +
    Math.max(0, progress.profileFetchedCount || 0) * 10_000 +
    Math.max(0, progress.dedupedCandidateCount || 0) * 100 +
    Math.max(0, progress.currentSearchReturnedCount || 0) +
    Math.max(0, progress.formerSearchReturnedCount || 0) +
    Math.max(0, progress.allSearchReturnedCount || 0)
  );
}

function resultViewLifecycleFreshnessScore(lifecycle: ResultViewLifecycle | undefined): number {
  if (!lifecycle) {
    return 0;
  }
  const stateRank: Record<string, number> = {
    unavailable: 0,
    baseline_serving: 1,
    delta_applying: 2,
    current_snapshot_materializing: 3,
    post_result_layering: 4,
    current_snapshot_serving: 5,
  };
  return (
    (stateRank[lifecycle.state] || 0) * 1_000_000_000 +
    Math.max(0, lifecycle.expectedCandidateCount || 0) * 1_000_000 +
    Math.max(0, lifecycle.servedCandidateCount || 0) * 10_000 +
    Math.max(0, lifecycle.deltaProfileFetchedCount || 0) * 100 +
    Math.max(0, lifecycleEffectiveDeltaMaterializedCount(lifecycle)) * 10 +
    Math.max(0, lifecycle.deltaProfileMaterializedCount || 0)
  );
}

function pickFresherLinkedinProgress(
  incoming: LinkedinStage1Progress | undefined,
  current: LinkedinStage1Progress | undefined,
): LinkedinStage1Progress | undefined {
  if (!incoming) {
    return current;
  }
  if (!current) {
    return incoming;
  }
  return linkedinProgressFreshnessScore(incoming) >= linkedinProgressFreshnessScore(current) ? incoming : current;
}

function pickFresherResultViewLifecycle(
  incoming: ResultViewLifecycle | undefined,
  current: ResultViewLifecycle | undefined,
): ResultViewLifecycle | undefined {
  if (!incoming) {
    return current;
  }
  if (!current) {
    return incoming;
  }
  return resultViewLifecycleFreshnessScore(incoming) >= resultViewLifecycleFreshnessScore(current) ? incoming : current;
}

function boardRuntimeStateFreshnessScore(state: BoardRuntimeState | undefined): number {
  if (!state) {
    return 0;
  }
  const phaseRank: Record<string, number> = {
    unavailable: 0,
    awaiting_publication: 1,
    partial_serving: 2,
    post_result_layering: 3,
    current_snapshot_serving: 4,
    canonical_projection_serving: 5,
  };
  const expectedCount = Math.max(0, state.expectedCandidateCount || 0);
  const completeFacetContract = Boolean(
    state.facetSummaryStatus === "complete" &&
      ["global_full_population", "exact_projection"].includes(state.facetSummaryScope) &&
      (expectedCount <= 0 || Math.max(0, state.facetSummaryCandidateCount || 0) >= expectedCount),
  );
  const completeLayeringContract = state.layeringStatus === "completed";
  return (
    (phaseRank[state.phase] || 0) * 1_000_000_000 +
    (completeFacetContract ? 100_000_000 : 0) +
    (completeLayeringContract ? 10_000_000 : 0) +
    expectedCount * 1_000_000 +
    Math.max(0, state.rowHydrationTargetCount || 0) * 10_000 +
    Math.max(0, state.displayReadyCandidateCount || 0) * 1_000 +
    Math.max(0, state.profileDetailCandidateCount || 0) * 100 +
    Math.max(0, state.rowPublicationSequence || 0) * 100
  );
}

function canonicalProjectionRevision(state: BoardRuntimeState | undefined): string {
  if (String(state?.rowPublicationTier || "").trim() !== "serving_projection_members") {
    return "";
  }
  return String(state?.rowPublicationRevision || "").trim();
}

function boardRuntimeStatePublicationTierRank(state: BoardRuntimeState | undefined): number {
  if (!state) {
    return 0;
  }
  const tier = String(state.rowPublicationTier || "").trim();
  if (tier === "serving_projection_members" && canonicalProjectionRevision(state)) {
    return 4;
  }
  if (tier === "current_snapshot_serving") {
    return 3;
  }
  if (
    ["current_snapshot_serving", "canonical_projection_serving"].includes(state.phase) &&
    state.publicationStatus === "complete" &&
    Math.max(0, state.expectedCandidateCount || 0) > 0 &&
    Math.max(0, state.servedCandidateCount || 0) >= Math.max(0, state.expectedCandidateCount || 0)
  ) {
    return 3;
  }
  if (tier === "partial_patch") {
    return 2;
  }
  if (tier === "lifecycle") {
    return 1;
  }
  return Math.max(0, state.rowPublicationSequence || 0) > 0 ? 2 : 1;
}

function pickFresherBoardRuntimeState(
  incoming: BoardRuntimeState | undefined,
  current: BoardRuntimeState | undefined,
): BoardRuntimeState | undefined {
  if (!incoming) {
    return current;
  }
  if (!current) {
    return incoming;
  }
  const incomingTier = boardRuntimeStatePublicationTierRank(incoming);
  const currentTier = boardRuntimeStatePublicationTierRank(current);
  if (incomingTier !== currentTier) {
    return incomingTier > currentTier ? incoming : current;
  }
  if (incomingTier === 4) {
    const incomingEqualityRevision = canonicalProjectionRevision(incoming);
    const currentEqualityRevision = canonicalProjectionRevision(current);
    if (!incomingEqualityRevision) {
      return current;
    }
    if (!currentEqualityRevision) {
      return incoming;
    }
    if (incomingEqualityRevision !== currentEqualityRevision) {
      // The storage-owned revision is an equality token, not an ordered
      // sequence. Conflicting snapshots require an authoritative dashboard
      // refresh; timestamp and arrival order are not freshness proofs.
      return current;
    }
    // Equal revisions describe the same semantic member input, but they do not
    // order independently built facet/index metadata. Preserve the fresher
    // complete state instead of allowing a later-arriving pending snapshot to
    // regress the UI.
    return boardRuntimeStateFreshnessScore(incoming) >= boardRuntimeStateFreshnessScore(current)
      ? incoming
      : current;
  }
  const incomingSequence = Math.max(0, incoming.rowPublicationSequence || 0);
  const currentSequence = Math.max(0, current.rowPublicationSequence || 0);
  if (incomingSequence !== currentSequence) {
    return incomingSequence > currentSequence ? incoming : current;
  }
  return boardRuntimeStateFreshnessScore(incoming) >= boardRuntimeStateFreshnessScore(current) ? incoming : current;
}

export function dashboardCandidatePageRevisionMatches(
  dashboard: DashboardData,
  page: DashboardCandidatePage,
): boolean {
  const currentProjectionRevision = canonicalProjectionRevision(dashboard.boardRuntimeState);
  const pageProjectionRevision = canonicalProjectionRevision(page.boardRuntimeState);
  if (!currentProjectionRevision && !pageProjectionRevision) {
    return true;
  }
  const currentCount = Math.max(0, dashboard.boardRuntimeState?.expectedCandidateCount || 0);
  const pageCount = Math.max(0, page.boardRuntimeState?.expectedCandidateCount || 0);
  return Boolean(
    currentProjectionRevision &&
    pageProjectionRevision &&
    currentProjectionRevision === pageProjectionRevision &&
    currentCount === pageCount
  );
}

export function mergeDashboardCandidatePage(
  dashboard: DashboardData,
  page: DashboardCandidatePage,
): DashboardData {
  if (!dashboardCandidatePageRevisionMatches(dashboard, page)) {
    // Candidate identity membership cannot be reconciled by retaining or
    // truncating an older row window. A fresh dashboard owns replacement.
    return dashboard;
  }
  const mergedCandidates = mergeDashboardCandidatesInPageOrder(dashboard.candidates, page.candidates, {
    offset: page.offset,
  });
  const mergedBoardRuntimeState = pickFresherBoardRuntimeState(page.boardRuntimeState, dashboard.boardRuntimeState);
  const totalCandidates = mergedBoardRuntimeState
    ? Math.max(0, mergedBoardRuntimeState.expectedCandidateCount || 0)
    : Math.max(
        page.totalCandidates,
        mergedCandidates.length,
        dashboardExpectedCandidateCount(dashboard),
      );
  const pageFacetSummary = candidateFacetSummaryMatchesCanonicalBoard(
    page.candidateFacetSummary,
    page.candidateFacetSummaryScope,
    mergedBoardRuntimeState,
  )
    ? page.candidateFacetSummary
    : undefined;
  const currentFacetSummary = candidateFacetSummaryMatchesCanonicalBoard(
    dashboard.candidateFacetSummary,
    dashboard.candidateFacetSummaryScope,
    mergedBoardRuntimeState,
  )
    ? dashboard.candidateFacetSummary
    : undefined;
  const candidateFacetSummary = pickCanonicalCandidateFacetSummary(pageFacetSummary, currentFacetSummary);
  const candidateFacetSummaryScope =
    !candidateFacetSummary
      ? ""
      : candidateFacetSummary === pageFacetSummary
      ? page.candidateFacetSummaryScope || ""
      : candidateFacetSummary === currentFacetSummary
        ? dashboard.candidateFacetSummaryScope || ""
        : "";
  return {
    ...dashboard,
    resultMode: page.resultMode,
    resultModeLabel: page.resultMode === "asset_population" ? "公司级资产视图" : "检索排序结果",
    rankedCandidateCount:
      page.resultMode === "ranked_results"
        ? Math.max(totalCandidates, dashboard.rankedCandidateCount)
        : dashboard.rankedCandidateCount,
    assetPopulationCount:
      page.resultMode === "asset_population"
        ? totalCandidates
        : dashboard.assetPopulationCount,
    totalCandidates,
    layers: candidateFacetSummary?.layers?.length
      ? candidateFacetSummary.layers
      : buildLayeredSegmentationOptions(mergedCandidates),
    groups: Array.from(new Set(["All", ...mergedCandidates.map((candidate) => candidate.team || "Unknown")])),
    candidates: mergedCandidates,
    profileFetchProgress: page.profileFetchProgress || dashboard.profileFetchProgress,
    linkedinStage1Progress: pickFresherLinkedinProgress(page.linkedinStage1Progress, dashboard.linkedinStage1Progress),
    resultViewLifecycle: pickFresherResultViewLifecycle(page.resultViewLifecycle, dashboard.resultViewLifecycle),
    boardRuntimeState: mergedBoardRuntimeState,
    executionPhaseContract: dashboard.executionPhaseContract,
    effectiveExecutionSemantics: dashboard.effectiveExecutionSemantics,
    candidateFacetSummary,
    candidateFacetSummaryScope,
  };
}

export async function getDashboard(
  jobId?: string,
  options?: {
    forceRefresh?: boolean;
  },
): Promise<DashboardData> {
  if (!jobId) {
    throw new Error("Missing job_id. Real search results require a completed backend job.");
  }
  const cacheKey = jobId;
  const forceRefresh = options?.forceRefresh === true;
  if (forceRefresh) {
    dashboardCache.delete(cacheKey);
  }
  const cached = forceRefresh ? null : readFreshCacheValue(dashboardCache, cacheKey, DASHBOARD_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = forceRefresh ? undefined : dashboardPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    const requestGeneration = (dashboardRequestGeneration.get(cacheKey) || 0) + 1;
    dashboardRequestGeneration.set(cacheKey, requestGeneration);
    fetchPromise = fetchJson<any>(`/api/jobs/${jobId}/dashboard?include_candidates=0`, undefined, RESULTS_API_TIMEOUT_MS)
      .catch(async (error) => {
        const message = error instanceof Error ? error.message : "";
        if (message.includes("Request failed: 410")) {
          const projectionId = await getRunProjectionId(jobId);
          return getProjectionDashboard(projectionId, {
            forceRefresh: true,
            runId: jobId,
          });
        }
        if (message.includes("Request failed: 404")) {
          return fetchJson<any>(`/api/jobs/${jobId}/results`, undefined, RESULTS_API_TIMEOUT_MS);
        }
        throw error;
      })
      .then(async (payload) => {
        const summaryDashboard = mapJobResultsToDashboard(payload);
        const jobStatus = asString(payload?.job?.status).toLowerCase();
        const terminalJobStatus = resolveWorkflowStatus(jobStatus).terminal;
        const shouldCacheDashboard = (dashboard: DashboardData) =>
          terminalJobStatus && dashboardHasRenderableCandidates(dashboard);
        const initialHydrationTarget = summaryDashboard.boardRuntimeState
          ? dashboardRowHydrationTargetCount(summaryDashboard)
          : dashboardExpectedCandidateCount(summaryDashboard);
        const initialChunkTarget = Math.min(
          initialHydrationTarget,
          DASHBOARD_INITIAL_CANDIDATE_CHUNK_SIZE,
        );
        const shouldBackfillFirstPage =
          initialHydrationTarget > 0
          && summaryDashboard.candidates.length < initialChunkTarget;
        if (shouldBackfillFirstPage) {
          const firstPage = await getDashboardCandidatePage(jobId, {
            offset: 0,
            limit: DASHBOARD_INITIAL_CANDIDATE_CHUNK_SIZE,
            forceRefresh,
          }).catch(() => null);
          if (firstPage) {
            const mergedDashboard = mergeDashboardCandidatePage(summaryDashboard, firstPage);
            return shouldCacheDashboard(mergedDashboard) && dashboardRequestGeneration.get(cacheKey) === requestGeneration
              ? storeDashboardCache(cacheKey, mergedDashboard)
              : mergedDashboard;
          }
        }
        if (!shouldCacheDashboard(summaryDashboard)) {
          return summaryDashboard;
        }
        return dashboardRequestGeneration.get(cacheKey) === requestGeneration
          ? storeDashboardCache(cacheKey, summaryDashboard)
          : summaryDashboard;
      })
      .finally(() => {
        evictPromiseCacheEntry(dashboardPromiseCache, cacheKey, fetchPromise as Promise<DashboardData>);
      });
    dashboardPromiseCache.set(cacheKey, fetchPromise);
  }
  return fetchPromise;
}

export function peekDashboardCache(jobId?: string): DashboardData | null {
  if (!jobId) {
    return null;
  }
  return readFreshCacheValue(dashboardCache, jobId, DASHBOARD_CACHE_TTL_MS);
}

export function peekProjectionDashboardCache(projectionId?: string): DashboardData | null {
  if (!projectionId) {
    return null;
  }
  return readFreshCacheValue(projectionDashboardCache, projectionId, DASHBOARD_CACHE_TTL_MS);
}

export async function getRunProjectionId(runId: string): Promise<string> {
  if (!runId) {
    throw new Error("Missing run_id. Projection result pages require a linked acquisition run.");
  }
  let fetchPromise = runProjectionLinkPromiseCache.get(runId);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(`/api/runs/${encodeURIComponent(runId)}/projection-link`, undefined, RESULTS_API_TIMEOUT_MS)
      .then((payload) => {
        const projectionId = pickFirstString(payload, ["projection_id"]);
        if (!projectionId) {
          throw new Error("Run projection link is missing projection_id.");
        }
        return projectionId;
      })
      .finally(() => {
        evictPromiseCacheEntry(runProjectionLinkPromiseCache, runId, fetchPromise as Promise<string>);
      });
    runProjectionLinkPromiseCache.set(runId, fetchPromise);
  }
  return fetchPromise;
}

export async function getCollectionAuthoritativeProjectionId(collectionId: string): Promise<string> {
  const normalizedCollectionId = collectionId.trim();
  if (!normalizedCollectionId) {
    throw new Error("Missing collection_id. Local asset pages require a collection authoritative projection.");
  }
  let fetchPromise = collectionProjectionLinkPromiseCache.get(normalizedCollectionId);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/collections/${encodeURIComponent(normalizedCollectionId)}/authoritative-projection`,
      undefined,
      RESULTS_API_TIMEOUT_MS,
    )
      .then((payload) => {
        const projectionId = pickFirstString(payload, ["projection_id"]);
        if (!projectionId) {
          throw new Error("Collection authoritative pointer is missing projection_id.");
        }
        return projectionId;
      })
      .finally(() => {
        evictPromiseCacheEntry(
          collectionProjectionLinkPromiseCache,
          normalizedCollectionId,
          fetchPromise as Promise<string>,
        );
      });
    collectionProjectionLinkPromiseCache.set(normalizedCollectionId, fetchPromise);
  }
  return fetchPromise;
}

export interface CollectionCompanyMedia {
  logoStatus: string;
  logoUrl: string;
  logoAssetId: string;
  logoAlt: string;
  placeholderKind: string;
  placeholderText: string;
  fallbackUsed: boolean;
  fallbackReason: string;
  raw: any;
}

export interface CollectionAssetEntry {
  status: string;
  collectionId: string;
  displayName: string;
  companyMedia: CollectionCompanyMedia;
  projectionId: string;
  activeCollectionVersion: string;
  candidateCount: number;
  profileFetchRequiredCount: number;
  profileFetchedCount: number;
  cardMaterializedCount: number;
  countScope: string;
  coverageStatus: string;
  coverageKind: string;
  rawProfileIndexWatermark: string;
  evidenceIndexWatermark: string;
  acquisitionHandoffAvailable: boolean;
  acquisitionHandoffReason: string;
  updatedAt: string;
  raw: any;
}

export interface CollectionAssetOverviewItem {
  collectionId: string;
  displayName: string;
  companyMedia: CollectionCompanyMedia;
  status: string;
  reason: string;
  activeProjectionId: string;
  activeCollectionVersion: string;
  candidateCount: number;
  profileFetchRequiredCount: number;
  profileFetchedCount: number;
  cardMaterializedCount: number;
  coverageStatus: string;
  coverageKind: string;
  rawProfileIndexWatermark: string;
  evidenceIndexWatermark: string;
  updatedAt: string;
  publishedAt: string;
  projectionUrl: string;
  raw: any;
}

export interface CollectionAssetOverview {
  status: string;
  collectionCount: number;
  collections: CollectionAssetOverviewItem[];
  raw: any;
}

export interface CompanyAssetRecord {
  assetId: string;
  companyKey: string;
  targetCompany: string;
  assetType: string;
  sourceKind: string;
  contentRef: string;
  sourceUrl: string;
  visibilityScope: string;
  status: string;
  metadata: Record<string, unknown>;
  raw: Record<string, unknown>;
}

export interface CompanyEvidenceRecord {
  evidenceId: string;
  companyKey: string;
  targetCompany: string;
  assetId: string;
  evidenceType: string;
  value: string;
  sourceUrl: string;
  sourceDomain: string;
  status: string;
  metadata: Record<string, unknown>;
  raw: Record<string, unknown>;
}

export interface CompanyAssertionRecord {
  assertionId: string;
  companyKey: string;
  targetCompany: string;
  assertionType: string;
  value: string;
  authority: string;
  verificationStatus: string;
  sourceEvidenceId: string;
  metadata: Record<string, unknown>;
  raw: Record<string, unknown>;
}

export interface CompanyAssetFacts {
  status: string;
  fallbackUsed: boolean;
  assets: CompanyAssetRecord[];
  evidence: CompanyEvidenceRecord[];
  assertions: CompanyAssertionRecord[];
}

function mapCollectionCompanyMedia(source: unknown, displayName: string): CollectionCompanyMedia {
  const item = source && typeof source === "object" && !Array.isArray(source) ? (source as Record<string, unknown>) : {};
  const placeholder =
    item.placeholder && typeof item.placeholder === "object" && !Array.isArray(item.placeholder)
      ? (item.placeholder as Record<string, unknown>)
      : {};
  const mediaContract =
    item.media_contract && typeof item.media_contract === "object" && !Array.isArray(item.media_contract)
      ? (item.media_contract as Record<string, unknown>)
      : {};
  const fallbackUsed = asBoolean(mediaContract.fallback_used) ?? true;
  return {
    logoStatus: pickFirstString(item, ["logo_status"]) || "company_media_missing",
    logoUrl: resolveApiMediaUrl(pickFirstString(item, ["logo_url"])),
    logoAssetId: pickFirstString(item, ["logo_asset_id"]),
    logoAlt: pickFirstString(item, ["logo_alt"]) || displayName,
    placeholderKind: pickFirstString(placeholder, ["kind"]) || "unavailable",
    placeholderText: pickFirstString(placeholder, ["text"]) || "??",
    fallbackUsed,
    fallbackReason: pickFirstString(mediaContract, ["reason"]) || (fallbackUsed ? "company_media_contract_missing" : ""),
    raw: item,
  };
}

function mapCollectionAssetOverviewItem(item: Record<string, unknown>): CollectionAssetOverviewItem {
  const displayName = pickFirstString(item, ["display_name"]) || pickFirstString(item, ["collection_id"]);
  return {
    collectionId: pickFirstString(item, ["collection_id"]),
    displayName,
    companyMedia: mapCollectionCompanyMedia(item.company_media, displayName),
    status: pickFirstString(item, ["status"]),
    reason: pickFirstString(item, ["reason"]),
    activeProjectionId: pickFirstString(item, ["active_projection_id"]),
    activeCollectionVersion: pickFirstString(item, ["active_collection_version"]),
    candidateCount: asNumber(item.candidate_count) ?? 0,
    profileFetchRequiredCount: asNumber(item.profile_fetch_required_count) ?? 0,
    profileFetchedCount: asNumber(item.profile_fetched_count) ?? 0,
    cardMaterializedCount: asNumber(item.card_materialized_count) ?? 0,
    coverageStatus: pickFirstString(item, ["coverage_status"]),
    coverageKind: pickFirstString(item, ["coverage_kind"]),
    rawProfileIndexWatermark: pickFirstString(item, ["raw_profile_index_watermark"]),
    evidenceIndexWatermark: pickFirstString(item, ["evidence_index_watermark"]),
    updatedAt: pickFirstString(item, ["updated_at"]),
    publishedAt: pickFirstString(item, ["published_at"]),
    projectionUrl: pickFirstString(item, ["projection_url"]),
    raw: item,
  };
}

export async function getCollectionAssetOverview(limit = 250): Promise<CollectionAssetOverview> {
  const cacheKey = String(limit || 250);
  const cached = readFreshCacheValue(collectionAssetOverviewCache, cacheKey, LIST_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = collectionAssetOverviewPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/collections${buildApiQueryString({ limit })}`,
      undefined,
      RESULTS_API_TIMEOUT_MS,
    )
      .then((payload) => {
        const overview = {
          status: asString(payload.status),
          collectionCount: asNumber(payload.collection_count) ?? 0,
          collections: asArray(payload.collections)
            .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
            .map(mapCollectionAssetOverviewItem),
          raw: payload,
        };
        collectionAssetOverviewCache.set(cacheKey, { value: overview, cachedAt: Date.now() });
        return overview;
      })
      .finally(() => {
        evictPromiseCacheEntry(collectionAssetOverviewPromiseCache, cacheKey, fetchPromise as Promise<CollectionAssetOverview>);
      });
    collectionAssetOverviewPromiseCache.set(cacheKey, fetchPromise);
  }
  return fetchPromise;
}

export async function getCollectionAssetEntry(collectionId: string): Promise<CollectionAssetEntry> {
  const normalizedCollectionId = collectionId.trim();
  if (!normalizedCollectionId) {
    throw new Error("Missing collection_id. Local asset pages require a collection authoritative projection.");
  }
  const payload = await fetchJson<any>(
    `/api/collections/${encodeURIComponent(normalizedCollectionId)}/asset-entry`,
    undefined,
    RESULTS_API_TIMEOUT_MS,
  );
  const assetEntry = (payload.asset_entry as Record<string, unknown>) || {};
  const pointer = (payload.pointer as Record<string, unknown>) || {};
  const projection = (payload.projection as Record<string, unknown>) || {};
  const coverage = (assetEntry.coverage as Record<string, unknown>) || {};
  const acquisitionHandoff = (assetEntry.acquisition_handoff as Record<string, unknown>) || {};
  const displayName =
    pickFirstString(assetEntry, ["display_name"]) ||
    pickFirstString(payload, ["display_name"]) ||
    normalizedCollectionId.replace(/^company:/i, "");
  return {
    status: asString(payload.status),
    collectionId: pickFirstString(payload, ["collection_id"]) || normalizedCollectionId,
    displayName,
    companyMedia: mapCollectionCompanyMedia(assetEntry.company_media, displayName),
    projectionId: pickFirstString(payload, ["projection_id"]) || pickFirstString(assetEntry, ["active_projection_id"]),
    activeCollectionVersion:
      pickFirstString(assetEntry, ["active_collection_version"]) ||
      pickFirstString(pointer, ["active_collection_version"]),
    candidateCount: asNumber(assetEntry.candidate_count) ?? 0,
    profileFetchRequiredCount: asNumber(assetEntry.profile_fetch_required_count) ?? 0,
    profileFetchedCount: asNumber(assetEntry.profile_fetched_count) ?? 0,
    cardMaterializedCount: asNumber(assetEntry.card_materialized_count) ?? 0,
    countScope: pickFirstString(assetEntry, ["count_scope"]),
    coverageStatus: pickFirstString(coverage, ["coverage_status"]),
    coverageKind: pickFirstString(coverage, ["coverage_kind"]),
    rawProfileIndexWatermark: pickFirstString(assetEntry, ["raw_profile_index_watermark"]),
    evidenceIndexWatermark: pickFirstString(assetEntry, ["evidence_index_watermark"]),
    acquisitionHandoffAvailable: asBoolean(acquisitionHandoff.available) ?? false,
    acquisitionHandoffReason: pickFirstString(acquisitionHandoff, ["reason"]),
    updatedAt: pickFirstString(projection, ["updated_at"]) || pickFirstString(pointer, ["updated_at"]),
    raw: payload,
  };
}

function mapCompanyAssetRecord(record: Record<string, unknown>): CompanyAssetRecord {
  return {
    assetId: pickFirstString(record, ["asset_id"]),
    companyKey: pickFirstString(record, ["company_key"]),
    targetCompany: pickFirstString(record, ["target_company"]),
    assetType: pickFirstString(record, ["asset_type"]),
    sourceKind: pickFirstString(record, ["source_kind"]),
    contentRef: pickFirstString(record, ["content_ref"]),
    sourceUrl: pickFirstString(record, ["source_url"]),
    visibilityScope: pickFirstString(record, ["visibility_scope"]),
    status: pickFirstString(record, ["status"]),
    metadata: (record.metadata as Record<string, unknown>) || {},
    raw: record,
  };
}

function mapCompanyEvidenceRecord(record: Record<string, unknown>): CompanyEvidenceRecord {
  return {
    evidenceId: pickFirstString(record, ["evidence_id"]),
    companyKey: pickFirstString(record, ["company_key"]),
    targetCompany: pickFirstString(record, ["target_company"]),
    assetId: pickFirstString(record, ["asset_id"]),
    evidenceType: pickFirstString(record, ["evidence_type"]),
    value: pickFirstString(record, ["value"]),
    sourceUrl: pickFirstString(record, ["source_url"]),
    sourceDomain: pickFirstString(record, ["source_domain"]),
    status: pickFirstString(record, ["status"]),
    metadata: (record.metadata as Record<string, unknown>) || {},
    raw: record,
  };
}

function mapCompanyAssertionRecord(record: Record<string, unknown>): CompanyAssertionRecord {
  return {
    assertionId: pickFirstString(record, ["assertion_id"]),
    companyKey: pickFirstString(record, ["company_key"]),
    targetCompany: pickFirstString(record, ["target_company"]),
    assertionType: pickFirstString(record, ["assertion_type"]),
    value: pickFirstString(record, ["value"]),
    authority: pickFirstString(record, ["authority"]),
    verificationStatus: pickFirstString(record, ["verification_status"]),
    sourceEvidenceId: pickFirstString(record, ["source_evidence_id"]),
    metadata: (record.metadata as Record<string, unknown>) || {},
    raw: record,
  };
}

function emptyCompanyAssetFacts(status = "not_ready"): CompanyAssetFacts {
  return {
    status,
    fallbackUsed: false,
    assets: [],
    evidence: [],
    assertions: [],
  };
}

export async function getCompanyAssetFacts(params: {
  companyKey?: string;
  targetCompany?: string;
  limit?: number;
}): Promise<CompanyAssetFacts> {
  const companyKey = String(params.companyKey || "").trim();
  const targetCompany = String(params.targetCompany || "").trim();
  if (!companyKey && !targetCompany) {
    return emptyCompanyAssetFacts("invalid");
  }
  const query = buildApiQueryString({
    company_key: companyKey,
    target_company: targetCompany,
    limit: params.limit || 100,
  });
  try {
    const [assetsPayload, evidencePayload, assertionsPayload] = await Promise.all([
      fetchJson<any>(`/api/company-assets${query}`, undefined, RESULTS_API_TIMEOUT_MS),
      fetchJson<any>(`/api/company-assets/evidence${query}`, undefined, RESULTS_API_TIMEOUT_MS),
      fetchJson<any>(`/api/company-assets/assertions${query}`, undefined, RESULTS_API_TIMEOUT_MS),
    ]);
    return {
      status: "ready",
      fallbackUsed:
        Boolean(assetsPayload?.read_contract?.fallback_used) ||
        Boolean(evidencePayload?.read_contract?.fallback_used) ||
        Boolean(assertionsPayload?.read_contract?.fallback_used),
      assets: asArray(assetsPayload.assets)
        .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
        .map(mapCompanyAssetRecord),
      evidence: asArray(evidencePayload.evidence)
        .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
        .map(mapCompanyEvidenceRecord),
      assertions: asArray(assertionsPayload.assertions)
        .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
        .map(mapCompanyAssertionRecord),
    };
  } catch {
    return emptyCompanyAssetFacts("not_ready");
  }
}

function publicProjectionMemberToCandidateRecord(member: Record<string, unknown>): Record<string, unknown> {
  const publicSummary = (member.public_summary as Record<string, unknown>) || {};
  const projectionMetrics = (member.projection_metrics as Record<string, unknown>) || {};
  const mediaSummary = (member.media_summary as Record<string, unknown>) || {};
  return {
    ...publicSummary,
    media_summary: mediaSummary,
    candidate_identity_key: pickFirstString(member, ["candidate_identity_key"]),
    person_identity_key: pickFirstString(member, ["person_identity_key"]),
    profile_url_key: pickFirstString(member, ["profile_url_key"]) || pickFirstString(publicSummary, ["profile_url_key"]),
    candidate_id:
      pickFirstString(member, ["candidate_id"]) ||
      pickFirstString(publicSummary, ["candidate_id", "id"]) ||
      pickFirstString(member, ["candidate_identity_key"]),
    id:
      pickFirstString(member, ["candidate_id"]) ||
      pickFirstString(publicSummary, ["candidate_id", "id"]) ||
      pickFirstString(member, ["candidate_identity_key", "person_identity_key"]),
    employment_status:
      pickFirstString(member, ["employment_scope"]) ||
      pickFirstString(publicSummary, ["employment_status", "status"]),
    source_dataset:
      pickFirstString(member, ["source_shard_key", "lane"]) ||
      pickFirstString(publicSummary, ["source_dataset"]),
    has_profile_detail:
      publicSummary.has_profile_detail ?? projectionMetrics.has_profile_detail,
    needs_profile_completion:
      publicSummary.needs_profile_completion ?? projectionMetrics.needs_profile_completion,
    low_profile_richness:
      publicSummary.low_profile_richness ?? projectionMetrics.low_profile_richness,
  };
}

function nonNegativeInteger(value: unknown): number | null {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const count = Number(value);
  return Number.isInteger(count) && count >= 0 ? count : null;
}

function projectionVisibleMemberCount(
  projection: Record<string, unknown>,
  counts: Record<string, unknown>,
): number | null {
  const readContract = (projection.read_contract as Record<string, unknown>) || {};
  if (
    String(readContract.source || "").trim() !== "serving_projection_members" ||
    readContract.fallback_used !== false ||
    readContract.fail_closed !== true ||
    String(counts.count_scope || "").trim() !== "exact_projection"
  ) {
    return null;
  }
  const visibleCount = nonNegativeInteger(projection.visible_member_count);
  if (visibleCount === null) {
    return null;
  }
  return [counts.result_count, counts.candidate_count, counts.visible_member_count].every(
    (value) => nonNegativeInteger(value) === visibleCount,
  )
    ? visibleCount
    : null;
}

function projectionMembershipRevision(projection: Record<string, unknown>): string {
  return String(projection.membership_revision || "").trim();
}

function projectionCardReadinessSummary(
  totalCandidateCount: number,
  readiness: Record<string, unknown>,
): {
  cardReadyCount: number;
  profileReadyCount: number;
  explicitProfileCaptureCount: number | null;
  needsProfileCompletionCount: number | null;
  lowProfileRichnessCount: number | null;
  previewCount: number;
  qualityFieldsAvailable: boolean;
  statusText: string;
} {
  const totalCount = Math.max(0, Math.floor(Number(totalCandidateCount) || 0));
  const rowCount = nonNegativeInteger(readiness.row_count);
  const profileReadyCount = nonNegativeInteger(readiness.profile_ready_count);
  const cardReadyCount = nonNegativeInteger(readiness.card_ready_count);
  const qualityFieldsAvailable = Boolean(
    String(readiness.count_scope || "").trim() === "exact_projection" &&
      String(readiness.row || "").trim().toLowerCase() === "complete" &&
      rowCount === totalCount &&
      profileReadyCount !== null &&
      cardReadyCount !== null &&
      profileReadyCount <= totalCount &&
      cardReadyCount <= totalCount,
  );
  const exactCardReadyCount = qualityFieldsAvailable ? cardReadyCount || 0 : 0;
  const exactProfileReadyCount = qualityFieldsAvailable ? profileReadyCount || 0 : 0;
  const optionalOwnedCount = (key: string): number | null => {
    if (!qualityFieldsAvailable || !(key in readiness)) {
      return null;
    }
    const count = nonNegativeInteger(readiness[key]);
    return count !== null && count <= totalCount ? count : null;
  };
  return {
    cardReadyCount: exactCardReadyCount,
    profileReadyCount: exactProfileReadyCount,
    explicitProfileCaptureCount: optionalOwnedCount("explicit_profile_capture_candidate_count"),
    needsProfileCompletionCount: optionalOwnedCount("needs_profile_completion_candidate_count"),
    lowProfileRichnessCount: optionalOwnedCount("low_profile_richness_candidate_count"),
    previewCount: Math.max(0, totalCount - exactCardReadyCount),
    qualityFieldsAvailable,
    statusText: qualityFieldsAvailable && totalCount > 0
      ? `卡片详情已合入看板 ${exactCardReadyCount}/${totalCount}`
      : "",
  };
}

function projectionPayloadToDashboard(
  payload: any,
  candidates: Candidate[] = [],
  runId = "",
): DashboardData {
  const projection = (payload.projection as Record<string, unknown>) || {};
  const counts = (projection.counts as Record<string, unknown>) || {};
  const readiness = (projection.readiness as Record<string, unknown>) || {};
  const scopeSpec = (projection.scope_spec as Record<string, unknown>) || {};
  const projectionId = pickFirstString(projection, ["projection_id"]);
  const sourceRunId = pickFirstString(projection, ["source_run_id"]) || runId;
  const membershipRevision = projectionMembershipRevision(projection);
  if (!membershipRevision) {
    throw new Error("Canonical projection revision is unavailable.");
  }
  const resultCount = projectionVisibleMemberCount(projection, counts);
  if (resultCount === null) {
    throw new Error("Canonical projection membership is unavailable or inconsistent.");
  }
  const payloadCandidateCount = nonNegativeInteger(payload.total_candidates);
  if ((payloadCandidateCount !== null && payloadCandidateCount !== resultCount) || candidates.length > resultCount) {
    throw new Error("Canonical projection rows do not match the membership contract.");
  }
  const cardReadiness = projectionCardReadinessSummary(resultCount, readiness);
  const targetCompany =
    pickFirstString(scopeSpec, ["target_company"]) ||
    pickFirstString(projection, ["collection_id"]).replace(/^company:/, "");
  const intentKeywords = canonicalizeDisplayKeywords(
    asArray(scopeSpec.keywords).map((value) => asString(value)).filter(Boolean),
  );
  const candidateFacetSummary = mapCandidateFacetSummary(payload.facet_summary);
  const candidateFacetSummaryScope = candidateFacetSummary
    ? mapCandidateFacetSummaryScope(payload, "exact_projection")
    : "";
  return {
    projectionId,
    title: pickFirstString(projection, ["scope_label"]) || "Projection results",
    snapshotId: pickFirstString((projection.provenance as Record<string, unknown>) || {}, ["snapshot_id"]) || projectionId || "projection",
    queryLabel: pickFirstString(projection, ["scope_label"]) || "Projection results",
    targetCompany,
    intentKeywords,
    resultMode: "asset_population",
    resultModeLabel: "本地公司资产",
    rankedCandidateCount: 0,
    assetPopulationCount: resultCount,
    totalCandidates: resultCount,
    totalEvidence: candidates.reduce((sum, candidate) => sum + (candidate.evidence || []).length, 0),
    manualReviewCount: 0,
    layers: candidateFacetSummary?.layers?.length
      ? candidateFacetSummary.layers
      : buildLayeredSegmentationOptions(candidates),
    groups: Array.from(new Set(["All", ...candidates.map((candidate) => candidate.team || "Unknown")])),
    candidates,
    resultViewLifecycle: {
      state: "projection_serving",
      baselineSnapshotId: "",
      currentSnapshotId: "",
      servedSnapshotId: "",
      baselineCandidateCount: 0,
      servedCandidateCount: resultCount,
      expectedCandidateCount: resultCount,
      deltaProfileProgressApplicable: false,
      deltaProfileProgressReason: "projection_reader",
      deltaProfileRequiredCount: Number(readiness.profile_required_count || 0) || 0,
      deltaProfileFetchedCount: cardReadiness.profileReadyCount,
      deltaProfileMaterializedCount: cardReadiness.cardReadyCount,
      deltaProfileBoardVisibleCount: cardReadiness.cardReadyCount,
      deltaProfilePendingCount: 0,
      deltaProfileQueuedCount: 0,
      deltaProfileRetryableCount: 0,
      servingProjectionId: projectionId,
      servingProjectionPhase: "canonical_projection_serving",
      backgroundSnapshotMaterializationStatus: "",
      outreachLayeringStatus: "",
    },
    boardRuntimeState: {
      schemaVersion: 1,
      jobId: sourceRunId,
      resultMode: "asset_population",
      phase: "canonical_projection_serving",
      publicationStatus: "complete",
      expectedCandidateCount: resultCount,
      servedCandidateCount: resultCount,
      publishedCandidateCount: resultCount,
      displayReadyCandidateCount: cardReadiness.cardReadyCount,
      previewCandidateCount: cardReadiness.previewCount,
      profileDetailCandidateCount: cardReadiness.profileReadyCount,
      explicitProfileCaptureCandidateCount: cardReadiness.explicitProfileCaptureCount,
      needsProfileCompletionCandidateCount: cardReadiness.needsProfileCompletionCount,
      lowProfileRichnessCandidateCount: cardReadiness.lowProfileRichnessCount,
      cardMaterializationQualityFieldsAvailable: cardReadiness.qualityFieldsAvailable,
      rowHydrationTargetCount: resultCount,
      candidateDiscoveryCount: resultCount,
      profileFetchRequiredCount: Number(readiness.profile_required_count || 0) || 0,
      profileFetchedCount: cardReadiness.profileReadyCount,
      baselineCandidateCount: 0,
      deltaProfileRequiredCount: Number(readiness.profile_required_count || 0) || 0,
      deltaProfileFetchedCount: cardReadiness.profileReadyCount,
      deltaProfileMaterializedCount: cardReadiness.cardReadyCount,
      deltaProfileBoardVisibleCount: cardReadiness.cardReadyCount,
      deltaProfileDenominatorPromoted: true,
      rowPublicationSequence: 0,
      rowPublicationTier: "serving_projection_members",
      rowPublicationRevision: membershipRevision,
      rowPublicationWatermark: pickFirstString(projection, ["updated_at", "published_at"]),
      rowPublicationUpdatedAt: pickFirstString(projection, ["updated_at", "published_at"]),
      facetSummaryStatus: candidateFacetSummary ? "complete" : "unavailable",
      facetSummaryScope: candidateFacetSummaryScope || "unavailable",
      facetSummaryCandidateCount: candidateFacetSummary?.candidateCount || 0,
      layeringStatus: candidateFacetSummary ? "completed" : "unavailable",
      filterContract: {
        source: "serving_projection_reader",
        facetCountScope: candidateFacetSummaryScope || "unavailable",
        rowFilterScope: "projection_membership",
        backendFilteredPagingSupported: true,
      },
      syncStatusText: `${resultCount}/${resultCount}`,
      syncNoteLines: [
        { id: "candidate_discovery", text: `候选人发现 ${resultCount}/${resultCount}` },
        ...(cardReadiness.statusText
          ? [{ id: "card_materialization", text: cardReadiness.statusText }]
          : []),
      ],
      candidateDiscoveryStatusText: `候选人发现 ${resultCount}/${resultCount}`,
      profileFetchStatusText: "",
      cardMaterializationStatusText: cardReadiness.statusText,
      noteText: [
        `候选人发现 ${resultCount}/${resultCount}`,
        cardReadiness.statusText,
      ].filter(Boolean).join("；"),
    },
    effectiveExecutionSemantics: {
      effectiveAcquisitionMode: "projection_serving",
      defaultResultsMode: "asset_population",
      executionStrategyLabel: "本地公司资产",
      fullLocalAssetReuse: false,
      requiresDeltaAcquisition: false,
      assetPopulationSupported: true,
    },
    candidateFacetSummary,
    candidateFacetSummaryScope,
  };
}

export async function getProjectionDashboard(
  projectionId: string,
  options?: {
    forceRefresh?: boolean;
    runId?: string;
  },
): Promise<DashboardData> {
  if (!projectionId) {
    throw new Error("Missing projection_id. Result pages require a canonical projection.");
  }
  const forceRefresh = options?.forceRefresh === true;
  if (forceRefresh) {
    projectionDashboardCache.delete(projectionId);
  }
  const cached = forceRefresh ? null : readFreshCacheValue(projectionDashboardCache, projectionId, DASHBOARD_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = forceRefresh ? undefined : projectionDashboardPromiseCache.get(projectionId);
  if (!fetchPromise) {
    const requestGeneration = (projectionDashboardRequestGeneration.get(projectionId) || 0) + 1;
    projectionDashboardRequestGeneration.set(projectionId, requestGeneration);
    fetchPromise = Promise.all([
      fetchJson<any>(`/api/projections/${encodeURIComponent(projectionId)}`, undefined, RESULTS_API_TIMEOUT_MS),
      getProjectionCandidatePage(projectionId, {
        offset: 0,
        limit: DASHBOARD_INITIAL_CANDIDATE_CHUNK_SIZE,
        forceRefresh,
      }),
    ])
      .then(([projectionPayload, page]) => {
        const dashboard = projectionPayloadToDashboard(
          projectionPayload,
          [],
          options?.runId || "",
        );
        if (page && !dashboardCandidatePageRevisionMatches(dashboard, page)) {
          projectionDashboardCache.delete(projectionId);
          throw new Error("Canonical projection summary and page revisions do not match.");
        }
        const mergedDashboard = page ? mergeDashboardCandidatePage(dashboard, page) : dashboard;
        return projectionDashboardRequestGeneration.get(projectionId) === requestGeneration
          ? storeProjectionDashboardCache(projectionId, mergedDashboard)
          : mergedDashboard;
      })
      .finally(() => {
        evictPromiseCacheEntry(projectionDashboardPromiseCache, projectionId, fetchPromise as Promise<DashboardData>);
      });
    projectionDashboardPromiseCache.set(projectionId, fetchPromise);
  }
  return fetchPromise;
}

export function storeProjectionDashboardCache(projectionId: string, dashboard: DashboardData): DashboardData {
  return writeCacheValue(projectionDashboardCache, projectionId, dashboard);
}

export async function getProjectionCandidatePage(
  projectionId: string,
  options?: {
    offset?: number;
    limit?: number;
    forceRefresh?: boolean;
    filter?: DashboardCandidatePageFilter;
  },
): Promise<DashboardCandidatePage> {
  const offset = Math.max(Number(options?.offset || 0), 0);
  const limit = Math.max(Number(options?.limit || DASHBOARD_BACKGROUND_CANDIDATE_CHUNK_SIZE), 1);
  const forceRefresh = options?.forceRefresh === true;
  const filterSignature = dashboardCandidatePageFilterSignature(options?.filter);
  const filterQueryParams = candidatePageFilterQueryParams(options?.filter);
  const cacheKey = `${projectionId}:${offset}:${limit}:${filterSignature}`;
  let fetchPromise: Promise<DashboardCandidatePage> | undefined = forceRefresh
    ? undefined
    : projectionCandidatePagePromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/projections/${encodeURIComponent(projectionId)}/candidates${buildApiQueryString({
        offset,
        limit,
        ...filterQueryParams,
      })}`,
      undefined,
      RESULTS_API_TIMEOUT_MS,
    )
      .then((payload): DashboardCandidatePage => {
        const candidates = asArray(payload.candidates)
          .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
          .map((item) => publicProjectionMemberToCandidateRecord(item))
          .map((item) => deriveCandidate(item));
        const dashboard = projectionPayloadToDashboard(
          {
            projection: payload.projection,
            total_candidates: payload.total_candidates,
            facet_summary: payload.facet_summary,
            facet_summary_scope: payload.facet_summary_scope || payload.facet_summary?.count_scope,
          },
          candidates,
        );
        return {
          jobId: pickFirstString(payload.projection || {}, ["source_run_id"]) || projectionId,
          resultMode: "asset_population",
          offset: Number(payload.offset || 0) || 0,
          limit: Number(payload.limit || limit) || limit,
          returnedCount: candidates.length,
          totalCandidates: Number(payload.total_candidates || payload.candidate_count || 0) || 0,
          filteredCandidateCount:
            Number(payload.filtered_candidate_count ?? payload.total_candidates ?? payload.candidate_count ?? 0) || 0,
          hasMore: Boolean(payload.has_more),
          nextOffset:
            typeof payload.next_offset === "number" && Number.isFinite(payload.next_offset)
              ? Number(payload.next_offset)
              : null,
          candidates,
          resultViewLifecycle: dashboard.resultViewLifecycle,
          boardRuntimeState: dashboard.boardRuntimeState,
          candidateFacetSummary: dashboard.candidateFacetSummary,
          candidateFacetSummaryScope: dashboard.candidateFacetSummaryScope,
          filterSignature: asString(payload.filter_signature) || filterSignature,
          filterContract: mapCandidatePageFilterContract(payload.filter_contract),
        };
      })
      .finally(() => {
        evictPromiseCacheEntry(
          projectionCandidatePagePromiseCache,
          cacheKey,
          fetchPromise as Promise<DashboardCandidatePage>,
        );
      });
    projectionCandidatePagePromiseCache.set(cacheKey, fetchPromise);
  }
  return fetchPromise;
}

export async function getDashboardCandidatePage(
  jobId: string,
  options?: {
    offset?: number;
    limit?: number;
    forceRefresh?: boolean;
    lightweight?: boolean;
    filter?: DashboardCandidatePageFilter;
  },
): Promise<DashboardCandidatePage> {
  const offset = Math.max(Number(options?.offset || 0), 0);
  const limit = Math.max(Number(options?.limit || DASHBOARD_BACKGROUND_CANDIDATE_CHUNK_SIZE), 1);
  const forceRefresh = options?.forceRefresh === true;
  const lightweight = options?.lightweight !== false;
  const filterSignature = dashboardCandidatePageFilterSignature(options?.filter);
  const filterQueryParams = candidatePageFilterQueryParams(options?.filter);
  const cacheKey = dashboardCandidatePageCacheKey(jobId, offset, limit, lightweight, filterSignature);
  let fetchPromise: Promise<DashboardCandidatePage> | undefined = forceRefresh
    ? undefined
    : dashboardCandidatePagePromiseCache.get(cacheKey);
  if (!fetchPromise) {
    if (useMock) {
      const mockCandidates = mockDashboard.candidates.slice(offset, offset + limit);
      fetchPromise = Promise.resolve({
        jobId,
        resultMode: mockDashboard.resultMode,
        offset,
        limit,
        returnedCount: mockCandidates.length,
        totalCandidates: mockDashboard.totalCandidates,
        filteredCandidateCount: mockDashboard.totalCandidates,
        hasMore: offset + mockCandidates.length < mockDashboard.totalCandidates,
        nextOffset: offset + mockCandidates.length < mockDashboard.totalCandidates ? offset + mockCandidates.length : null,
        candidates: mockCandidates,
        profileFetchProgress: mockDashboard.profileFetchProgress,
        linkedinStage1Progress: mockDashboard.linkedinStage1Progress,
        resultViewLifecycle: mockDashboard.resultViewLifecycle,
        boardRuntimeState: mockDashboard.boardRuntimeState,
        candidateFacetSummary: mockDashboard.candidateFacetSummary,
        candidateFacetSummaryScope: mockDashboard.candidateFacetSummaryScope,
      } satisfies DashboardCandidatePage);
      dashboardCandidatePagePromiseCache.set(cacheKey, fetchPromise);
      return fetchPromise;
    }
    fetchPromise = fetchJson<any>(
      `/api/jobs/${jobId}/candidates${buildApiQueryString({
        offset,
        limit,
        lightweight: lightweight ? 1 : undefined,
        force_refresh: forceRefresh ? 1 : undefined,
        ...filterQueryParams,
      })}`,
      undefined,
      RESULTS_API_TIMEOUT_MS,
    )
      .then((payload): DashboardCandidatePage => {
        const resultMode: DashboardData["resultMode"] =
          asString(payload.result_mode) === "asset_population" ? "asset_population" : "ranked_results";
        return {
          jobId: asString(payload.job_id) || jobId,
          resultMode,
          offset: Number(payload.offset || 0) || 0,
          limit: Number(payload.limit || limit) || limit,
          returnedCount: Number(payload.returned_count || 0) || 0,
          totalCandidates: Number(payload.total_candidates || 0) || 0,
          filteredCandidateCount:
            Number(payload.filtered_candidate_count ?? payload.total_candidates ?? 0) || 0,
          hasMore: Boolean(payload.has_more),
          nextOffset:
            typeof payload.next_offset === "number" && Number.isFinite(payload.next_offset)
              ? Number(payload.next_offset)
              : null,
          candidates: asArray(payload.candidates)
            .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
            .filter((item) => Object.keys(item).length > 0)
            .map((item) => deriveCandidate(item)),
          profileFetchProgress: mapProfileFetchProgress(
            (payload.profile_fetch_progress as Record<string, unknown>) || {},
          ),
          linkedinStage1Progress: mapLinkedinStage1Progress(
            (payload.linkedin_stage_1_progress as Record<string, unknown>) || {},
          ),
          resultViewLifecycle: mapResultViewLifecycle(
            (payload.result_view_lifecycle as Record<string, unknown>) || {},
          ),
          boardRuntimeState: mapBoardRuntimeState(
            (payload.board_runtime_state as Record<string, unknown>) || {},
          ),
          candidateFacetSummary: mapCandidateFacetSummary(payload.facet_summary),
          candidateFacetSummaryScope: asString(payload.facet_summary_scope),
          filterSignature: asString(payload.filter_signature) || filterSignature,
          filterContract: mapCandidatePageFilterContract(payload.filter_contract),
        };
      })
      .finally(() => {
        evictPromiseCacheEntry(
          dashboardCandidatePagePromiseCache,
          cacheKey,
          fetchPromise as Promise<DashboardCandidatePage>,
        );
      });
    if (fetchPromise) {
      dashboardCandidatePagePromiseCache.set(cacheKey, fetchPromise);
    }
  }
  return fetchPromise as Promise<DashboardCandidatePage>;
}

export async function getDashboardBoardPatches(
  jobId: string,
  options?: {
    afterPublishedAt?: string;
    afterSequence?: number;
    limit?: number;
  },
): Promise<BoardVisiblePatchLog> {
  if (!jobId) {
    throw new Error("Missing job_id. Board patch polling requires a valid backend job.");
  }
  const payload = await fetchJson<any>(
    `/api/jobs/${jobId}/board-patches${buildApiQueryString({
      after_published_at: options?.afterPublishedAt || undefined,
      after_sequence: options?.afterSequence || undefined,
      limit: options?.limit || 50,
    })}`,
    undefined,
    RESULTS_API_TIMEOUT_MS,
  );
  return {
    jobId: asString(payload.job_id) || jobId,
    patches: asArray(payload.patches)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => ({
        patchId: pickFirstString(item, ["patch_id"]),
        sequenceIndex: Number(item.sequence_index || 0) || 0,
        candidateCount: Number(item.candidate_count || 0) || 0,
        cumulativeCandidateCount: Number(item.cumulative_candidate_count || 0) || 0,
        servedCandidateCount: Number(item.served_candidate_count || 0) || 0,
        displayReadyCandidateCount: Number(item.display_ready_candidate_count || 0) || 0,
        profileDetailCandidateCount: Number(item.profile_detail_candidate_count || 0) || 0,
        explicitProfileCaptureCandidateCount: Number(item.explicit_profile_capture_candidate_count || 0) || 0,
        previewCandidateCount: Number(item.preview_candidate_count || 0) || 0,
        needsProfileCompletionCandidateCount: Number(item.needs_profile_completion_candidate_count || 0) || 0,
        lowProfileRichnessCandidateCount: Number(item.low_profile_richness_candidate_count || 0) || 0,
        qualityFieldsAvailable: Boolean(item.quality_fields_available),
        publishedAt: pickFirstString(item, ["published_at"]),
      })),
    returnedCount: Number(payload.returned_count || 0) || 0,
    hasMore: Boolean(payload.has_more),
    latestPublishedAt: pickFirstString(payload, ["latest_published_at"]),
    latestSequenceIndex: Number(payload.latest_sequence_index || 0) || 0,
    boardRuntimeState: mapBoardRuntimeState((payload.board_runtime_state as Record<string, unknown>) || {}),
    resultViewLifecycle: mapResultViewLifecycle((payload.result_view_lifecycle as Record<string, unknown>) || {}),
  };
}

export async function triggerJobCandidateProfileCompletion(payload: {
  jobId: string;
  candidateIds: string[];
  forceRefresh?: boolean;
}): Promise<Record<string, unknown>> {
  const candidateIds = Array.from(
    new Set(
      (payload.candidateIds || [])
        .map((item) => asString(item))
        .filter(Boolean),
    ),
  );
  if (!payload.jobId || candidateIds.length === 0) {
    throw new Error("Missing job_id or candidate_ids for profile completion.");
  }
  return fetchJson<Record<string, unknown>>(
    `/api/jobs/${payload.jobId}/profile-completion`,
    {
      method: "POST",
      body: JSON.stringify({
        candidate_ids: candidateIds,
        force_refresh: payload.forceRefresh !== false,
      }),
    },
    PROFILE_COMPLETION_TIMEOUT_MS,
  );
}

export async function supplementCompanyAssets(payload: {
  targetCompany: string;
  snapshotId?: string;
  importLocalBootstrapPackage?: boolean;
  syncProjectLocalPackage?: boolean;
  buildArtifacts?: boolean;
  repairCurrentRosterProfileRefs?: boolean;
  repairCurrentRosterRegistryAliases?: boolean;
  runFormerSearchSeed?: boolean;
  formerSearchLimit?: number;
  formerSearchPages?: number;
  formerSearchQueries?: string[];
}): Promise<SupplementOperationResult> {
  return fetchJson<SupplementOperationResult>(
    "/api/company-assets/supplement",
    {
      method: "POST",
      body: JSON.stringify({
        target_company: payload.targetCompany,
        snapshot_id: payload.snapshotId || "",
        import_local_bootstrap_package: payload.importLocalBootstrapPackage === true,
        sync_project_local_package: payload.syncProjectLocalPackage !== false,
        build_artifacts: payload.buildArtifacts !== false,
        repair_current_roster_profile_refs: payload.repairCurrentRosterProfileRefs === true,
        repair_current_roster_registry_aliases: payload.repairCurrentRosterRegistryAliases === true,
        run_former_search_seed: payload.runFormerSearchSeed === true,
        former_search_limit: payload.formerSearchLimit || 25,
        former_search_pages: payload.formerSearchPages || 1,
        former_search_queries: payload.formerSearchQueries || [],
      }),
    },
    PROFILE_COMPLETION_TIMEOUT_MS,
  );
}

export async function ingestExcelContacts(payload: {
  file?: File;
  filePath?: string;
  fileContentBase64?: string;
  filename?: string;
  targetCompany?: string;
  snapshotId?: string;
  attachToSnapshot?: boolean;
  buildArtifacts?: boolean;
}): Promise<ExcelIntakeResponse> {
  const body = payload.file
    ? (() => {
        const form = new FormData();
        form.append("file", payload.file, payload.filename || payload.file.name);
        appendOptionalFormField(form, "filename", payload.filename || payload.file.name);
        appendOptionalFormField(form, "target_company", payload.targetCompany || "");
        appendOptionalFormField(form, "snapshot_id", payload.snapshotId || "");
        appendOptionalFormField(form, "attach_to_snapshot", payload.attachToSnapshot === true);
        appendOptionalFormField(form, "build_artifacts", payload.buildArtifacts !== false);
        return form;
      })()
    : JSON.stringify({
        file_path: payload.filePath || "",
        file_content_base64: payload.fileContentBase64 || "",
        filename: payload.filename || "",
        target_company: payload.targetCompany || "",
        snapshot_id: payload.snapshotId || "",
        attach_to_snapshot: payload.attachToSnapshot === true,
        build_artifacts: payload.buildArtifacts !== false,
      });
  return fetchJson<ExcelIntakeResponse>(
    "/api/intake/excel",
    {
      method: "POST",
      body,
    },
    PROFILE_COMPLETION_TIMEOUT_MS,
  );
}

export async function startExcelIntakeWorkflow(payload: {
  file?: File;
  filePath?: string;
  fileContentBase64?: string;
  filename?: string;
  historyId?: string;
  queryText?: string;
  attachToSnapshot?: boolean;
  buildArtifacts?: boolean;
}): Promise<{
  status: string;
  workflowKind?: string;
  batchId: string;
  inputFilename: string;
  totalRowCount: number;
  createdJobCount: number;
  groupCount: number;
  unassignedRowCount: number;
  unassignedRows: Array<{
    rowKey: string;
    name: string;
    company: string;
    title: string;
  }>;
  groups: Array<{
    status: string;
    jobId: string;
    historyId: string;
    queryText: string;
    workflowKind?: string;
    targetCompany: string;
    rowCount: number;
    sourceCompanies: string[];
  }>;
  jobId?: string;
  historyId?: string;
  queryText?: string;
  raw: unknown;
}> {
  const body = payload.file
    ? (() => {
        const form = new FormData();
        form.append("file", payload.file, payload.filename || payload.file.name);
        appendOptionalFormField(form, "filename", payload.filename || payload.file.name);
        appendOptionalFormField(form, "history_id", payload.historyId || "");
        appendOptionalFormField(form, "query_text", payload.queryText || "");
        appendOptionalFormField(form, "attach_to_snapshot", payload.attachToSnapshot !== false);
        appendOptionalFormField(form, "build_artifacts", payload.buildArtifacts !== false);
        return form;
      })()
    : JSON.stringify({
        file_path: payload.filePath || "",
        file_content_base64: payload.fileContentBase64 || "",
        filename: payload.filename || "",
        history_id: payload.historyId || "",
        query_text: payload.queryText || "",
        attach_to_snapshot: payload.attachToSnapshot !== false,
        build_artifacts: payload.buildArtifacts !== false,
      });
  const response = await fetchJson<any>(
    "/api/intake/excel/workflow",
    {
      method: "POST",
      body,
    },
    PROFILE_COMPLETION_TIMEOUT_MS,
  );
  return {
    status: String(response.status || ""),
    batchId: String(response.batch_id || ""),
    inputFilename: String(response.input_filename || payload.filename || ""),
    totalRowCount: Number(response.total_row_count || 0),
    createdJobCount: Number(response.created_job_count || 0),
    groupCount: Number(response.group_count || 0),
    unassignedRowCount: Number(response.unassigned_row_count || 0),
    unassignedRows: Array.isArray(response.unassigned_rows)
      ? response.unassigned_rows.map((item: any) => ({
          rowKey: String(item?.row_key || ""),
          name: String(item?.name || ""),
          company: String(item?.company || ""),
          title: String(item?.title || ""),
        }))
      : [],
    groups: Array.isArray(response.groups)
      ? response.groups.map((item: any) => ({
          status: String(item?.status || ""),
          jobId: String(item?.job_id || ""),
          historyId: String(item?.history_id || ""),
          queryText: String(item?.query_text || ""),
          workflowKind: String(item?.workflow_kind || ""),
          targetCompany: String(item?.target_company || ""),
          rowCount: Number(item?.row_count || 0),
          sourceCompanies: Array.isArray(item?.source_companies)
            ? item.source_companies.map((value: unknown) => String(value || "")).filter(Boolean)
            : [],
        }))
      : [],
    jobId: response.job_id ? String(response.job_id || "") : undefined,
    historyId: response.history_id ? String(response.history_id || "") : undefined,
    queryText: response.query_text ? String(response.query_text || "") : undefined,
    workflowKind: String(response.workflow_kind || ""),
    raw: response,
  };
}

export async function continueExcelIntakeReview(payload: {
  intakeId: string;
  decisions: Array<Record<string, unknown>>;
  targetCompany?: string;
  snapshotId?: string;
  attachToSnapshot?: boolean;
  buildArtifacts?: boolean;
}): Promise<ExcelIntakeResponse> {
  return fetchJson<ExcelIntakeResponse>(
    "/api/intake/excel/continue",
    {
      method: "POST",
      body: JSON.stringify({
        intake_id: payload.intakeId,
        decisions: payload.decisions,
        target_company: payload.targetCompany || "",
        snapshot_id: payload.snapshotId || "",
        attach_to_snapshot: payload.attachToSnapshot === true,
        build_artifacts: payload.buildArtifacts !== false,
      }),
    },
    PROFILE_COMPLETION_TIMEOUT_MS,
  );
}

export async function getManualReviewItems(options?: {
  jobId?: string;
  targetCompany?: string;
  status?: string;
}): Promise<ManualReviewItem[]> {
  if (preferLocalAssets) {
    const localItems = await getManualReviewItemsFromLocalAssets();
    if (localItems) {
      return localItems;
    }
  }
  if (useMock) {
    return mockManualReviewItems;
  }
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    targetCompany: options?.targetCompany || "",
    status: options?.status ?? "open",
  });
  const cached = readFreshCacheValue(manualReviewItemsCache, cacheKey, LIST_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = manualReviewItemsPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/manual-review${buildApiQueryString({
        job_id: options?.jobId,
        target_company: options?.targetCompany,
        status: options?.status ?? "open",
      })}`,
    )
      .then((payload) => {
        const items = Array.isArray(payload.manual_review_items) ? payload.manual_review_items : [];
        return writeCacheValue(
          manualReviewItemsCache,
          cacheKey,
          items.map((item: Record<string, unknown>) => deriveManualReviewItem(item)),
        );
      })
      .finally(() => {
        manualReviewItemsPromiseCache.delete(cacheKey);
      });
    manualReviewItemsPromiseCache.set(cacheKey, fetchPromise);
  }
  return fetchPromise;
}

export function peekManualReviewItemsCache(options?: {
  jobId?: string;
  targetCompany?: string;
  status?: string;
}): ManualReviewItem[] {
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    targetCompany: options?.targetCompany || "",
    status: options?.status ?? "open",
  });
  return readFreshCacheValue(manualReviewItemsCache, cacheKey, LIST_CACHE_TTL_MS) || [];
}

export async function getCandidateReviewRecords(options?: {
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  status?: CandidateReviewStatus;
}): Promise<CandidateReviewRecord[]> {
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    historyId: options?.historyId || "",
    candidateId: options?.candidateId || "",
    status: options?.status || "",
  });
  const cached = readFreshCacheValue(candidateReviewRecordsCache, cacheKey, LIST_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = candidateReviewRecordsPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/candidate-review-registry${buildApiQueryString({
        job_id: options?.jobId,
        history_id: options?.historyId,
        candidate_id: options?.candidateId,
        status: options?.status,
      })}`,
    )
      .then((payload) => {
        const items = Array.isArray(payload.candidate_review_records) ? payload.candidate_review_records : [];
        return writeCacheValue(
          candidateReviewRecordsCache,
          cacheKey,
          items.map((item: Record<string, unknown>) => deriveCandidateReviewRecord(item)),
        );
      })
      .finally(() => {
        candidateReviewRecordsPromiseCache.delete(cacheKey);
      });
    candidateReviewRecordsPromiseCache.set(cacheKey, fetchPromise);
  }
  return fetchPromise;
}

export function peekCandidateReviewRecordsCache(options?: {
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  status?: CandidateReviewStatus;
}): CandidateReviewRecord[] {
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    historyId: options?.historyId || "",
    candidateId: options?.candidateId || "",
    status: options?.status || "",
  });
  return readFreshCacheValue(candidateReviewRecordsCache, cacheKey, LIST_CACHE_TTL_MS) || [];
}

export async function upsertCandidateReviewRecord(payload: {
  id?: string;
  jobId?: string;
  historyId?: string;
  candidateId: string;
  candidateName: string;
  headline?: string;
  currentCompany?: string;
  avatarUrl?: string;
  linkedinUrl?: string;
  primaryEmail?: string;
  status: CandidateReviewStatus;
  comment?: string;
  source?: "manual_add" | "backend_override" | "manual_review";
  metadata?: Record<string, unknown>;
}): Promise<CandidateReviewRecord> {
  const response = await fetchJson<any>("/api/candidate-review-registry", {
    method: "POST",
    body: JSON.stringify({
      record_id: payload.id,
      job_id: payload.jobId || "",
      history_id: payload.historyId || "",
      candidate_id: payload.candidateId,
      candidate_name: payload.candidateName,
      headline: payload.headline || "",
      current_company: payload.currentCompany || "",
      avatar_url: payload.avatarUrl || "",
      linkedin_url: payload.linkedinUrl || "",
      primary_email: payload.primaryEmail || "",
      status: payload.status,
      comment: payload.comment || "",
      source: payload.source || "manual_review",
      metadata: payload.metadata || {},
    }),
  });
  candidateReviewRecordsCache.clear();
  candidateReviewRecordsPromiseCache.clear();
  manualReviewItemsCache.clear();
  manualReviewItemsPromiseCache.clear();
  return deriveCandidateReviewRecord((response.candidate_review_record || {}) as Record<string, unknown>);
}

export async function synthesizeManualReviewItem(
  reviewItemId: number,
  options?: {
    forceRefresh?: boolean;
  },
): Promise<ManualReviewItem> {
  const payload = await fetchJson<any>("/api/manual-review/synthesize", {
    method: "POST",
    body: JSON.stringify({
      review_item_id: reviewItemId,
      force_refresh: Boolean(options?.forceRefresh),
    }),
  });
  return deriveManualReviewItem((payload.manual_review_item || {}) as Record<string, unknown>);
}

export async function reviewManualReviewItem(
  reviewItemId: number,
  action: "open" | "resolve" | "dismiss" | "escalate",
  options?: {
    reviewer?: string;
    notes?: string;
    metadataMerge?: Record<string, unknown>;
  },
): Promise<ManualReviewItem> {
  const payload = await fetchJson<any>("/api/manual-review/review", {
    method: "POST",
    body: JSON.stringify({
      review_item_id: reviewItemId,
      action,
      reviewer: options?.reviewer || "",
      notes: options?.notes || "",
      metadata_merge: options?.metadataMerge || {},
    }),
  });
  manualReviewItemsCache.clear();
  manualReviewItemsPromiseCache.clear();
  candidateReviewRecordsCache.clear();
  candidateReviewRecordsPromiseCache.clear();
  return deriveManualReviewItem((payload.manual_review_item || {}) as Record<string, unknown>);
}

export async function getTargetCandidates(options?: {
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
  sourceProjectionId?: string;
  sourceCollectionId?: string;
}): Promise<TargetCandidateRecord[]> {
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    historyId: options?.historyId || "",
    candidateId: options?.candidateId || "",
    followUpStatus: options?.followUpStatus || "",
    sourceProjectionId: options?.sourceProjectionId || "",
    sourceCollectionId: options?.sourceCollectionId || "",
  });
  const cached = readFreshCacheValue(targetCandidatesCache, cacheKey, LIST_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = targetCandidatesPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/crm/records${buildApiQueryString({
        source_projection_id: options?.sourceProjectionId,
        source_collection_id: options?.sourceCollectionId,
        limit: 1000,
      })}`,
    )
      .then((payload) => {
        const items = Array.isArray(payload.crm_records) ? payload.crm_records : [];
        const mapped = items.map((item: Record<string, unknown>) => deriveTargetCandidateRecord(item));
        const filtered = mapped.filter((item: TargetCandidateRecord) => {
          if (options?.jobId && item.jobId !== options.jobId) {
            return false;
          }
          if (options?.historyId && item.historyId !== options.historyId) {
            return false;
          }
          if (options?.candidateId && item.candidateId !== options.candidateId) {
            return false;
          }
          if (options?.followUpStatus && item.followUpStatus !== options.followUpStatus) {
            return false;
          }
          return true;
        });
        return writeCacheValue(
          targetCandidatesCache,
          cacheKey,
          filtered,
        );
      })
      .finally(() => {
        targetCandidatesPromiseCache.delete(cacheKey);
      });
    targetCandidatesPromiseCache.set(cacheKey, fetchPromise);
  }
  return fetchPromise;
}

export function peekTargetCandidatesCache(options?: {
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
  sourceProjectionId?: string;
  sourceCollectionId?: string;
}): TargetCandidateRecord[] {
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    historyId: options?.historyId || "",
    candidateId: options?.candidateId || "",
    followUpStatus: options?.followUpStatus || "",
    sourceProjectionId: options?.sourceProjectionId || "",
    sourceCollectionId: options?.sourceCollectionId || "",
  });
  return readFreshCacheValue(targetCandidatesCache, cacheKey, LIST_CACHE_TTL_MS) || [];
}

export async function addProjectionCandidateToCrm(payload: {
  projectionId: string;
  candidateIdentityKey: string;
  expectedMembershipRevision: string;
  workspaceId?: string;
  stage?: string;
  sourceReason?: string;
  idempotencyKey?: string;
}): Promise<TargetCandidateRecord> {
  if (!payload.expectedMembershipRevision.trim()) {
    throw new Error("Canonical projection revision is required for CRM selection.");
  }
  const response = await fetchJson<any>("/api/crm/records", {
    method: "POST",
    body: JSON.stringify({
      projection_id: payload.projectionId,
      workspace_id: payload.workspaceId || "default",
      candidate_identity_key: payload.candidateIdentityKey,
      expected_membership_revision: payload.expectedMembershipRevision,
      stage: payload.stage || "outreach_ready",
      source_reason: payload.sourceReason || "operator_selected_from_projection",
      idempotency_key: payload.idempotencyKey || "",
    }),
  });
  const responseRevision = String(response.membership_revision || "").trim();
  if (responseRevision !== payload.expectedMembershipRevision.trim()) {
    throw new Error("CRM selection revision does not match the requested projection.");
  }
  targetCandidatesCache.clear();
  targetCandidatesPromiseCache.clear();
  return deriveTargetCandidateRecord((response.crm_record || {}) as Record<string, unknown>);
}

export async function addProjectionCandidatesToCrm(payload: {
  projectionId: string;
  candidateIdentityKeys: string[];
  expectedMembershipRevision: string;
  workspaceId?: string;
  stage?: string;
  sourceReason?: string;
  idempotencyKey?: string;
}): Promise<{
  records: TargetCandidateRecord[];
  requestedCandidateCount: number;
  successfulWriteCount: number;
  failedWriteCount: number;
  membershipRevision: string;
}> {
  const candidateIdentityKeys = Array.from(
    new Set(payload.candidateIdentityKeys.map((value) => value.trim()).filter(Boolean)),
  );
  if (!payload.expectedMembershipRevision.trim()) {
    throw new Error("Canonical projection revision is required for CRM selection.");
  }
  const response = await fetchJson<any>("/api/crm/records", {
    method: "POST",
    body: JSON.stringify({
      projection_id: payload.projectionId,
      workspace_id: payload.workspaceId || "default",
      candidate_identity_keys: candidateIdentityKeys,
      expected_membership_revision: payload.expectedMembershipRevision,
      stage: payload.stage || "outreach_ready",
      source_reason: payload.sourceReason || "operator_selected_from_projection",
      idempotency_key: payload.idempotencyKey || "",
    }),
  });
  const responseRevision = String(response.membership_revision || "").trim();
  if (responseRevision !== payload.expectedMembershipRevision.trim()) {
    throw new Error("CRM bulk selection revision does not match the requested projection.");
  }
  targetCandidatesCache.clear();
  targetCandidatesPromiseCache.clear();
  return {
    records: ((response.crm_records || []) as Record<string, unknown>[]).map(deriveTargetCandidateRecord),
    requestedCandidateCount: Number(response.requested_candidate_count || candidateIdentityKeys.length),
    successfulWriteCount: Number(response.successful_write_count || 0),
    failedWriteCount: Number(response.failed_write_count || 0),
    membershipRevision: responseRevision,
  };
}

export async function upsertTargetCandidate(payload: {
  id?: string;
  workspaceId?: string;
  candidateId: string;
  historyId?: string;
  jobId?: string;
  candidateName: string;
  headline?: string;
  currentCompany?: string;
  avatarUrl?: string;
  linkedinUrl?: string;
  primaryEmail?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
  qualityScore?: number | null;
  comment?: string;
  metadata?: Record<string, unknown>;
}): Promise<TargetCandidateRecord> {
  if (!payload.id) {
    throw new Error("CRM target candidate updates require crm_record_id.");
  }
  const response = await fetchJson<any>(`/api/crm/records/${encodeURIComponent(payload.id)}`, {
    method: "PATCH",
    body: JSON.stringify({
      candidate_id: payload.candidateId,
      workspace_id: payload.workspaceId || "default",
      history_id: payload.historyId || "",
      job_id: payload.jobId || "",
      candidate_name: payload.candidateName,
      headline: payload.headline || "",
      current_company: payload.currentCompany || "",
      avatar_url: payload.avatarUrl || "",
      linkedin_url: payload.linkedinUrl || "",
      primary_email: payload.primaryEmail || "",
      follow_up_status: payload.followUpStatus || "pending_outreach",
      quality_score: payload.qualityScore ?? null,
      comment: payload.comment || "",
      metadata: payload.metadata || {},
    }),
  });
  targetCandidatesCache.clear();
  targetCandidatesPromiseCache.clear();
  return deriveTargetCandidateRecord((response.crm_record || {}) as Record<string, unknown>);
}

export async function importTargetCandidatesFromJob(payload: {
  jobId: string;
  historyId?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
  limit?: number;
}): Promise<{
  status: string;
  importedCount: number;
  targetCandidates: TargetCandidateRecord[];
}> {
  const response = await fetchJson<any>("/api/target-candidates/import-from-job", {
    method: "POST",
    body: JSON.stringify({
      job_id: payload.jobId,
      history_id: payload.historyId || "",
      follow_up_status: payload.followUpStatus || "pending_outreach",
      limit: payload.limit || 2000,
    }),
  });
  targetCandidatesCache.clear();
  targetCandidatesPromiseCache.clear();
  const items = Array.isArray(response.target_candidates) ? response.target_candidates : [];
  return {
    status: String(response.status || ""),
    importedCount: Number(response.imported_count || 0),
    targetCandidates: items.map((item: Record<string, unknown>) => deriveTargetCandidateRecord(item)),
  };
}

export async function exportTargetCandidatesArchive(payload?: {
  recordIds?: string[];
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
}): Promise<{
  blob: Blob;
  filename: string;
  contentType: string;
  exportContract: {
    legacyExportPath: string;
    canonicalExportPath: string;
    cutoverStatus: string;
  };
}> {
  void payload;
  throw new Error("当前人选信息导出入口不可用，请使用人选信息导出或 Web Search 导出。");
}

function requireTargetCandidatePublicWebWorkspaceId(workspaceId?: string): string {
  const value = (workspaceId || "").trim();
  if (!value) {
    throw new Error("Public Web Search 操作缺少后端 workspace_id，已阻止本次请求。");
  }
  return value;
}

// C1.4/C1.5: exports are durable async tasks. Submission, wait, and download are
// separate contracts so task_id can later be persisted for reload/resume without
// changing submission semantics. A succeeded response must carry the exact owner-
// supplied artifact route; missing or near-miss handles fail closed.
interface ExportTaskSubmission {
  taskId: string;
  payload: any;
}

const EXPORT_TASK_STATUSES = new Set(["queued", "running", "succeeded", "failed", "cancelled", "expired"]);

function hasUnsafeDecodedExportTaskId(value: string): boolean {
  let decoded = value;
  for (let pass = 0; pass < 2; pass += 1) {
    try {
      decoded = decodeURIComponent(decoded);
    } catch {
      return true;
    }
    if (
      decoded === "." ||
      decoded === ".." ||
      /[\\/?#\u0000-\u001f\u007f]/.test(decoded)
    ) {
      return true;
    }
  }
  return false;
}

function requireExactExportTaskId(value: unknown): string {
  const taskId = typeof value === "string" ? value : "";
  if (
    !taskId ||
    taskId !== taskId.trim() ||
    taskId.includes("%") ||
    taskId === "." ||
    taskId === ".." ||
    /[\\/?#\u0000-\u001f\u007f]/.test(taskId) ||
    /%(?:2f|5c)/i.test(taskId) ||
    /^(?:%2e|%2e%2e)$/i.test(taskId) ||
    hasUnsafeDecodedExportTaskId(taskId)
  ) {
    throw new Error("Export task id is not a valid single path segment.");
  }
  return taskId;
}

function throwOnExportTerminalFailure(status: string, payload: any): void {
  if (status === "failed" || status === "cancelled" || status === "expired") {
    const reason = String((payload && payload.error && payload.error.reason) || status);
    throw new Error(`Export failed: ${reason}`);
  }
}

async function submitExportTask(
  submitPath: string,
  submitBody: unknown,
  timeoutMs = RESULTS_API_TIMEOUT_MS,
): Promise<ExportTaskSubmission> {
  const submitted = await fetchJson<any>(
    submitPath,
    { method: "POST", body: JSON.stringify(submitBody) },
    timeoutMs,
  );
  const taskId = requireExactExportTaskId(submitted && submitted.task_id);
  return { taskId, payload: submitted };
}

async function waitForExportArtifact(
  taskId: string,
  initialPayload: any,
  timeoutMs = RESULTS_API_TIMEOUT_MS,
): Promise<any> {
  requireExactExportTaskId(taskId);
  let payload = initialPayload;
  let status = String((payload && payload.status) || "").trim().toLowerCase();
  if (!EXPORT_TASK_STATUSES.has(status)) {
    throw new Error(`Export returned an unknown status: ${status || "missing"}`);
  }
  throwOnExportTerminalFailure(status, payload);
  const deadline = Date.now() + timeoutMs;
  while (status !== "succeeded") {
    if (Date.now() > deadline) {
      throw new Error("Export timed out waiting for the artifact to be generated.");
    }
    await new Promise((resolve) => setTimeout(resolve, EXPORT_POLL_INTERVAL_MS));
    payload = await fetchJson<any>(`/api/exports/${encodeURIComponent(taskId)}`);
    status = String((payload && payload.status) || "").trim().toLowerCase();
    if (!EXPORT_TASK_STATUSES.has(status)) {
      throw new Error(`Export returned an unknown status: ${status || "missing"}`);
    }
    throwOnExportTerminalFailure(status, payload);
  }
  return payload;
}

function requireExactExportArtifactHandle(taskId: string, payload: any): string {
  const normalizedTaskId = requireExactExportTaskId(taskId);
  const artifact = payload && typeof payload.artifact === "object" && !Array.isArray(payload.artifact)
    ? payload.artifact
    : null;
  const suppliedHandle = artifact && typeof artifact.handle === "string" ? artifact.handle : "";
  const expectedHandle = `/api/exports/${encodeURIComponent(normalizedTaskId)}/artifact`;
  const hasDotSegment = /(?:^|\/)(?:\.{1,2}|%2e(?:%2e)?)(?:\/|$)/i.test(suppliedHandle);
  if (
    !suppliedHandle ||
    suppliedHandle !== suppliedHandle.trim() ||
    !suppliedHandle.startsWith("/") ||
    suppliedHandle.startsWith("//") ||
    /^[a-z][a-z0-9+.-]*:/i.test(suppliedHandle) ||
    /[?#\\]/.test(suppliedHandle) ||
    /%(?:2f|5c)/i.test(suppliedHandle) ||
    hasDotSegment ||
    suppliedHandle !== expectedHandle
  ) {
    throw new Error("Export succeeded without the exact canonical artifact handle.");
  }
  return expectedHandle;
}

export function __testRequireExactExportArtifactHandle(taskId: string, payload: any): string {
  return requireExactExportArtifactHandle(taskId, payload);
}

export function __testRequireExactExportTaskId(value: unknown): string {
  return requireExactExportTaskId(value);
}

async function downloadExportArtifact(
  taskId: string,
  completedPayload: any,
  timeoutMs = RESULTS_API_TIMEOUT_MS,
): Promise<{ blob: Blob; filename: string; contentType: string; headers: Headers }> {
  const canonicalHandle = requireExactExportArtifactHandle(taskId, completedPayload);
  return fetchBinary(canonicalHandle, { method: "GET" }, timeoutMs);
}

export async function exportTargetCandidatePublicWebArchive(payload?: {
  recordIds?: string[];
  workspaceId?: string;
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
  mode?: "promoted_only" | "promoted_and_publishable";
}): Promise<{
  blob: Blob;
  filename: string;
  contentType: string;
  exportStats: {
    recordCount: number;
    exportedRecordCount: number;
    exportedSignalCount: number;
    noPublicWebResultCount: number;
    noExportableSignalCount: number;
    nonTerminalRunCount: number;
  };
}> {
  const workspaceId = requireTargetCandidatePublicWebWorkspaceId(payload?.workspaceId);
  const submission = await submitExportTask("/api/crm/records/public-web-export", {
    crm_record_ids: Array.from(new Set((payload?.recordIds || []).map((value) => value.trim()).filter(Boolean))),
    workspace_id: workspaceId,
    mode: payload?.mode || "promoted_only",
  });
  const completedPayload = await waitForExportArtifact(submission.taskId, submission.payload);
  const result = await downloadExportArtifact(submission.taskId, completedPayload);
  const headerNumber = (name: string): number => {
    const value = Number.parseInt(result.headers.get(name) || "0", 10);
    return Number.isFinite(value) ? value : 0;
  };
  return {
    blob: result.blob,
    filename: result.filename,
    contentType: result.contentType,
    exportStats: {
      recordCount: headerNumber("X-Sourcing-Export-Record-Count"),
      exportedRecordCount: headerNumber("X-Sourcing-Exported-Record-Count"),
      exportedSignalCount: headerNumber("X-Sourcing-Exported-Signal-Count"),
      noPublicWebResultCount: headerNumber("X-Sourcing-No-Public-Web-Result-Count"),
      noExportableSignalCount: headerNumber("X-Sourcing-No-Exportable-Signal-Count"),
      nonTerminalRunCount: headerNumber("X-Sourcing-Non-Terminal-Run-Count"),
    },
  };
}

export async function exportProjectionCandidatesArchive(payload: {
  projectionId: string;
  expectedMembershipRevision: string;
  candidateIdentityKeys?: string[];
  includeLlmReviewedUnconfirmedAssertions?: boolean;
}): Promise<{
  blob: Blob;
  filename: string;
  contentType: string;
  exportStats: {
    projectionId: string;
    membershipRevision: string;
    sourceCandidateCount: number;
    recordCount: number;
    exportedRecordCount: number;
    skippedAssertionCount: number;
  };
}> {
  if (!payload.expectedMembershipRevision.trim()) {
    throw new Error("Canonical projection revision is required for export.");
  }
  const submission = await submitExportTask("/api/projections/export", {
    projection_id: payload.projectionId,
    expected_membership_revision: payload.expectedMembershipRevision,
    candidate_identity_keys: Array.from(
      new Set((payload.candidateIdentityKeys || []).map((value) => value.trim()).filter(Boolean)),
    ),
    include_llm_reviewed_unconfirmed_assertions: Boolean(payload.includeLlmReviewedUnconfirmedAssertions),
  });
  const completedPayload = await waitForExportArtifact(submission.taskId, submission.payload);
  const result = await downloadExportArtifact(submission.taskId, completedPayload);
  const headerNumber = (name: string): number => {
    const value = Number.parseInt(result.headers.get(name) || "0", 10);
    return Number.isFinite(value) ? value : 0;
  };
  const membershipRevision = (result.headers.get("X-Sourcing-Membership-Revision") || "").trim();
  const sourceCandidateCount = headerNumber("X-Sourcing-Source-Candidate-Count");
  if (!membershipRevision) {
    throw new Error("Projection export artifact is missing its membership revision.");
  }
  if (membershipRevision !== payload.expectedMembershipRevision.trim()) {
    throw new Error("Projection export artifact revision does not match the requested projection.");
  }
  const recordCount = headerNumber("X-Sourcing-Export-Record-Count");
  if (recordCount > sourceCandidateCount) {
    throw new Error("Projection export record count exceeds its canonical source membership.");
  }
  return {
    blob: result.blob,
    filename: result.filename,
    contentType: result.contentType,
    exportStats: {
      projectionId: result.headers.get("X-Sourcing-Projection-Id") || payload.projectionId,
      membershipRevision,
      sourceCandidateCount,
      recordCount,
      exportedRecordCount: headerNumber("X-Sourcing-Exported-Record-Count"),
      skippedAssertionCount: headerNumber("X-Sourcing-Skipped-Assertion-Count"),
    },
  };
}

export async function getTargetCandidatePublicWebSearches(options?: {
  batchId?: string;
  recordId?: string;
  recordIds?: string[];
  workspaceId?: string;
  status?: string;
  limit?: number;
}): Promise<TargetCandidatePublicWebSearchState> {
  const workspaceId = requireTargetCandidatePublicWebWorkspaceId(options?.workspaceId);
  const scopedRecordIds = Array.from(new Set((options?.recordIds || []).map((value) => value.trim()).filter(Boolean)));
  if (scopedRecordIds.length > 0) {
    const response = await fetchJson<any>("/api/crm/records/public-web-search/poll", {
      method: "POST",
      body: JSON.stringify({
        batch_id: options?.batchId || "",
        crm_record_ids: scopedRecordIds,
        workspace_id: workspaceId,
        status: options?.status || "",
        limit: options?.limit || Math.min(1000, Math.max(100, scopedRecordIds.length)),
      }),
    });
    return deriveTargetCandidatePublicWebSearchState(response);
  }
  const response = await fetchJson<any>(
    "/api/crm/records/public-web-search/poll",
    {
      method: "POST",
      body: JSON.stringify({
        batch_id: options?.batchId,
        crm_record_ids: options?.recordId ? [options.recordId] : [],
        workspace_id: workspaceId,
        status: options?.status,
        limit: options?.limit,
      }),
    },
  );
  return deriveTargetCandidatePublicWebSearchState(response);
}

export async function getTargetCandidatePublicWebDetail(recordId: string): Promise<TargetCandidatePublicWebDetail> {
  const response = await fetchJson<any>(
    `/api/crm/records/${encodeURIComponent(recordId)}/public-web-search`,
  );
  return deriveTargetCandidatePublicWebDetail(response);
}

export async function getTargetCandidateProfile(recordId: string): Promise<TargetCandidateProfileDetail> {
  const response = await fetchJson<any>(`/api/crm/records/${encodeURIComponent(recordId)}/profile`);
  return deriveTargetCandidateProfileDetail(response);
}

export async function startTargetCandidatePublicWebSearch(payload: {
  recordIds: string[];
  workspaceId?: string;
  options?: Record<string, unknown>;
  forceRefresh?: boolean;
  requestedBy?: string;
}): Promise<TargetCandidatePublicWebStartResult> {
  const workspaceId = requireTargetCandidatePublicWebWorkspaceId(payload.workspaceId);
  const response = await fetchJson<any>("/api/crm/records/public-web-search", {
    method: "POST",
    body: JSON.stringify({
      crm_record_ids: Array.from(new Set((payload.recordIds || []).map((value) => value.trim()).filter(Boolean))),
      workspace_id: workspaceId,
      options: payload.options || {},
      force_refresh: Boolean(payload.forceRefresh),
      requested_by: payload.requestedBy || "",
    }),
  });
  const state = deriveTargetCandidatePublicWebSearchState(response);
  const batch =
    response.batch && typeof response.batch === "object"
      ? deriveTargetCandidatePublicWebBatch(response.batch as Record<string, unknown>)
      : state.batches[0] || null;
  return {
    ...state,
    batch,
    summary:
      response.summary && typeof response.summary === "object" && !Array.isArray(response.summary)
        ? (response.summary as Record<string, unknown>)
        : {},
    workerSummary:
      response.worker_summary && typeof response.worker_summary === "object" && !Array.isArray(response.worker_summary)
        ? (response.worker_summary as Record<string, unknown>)
        : {},
    job:
      response.job && typeof response.job === "object" && !Array.isArray(response.job)
        ? (response.job as Record<string, unknown>)
        : {},
  };
}

export async function cancelTargetCandidatePublicWebSearch(payload: {
  runIds?: string[];
  recordIds?: string[];
  workspaceId?: string;
  batchId?: string;
  reason?: string;
  operator?: string;
}): Promise<TargetCandidatePublicWebActionResult> {
  const workspaceId = requireTargetCandidatePublicWebWorkspaceId(payload.workspaceId);
  const response = await fetchJson<any>("/api/crm/records/public-web-search/cancel", {
    method: "POST",
    body: JSON.stringify({
      run_ids: Array.from(new Set((payload.runIds || []).map((value) => value.trim()).filter(Boolean))),
      crm_record_ids: Array.from(new Set((payload.recordIds || []).map((value) => value.trim()).filter(Boolean))),
      workspace_id: workspaceId,
      batch_id: payload.batchId || "",
      reason: payload.reason || "cancelled_from_target_candidates_panel",
      operator: payload.operator || "frontend",
    }),
  });
  return deriveTargetCandidatePublicWebActionResult(response);
}

export async function retryTargetCandidatePublicWebSearch(payload: {
  runIds?: string[];
  recordIds?: string[];
  workspaceId?: string;
  batchId?: string;
  reason?: string;
  operator?: string;
}): Promise<TargetCandidatePublicWebActionResult> {
  const workspaceId = requireTargetCandidatePublicWebWorkspaceId(payload.workspaceId);
  const response = await fetchJson<any>("/api/crm/records/public-web-search/retry", {
    method: "POST",
    body: JSON.stringify({
      run_ids: Array.from(new Set((payload.runIds || []).map((value) => value.trim()).filter(Boolean))),
      crm_record_ids: Array.from(new Set((payload.recordIds || []).map((value) => value.trim()).filter(Boolean))),
      workspace_id: workspaceId,
      batch_id: payload.batchId || "",
      reason: payload.reason || "retry_requested_from_target_candidates_panel",
      operator: payload.operator || "frontend",
    }),
  });
  return deriveTargetCandidatePublicWebActionResult(response);
}

export async function promoteTargetCandidatePublicWebSignal(payload: {
  recordId: string;
  signalId: string;
  action?: "promote" | "reject";
  operator?: string;
  note?: string;
  allowUnpublishable?: boolean;
  overrideReason?: string;
}): Promise<{
  status: string;
  promotion: TargetCandidatePublicWebPromotion | null;
  detail: TargetCandidatePublicWebDetail | null;
}> {
  const response = await fetchJson<any>(
    `/api/crm/records/${encodeURIComponent(payload.recordId)}/public-web-promotions`,
    {
      method: "POST",
      body: JSON.stringify({
        signal_id: payload.signalId,
        action: payload.action || "promote",
        operator: payload.operator || "frontend",
        note: payload.note || "",
        allow_unpublishable: Boolean(payload.allowUnpublishable),
        override_reason: payload.overrideReason || "",
      }),
    },
  );
  targetCandidatesCache.clear();
  targetCandidatesPromiseCache.clear();
  return {
    status: String(response.status || ""),
    promotion:
      response.promotion && typeof response.promotion === "object" && !Array.isArray(response.promotion)
        ? deriveTargetCandidatePublicWebPromotion(response.promotion as Record<string, unknown>)
        : null,
    detail:
      response.detail && typeof response.detail === "object" && !Array.isArray(response.detail)
        ? deriveTargetCandidatePublicWebDetail(response.detail as Record<string, unknown>)
        : null,
  };
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

export async function getCandidateDetailsBatch(
  candidateIds: string[],
  jobId: string,
): Promise<Record<string, CandidateDetail | null>> {
  if (!jobId) {
    throw new Error("Missing job_id. Candidate detail requests must be scoped to a workflow result set.");
  }
  const normalizedCandidateIds = Array.from(
    new Set(candidateIds.map((candidateId) => candidateId.trim()).filter(Boolean)),
  );
  if (normalizedCandidateIds.length === 0) {
    return {};
  }
  const now = Date.now();
  const cachedDetails: Record<string, CandidateDetail | null> = {};
  const uncachedCandidateIds: string[] = [];
  normalizedCandidateIds.forEach((candidateId) => {
    const cached = candidateDetailCache.get(candidateDetailCacheKey(jobId, candidateId));
    if (cached && now - cached.cachedAt <= CANDIDATE_DETAIL_CACHE_TTL_MS) {
      cachedDetails[candidateId] = cached.value;
      return;
    }
    uncachedCandidateIds.push(candidateId);
  });
  if (uncachedCandidateIds.length === 0) {
    return cachedDetails;
  }
  const batchKey = `${jobId}::${[...uncachedCandidateIds].sort().join(",")}`;
  let fetchPromise = candidateDetailBatchPromiseCache.get(batchKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/jobs/${jobId}/candidates/batch`,
      {
        method: "POST",
        body: JSON.stringify({
          candidate_ids: uncachedCandidateIds,
        }),
      },
      RESULTS_API_TIMEOUT_MS,
    )
      .then((payload) => {
        const detailsByCandidateId: Record<string, CandidateDetail | null> = {};
        asArray(payload.candidates).forEach((item) => {
          const record = item && typeof item === "object" ? (item as Record<string, unknown>) : {};
          const candidateId = asString(record.candidate_id);
          if (!candidateId) {
            return;
          }
          detailsByCandidateId[candidateId] = deriveCandidateDetail(record);
        });
        asArray(payload.not_found_candidate_ids)
          .map((candidateId) => asString(candidateId))
          .filter(Boolean)
          .forEach((candidateId) => {
            detailsByCandidateId[candidateId] = null;
          });
        uncachedCandidateIds.forEach((candidateId) => {
          if (!(candidateId in detailsByCandidateId)) {
            detailsByCandidateId[candidateId] = null;
          }
          candidateDetailCache.set(candidateDetailCacheKey(jobId, candidateId), {
            value: detailsByCandidateId[candidateId],
            cachedAt: Date.now(),
          });
        });
        return detailsByCandidateId;
      })
      .finally(() => {
        candidateDetailBatchPromiseCache.delete(batchKey);
      });
    candidateDetailBatchPromiseCache.set(batchKey, fetchPromise);
  }
  const fetchedDetails = await fetchPromise;
  return {
    ...cachedDetails,
    ...fetchedDetails,
  };
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
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

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function asBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (!normalized) {
      return undefined;
    }
    if (["1", "true", "yes", "y", "on"].includes(normalized)) {
      return true;
    }
    if (["0", "false", "no", "n", "off"].includes(normalized)) {
      return false;
    }
  }
  return undefined;
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

function pickCanonicalMediaSummaryAvatarUrl(...sources: Record<string, unknown>[]): string {
  for (const source of sources) {
    const mediaSummary = asRecord(source.media_summary);
    const mediaContract = asRecord(mediaSummary.media_contract);
    const avatarUrl = asString(mediaSummary.avatar_url);
    if (!avatarUrl) {
      continue;
    }
    if (asString(mediaSummary.avatar_status) !== "available") {
      continue;
    }
    if (asString(mediaContract.source) !== "PersonAsset.avatar_media") {
      continue;
    }
    if (asBoolean(mediaContract.fallback_used) === true) {
      continue;
    }
    return resolveApiMediaUrl(avatarUrl);
  }
  return "";
}

function pickEmailMetadata(
  ...values: unknown[]
): { source?: string; status?: string; qualityScore?: number; foundInLinkedInProfile?: boolean } | undefined {
  for (const value of values) {
    let payload: Record<string, unknown> | null = null;
    if (value && typeof value === "object" && !Array.isArray(value)) {
      payload = value as Record<string, unknown>;
    } else if (typeof value === "string") {
      const text = value.trim();
      if (!text || !text.startsWith("{") || !text.endsWith("}")) {
        continue;
      }
      try {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          payload = parsed as Record<string, unknown>;
        }
      } catch {
        payload = null;
      }
    }
    if (!payload) {
      continue;
    }
    const source = asString(payload.source);
    const status = asString(payload.status);
    const rawQualityScore = payload.qualityScore ?? payload.quality_score;
    const qualityScore =
      typeof rawQualityScore === "number"
        ? rawQualityScore
        : typeof rawQualityScore === "string" && rawQualityScore.trim()
          ? Number(rawQualityScore)
          : undefined;
    const rawFoundInLinkedInProfile =
      payload.foundInLinkedInProfile ?? payload.found_in_linkedin_profile;
    const foundInLinkedInProfile =
      typeof rawFoundInLinkedInProfile === "boolean"
        ? rawFoundInLinkedInProfile
        : typeof rawFoundInLinkedInProfile === "string"
          ? rawFoundInLinkedInProfile.trim().toLowerCase() === "true"
            ? true
            : rawFoundInLinkedInProfile.trim().toLowerCase() === "false"
              ? false
              : undefined
          : undefined;
    if (!source && !status && !Number.isFinite(qualityScore ?? NaN) && foundInLinkedInProfile === undefined) {
      continue;
    }
    return {
      source: source || undefined,
      status: status || undefined,
      qualityScore: Number.isFinite(qualityScore ?? NaN) ? Number(qualityScore) : undefined,
      foundInLinkedInProfile,
    };
  }
  return undefined;
}

function scrubCandidateEmail(
  email: string,
  metadata?: CandidateEmailMetadata,
): {
  email: string;
  metadata?: CandidateEmailMetadata;
} {
  const normalizedEmail = asString(email).trim();
  if (!normalizedEmail) {
    return { email: "" };
  }
  const normalizedMetadata = metadata ? pickEmailMetadata(metadata) : undefined;
  if (normalizedMetadata?.source === "harvestapi" && normalizedMetadata.foundInLinkedInProfile !== true) {
    return { email: "" };
  }
  return {
    email: normalizedEmail,
    metadata: normalizedMetadata,
  };
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

function normalizeStructuredCandidateLines(values: string[]): string[] {
  return values.map((value) => asString(value)).map((value) => value.trim()).filter(Boolean);
}

function candidateProfileLooksComplete(payload: {
  experience: string[];
  education: string[];
  hasProfileDetail?: boolean;
  profileCaptureKind?: string;
  headline?: string;
  summary?: string;
  primaryEmail?: string;
}): boolean {
  const experienceLines = normalizeStructuredCandidateLines(payload.experience);
  const educationLines = normalizeStructuredCandidateLines(payload.education);
  const captureKind = asString(payload.profileCaptureKind).trim().toLowerCase();
  if (PARTIAL_PROFILE_CAPTURE_KINDS.has(captureKind)) {
    if (educationLines.length > 0 || experienceLines.length >= 2) {
      return true;
    }
    return false;
  }
  if (FULL_PROFILE_CAPTURE_KINDS.has(captureKind)) {
    return educationLines.length > 0 || experienceLines.length > 0;
  }
  if (payload.hasProfileDetail) {
    return true;
  }
  if (educationLines.length > 0 || experienceLines.length >= 2) {
    return true;
  }
  return Boolean(
    experienceLines.length > 0 &&
      firstNonEmptyString([
        asString(payload.summary).trim(),
        asString(payload.headline).trim(),
        asString(payload.primaryEmail).trim(),
      ]),
  );
}

function normalizeCandidateProfileStatus(candidate: Candidate): Candidate {
  const inferredHasProfileDetail = candidateProfileLooksComplete({
    experience: candidate.experience,
    education: candidate.education,
    hasProfileDetail: candidate.hasProfileDetail,
    profileCaptureKind: candidate.profileCaptureKind,
    headline: candidate.headline,
    summary: candidate.summary,
    primaryEmail: candidate.primaryEmail,
  });
  const hasProfileDetail = inferredHasProfileDetail;
  const linkedinUrl = asString(candidate.linkedinUrl).trim();
  const needsProfileCompletion = Boolean(linkedinUrl && !hasProfileDetail);
  const experienceLines = normalizeStructuredCandidateLines(candidate.experience);
  const educationLines = normalizeStructuredCandidateLines(candidate.education);
  const lowProfileRichness = Boolean(
    linkedinUrl &&
      hasProfileDetail &&
      !needsProfileCompletion &&
      (candidate.lowProfileRichness || experienceLines.length === 0 || educationLines.length === 0),
  );
  return {
    ...candidate,
    hasProfileDetail,
    needsProfileCompletion,
    lowProfileRichness,
  };
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

function pickFunctionIds(sourceShardFilters: unknown, ...sources: unknown[]): string[] {
  const normalized = new Set<string>();
  for (const source of sources) {
    for (const item of asArray(source)) {
      const value = asString(item);
      if (value) {
        normalized.add(value);
      }
    }
  }
  const values = Array.from(normalized);
  if (values.length !== 1) {
    return [];
  }
  const shardFilterRecord =
    sourceShardFilters && typeof sourceShardFilters === "object"
      ? (sourceShardFilters as Record<string, unknown>)
      : {};
  const includeIds = Array.from(
    new Set(asArray(shardFilterRecord.function_ids).map((item) => asString(item)).filter(Boolean)),
  );
  const excludeIds = new Set(asArray(shardFilterRecord.exclude_function_ids).map((item) => asString(item)).filter(Boolean));
  const effectiveShardIds = includeIds.filter((item) => !excludeIds.has(item));
  if (
    effectiveShardIds.length > 0 &&
    effectiveShardIds.length === values.length &&
    effectiveShardIds.every((item, index) => item === values[index])
  ) {
    return [];
  }
  return values;
}

function deriveCandidate(record: Record<string, unknown>): Candidate {
  const metadata = (record.metadata as Record<string, unknown>) || {};
  const structuredEducationLines = asArray(record.education_lines).map((value) => asString(value)).filter(Boolean);
  const structuredExperienceLines = asArray(record.experience_lines).map((value) => asString(value)).filter(Boolean);
  const profileSkillLines = splitStructuredText(metadata.skills).map((value) => asString(value)).filter(Boolean);
  const matchedFieldRecords = asArray(record.matched_fields).map(
    (item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>),
  );
  const sourceMatches = normalizeSourceMatches(record.source_matches, metadata.source_matches);
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
    ...splitStructuredText(record.skills),
    ...profileSkillLines,
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
    ...matchedFieldRecords.map((source) => {
      return joinFragments([
        pickFirstString(source, ["field"]),
        pickFirstString(source, ["matched_on", "value"]),
      ]);
    }),
  ]
    .map((value) => asString(value))
    .filter(Boolean);
  const matchedKeywords = canonicalizeDisplayKeywords(
    dedupeStrings([
      ...distinctMatchedKeywords(matchedFieldRecords),
      ...sourceMatchKeywords(sourceMatches),
      ...splitStructuredText(record.matched_keywords),
      ...splitStructuredText(metadata.matched_keywords),
    ]),
  );

  const avatarUrl = firstNonEmptyString([
    pickCanonicalMediaSummaryAvatarUrl(record, metadata),
    pickFirstString(record, ["avatar_url", "photo_url", "media_url"]),
    pickFirstString(metadata, ["avatar_url", "photo_url", "media_url"]),
  ]);
  const rawPrimaryEmail = firstNonEmptyString([
    pickFirstString(record, ["primary_email", "email"]),
    pickFirstString(metadata, ["primary_email", "email"]),
  ]);
  const rawPrimaryEmailMetadata =
    pickEmailMetadata(record.primary_email_metadata, metadata.primary_email_metadata);
  const { email: primaryEmail, metadata: primaryEmailMetadata } = scrubCandidateEmail(
    rawPrimaryEmail,
    rawPrimaryEmailMetadata,
  );
  const functionIds = pickFunctionIds(
    metadata.source_shard_filters,
    record.function_ids,
    metadata.function_ids,
  );

  return normalizeCandidateProfileStatus({
    id:
      pickFirstString(record, ["candidate_id", "id"]) ||
      pickFirstString(record, ["candidate_identity_key", "person_identity_key", "profile_url_key"]) ||
      pickFirstString(metadata, ["candidate_id"]) ||
      crypto.randomUUID(),
    candidateIdentityKey: pickFirstString(record, ["candidate_identity_key"]) || pickFirstString(metadata, ["candidate_identity_key"]),
    personIdentityKey: pickFirstString(record, ["person_identity_key"]) || pickFirstString(metadata, ["person_identity_key"]),
    profileUrlKey: pickFirstString(record, ["profile_url_key"]) || pickFirstString(metadata, ["profile_url_key"]),
    name:
      pickFirstString(record, ["display_name", "name_en", "full_name", "name"]) ||
      pickFirstString(metadata, ["display_name"]) ||
      "Unknown Candidate",
    headline:
      pickFirstString(record, ["headline", "role", "title"]) ||
      pickFirstString(metadata, ["headline", "current_role"]) ||
      "Candidate profile",
    avatarUrl,
    team: normalizeTeam(team),
    employmentStatus:
      employmentStatus === "current" || employmentStatus === "former" ? employmentStatus : "lead",
    confidence: toConfidence(pickFirstString(record, ["confidence_label"]) || pickFirstString(metadata, ["confidence_label"])),
    summary:
      pickFirstString(record, ["summary", "explanation", "notes"]) ||
      pickFirstString(metadata, ["summary", "bio", "about", "headline"]) ||
      "Recovered from normalized backend candidate artifacts.",
    rank: typeof record.rank === "number" ? Number(record.rank) : undefined,
    score: typeof record.score === "number" ? Number(record.score) : undefined,
    outreachLayer: parseOutreachLayer(record.outreach_layer, metadata.outreach_layer),
    outreachLayerKey:
      pickFirstString(record, ["outreach_layer_key"]) ||
      pickFirstString(metadata, ["outreach_layer_key"]),
    matchedKeywords,
    sourceMatches,
    currentCompany:
      pickFirstString(record, ["current_company", "organization"]) ||
      pickFirstString(metadata, ["current_company", "organization"]),
    location:
      pickFirstString(record, ["profile_location", "location"]) ||
      pickFirstString(metadata, ["profile_location", "location"]),
    roleBucket:
      pickFirstString(record, ["role_bucket"]) ||
      pickFirstString(metadata, ["role_bucket"]),
    functionIds,
    linkedinUrl:
      pickFirstString(record, ["linkedin_url"]) ||
      pickFirstString(metadata, ["linkedin_url", "profile_url"]),
    sourceDataset:
      normalizeDatasetLabel(
        pickFirstString(record, ["source_dataset"]) || pickFirstString(metadata, ["source_dataset"]),
      ),
    notesSnippet: firstLine(
      pickFirstString(record, ["notes"]) ||
      pickFirstString(metadata, ["notes", "about", "summary"]),
    ),
    primaryEmail,
    primaryEmailMetadata,
    needsProfileCompletion:
      asBoolean(record.needs_profile_completion ?? metadata.needs_profile_completion) || false,
    lowProfileRichness:
      asBoolean(record.low_profile_richness ?? metadata.low_profile_richness) ||
      ((asBoolean(record.has_profile_detail ?? metadata.has_profile_detail) || false) &&
        !(asBoolean(record.needs_profile_completion ?? metadata.needs_profile_completion) || false) &&
        Boolean(
          firstNonEmptyString([
            pickFirstString(record, ["linkedin_url"]),
            pickFirstString(metadata, ["linkedin_url", "profile_url"]),
          ]),
        ) &&
        (structuredExperienceLines.length === 0 || structuredEducationLines.length === 0)),
    hasProfileDetail:
      asBoolean(record.has_profile_detail ?? metadata.has_profile_detail) || false,
    profileCaptureKind:
      pickFirstString(record, ["profile_capture_kind"]) ||
      pickFirstString(metadata, ["profile_capture_kind"]),
    focusAreas,
    matchReasons,
    education:
      structuredEducationLines.length > 0
        ? structuredEducationLines
        : splitStructuredText(record.education ?? metadata.education)
            .map((value) => asString(value))
            .filter(Boolean),
    experience:
      structuredExperienceLines.length > 0
        ? structuredExperienceLines
        : splitStructuredText(record.work_history ?? record.experience ?? metadata.experience)
            .map((value) => asString(value))
            .filter(Boolean),
    evidence: evidenceList,
  });
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
      pickCanonicalMediaSummaryAvatarUrl(profileSummary, materialized, record) ||
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
      asString(profileSummary.profile_location) ||
      asString(profileSummary.location) ||
      pickFirstString(materialized, ["profile_location", "location"]) ||
      base.location,
    roleBucket:
      pickFirstString(materialized, ["role_bucket"]) ||
      asString(profileSummary.role_bucket) ||
      base.roleBucket,
    functionIds: pickFunctionIds(
      (materialized.metadata as Record<string, unknown> | undefined)?.source_shard_filters,
      materialized.function_ids,
      base.functionIds,
    ),
    linkedinUrl,
    sourceDataset:
      normalizeDatasetLabel(pickFirstString(materialized, ["source_dataset"])) ||
      normalizeDatasetLabel(asString(asArray(record.source_datasets)[0])) ||
      base.sourceDataset,
    notesSnippet:
      firstLine(pickFirstString(materialized, ["notes"])) ||
      firstLine(pickFirstString(record, ["manual_review_rationale"])) ||
      base.notesSnippet,
    primaryEmail:
      pickFirstString(materialized, ["primary_email", "email"]) ||
      base.primaryEmail,
    primaryEmailMetadata:
      pickEmailMetadata(
        materialized.primary_email_metadata,
        profileSummary.primary_email_metadata,
        base.primaryEmailMetadata,
      ) || base.primaryEmailMetadata,
    needsProfileCompletion:
      asBoolean(materialized.needs_profile_completion ?? profileSummary.needs_profile_completion) ??
      base.needsProfileCompletion,
    lowProfileRichness:
      asBoolean(materialized.low_profile_richness ?? profileSummary.low_profile_richness) ??
      base.lowProfileRichness,
    hasProfileDetail:
      asBoolean(materialized.has_profile_detail ?? profileSummary.has_profile_detail) ??
      base.hasProfileDetail,
    profileCaptureKind:
      pickFirstString(materialized, ["profile_capture_kind"]) ||
      asString(profileSummary.profile_capture_kind) ||
      base.profileCaptureKind,
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
  const sanitizedEmail = scrubCandidateEmail(enriched.primaryEmail || "", enriched.primaryEmailMetadata);
  return normalizeCandidateProfileStatus(
    mergeExternalLinks(
      {
        ...enriched,
        primaryEmail: sanitizedEmail.email,
        primaryEmailMetadata: sanitizedEmail.metadata,
      },
      contactLinkMap,
      candidateId,
    ),
  );
}

function deriveCandidateFromDocument(
  record: Record<string, unknown>,
  profilePhotoMap: Record<string, string> = {},
  contactLinkMap: Record<string, unknown> = {},
): Candidate {
  const metadata = (record.metadata as Record<string, unknown>) || {};
  const structuredEducationLines = asArray(record.education_lines).map((value) => asString(value)).filter(Boolean);
  const structuredExperienceLines = asArray(record.experience_lines).map((value) => asString(value)).filter(Boolean);
  const matchedFieldRecords = asArray(record.matched_fields ?? metadata.matched_fields).map(
    (item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>),
  );
  const sourceMatches = normalizeSourceMatches(record.source_matches, metadata.source_matches);
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

  const rawPrimaryEmail = firstNonEmptyString([
    pickFirstString(record, ["primary_email", "email"]),
    pickFirstString(metadata, ["primary_email", "email"]),
  ]);
  const rawPrimaryEmailMetadata = pickEmailMetadata(record.primary_email_metadata, metadata.primary_email_metadata);
  const sanitizedEmail = scrubCandidateEmail(rawPrimaryEmail, rawPrimaryEmailMetadata);

  const candidate: Candidate = {
    id: pickFirstString(record, ["candidate_id", "id"]) || crypto.randomUUID(),
    name: pickFirstString(record, ["display_name", "name_en", "full_name", "name"]) || "Unknown Candidate",
    headline: pickFirstString(record, ["headline", "role", "title"]) || "Candidate profile",
    avatarUrl:
      pickCanonicalMediaSummaryAvatarUrl(record, metadata) ||
      pickProfilePhoto(profilePhotoMap, pickFirstString(record, ["candidate_id", "id"])) ||
      pickFirstString(record, ["avatar_url", "photo_url", "media_url"]),
    team: normalizeTeam(pickFirstString(record, ["team", "group_name", "group"])),
    employmentStatus:
      employmentStatus === "current" || employmentStatus === "former" ? employmentStatus : "lead",
    confidence: toConfidence(pickFirstString(record, ["confidence_label"])),
    summary:
      pickFirstString(record, ["summary", "notes", "description"]) ||
      "Recovered from candidate documents.",
    outreachLayer: parseOutreachLayer(record.outreach_layer, metadata.outreach_layer),
    outreachLayerKey:
      pickFirstString(record, ["outreach_layer_key"]) ||
      pickFirstString(metadata, ["outreach_layer_key"]),
    matchedKeywords: canonicalizeDisplayKeywords(
      dedupeStrings([
        ...distinctMatchedKeywords(matchedFieldRecords),
        ...sourceMatchKeywords(sourceMatches),
        ...splitStructuredText(record.matched_keywords),
        ...splitStructuredText(metadata.matched_keywords),
      ]),
    ),
    sourceMatches,
    currentCompany: pickFirstString(record, ["current_company", "organization"]),
    location: pickFirstString(record, ["profile_location", "location"]),
    roleBucket: pickFirstString(record, ["role_bucket"]),
    functionIds: pickFunctionIds(metadata.source_shard_filters, record.function_ids, metadata.function_ids),
    linkedinUrl: resolveLinkedinUrl(pickFirstString(record, ["linkedin_url"]), pickFirstString(record, ["display_name", "name_en", "full_name", "name"])),
    sourceDataset: normalizeDatasetLabel(pickFirstString(record, ["source_dataset"])),
    notesSnippet: firstLine(pickFirstString(record, ["notes"])),
    primaryEmail: sanitizedEmail.email,
    primaryEmailMetadata: sanitizedEmail.metadata,
    needsProfileCompletion:
      asBoolean(record.needs_profile_completion ?? metadata.needs_profile_completion) || false,
    lowProfileRichness:
      asBoolean(record.low_profile_richness ?? metadata.low_profile_richness) ||
      ((asBoolean(record.has_profile_detail ?? metadata.has_profile_detail) || false) &&
        !(asBoolean(record.needs_profile_completion ?? metadata.needs_profile_completion) || false) &&
        Boolean(resolveLinkedinUrl(pickFirstString(record, ["linkedin_url"]), pickFirstString(record, ["display_name", "name_en", "full_name", "name"]))) &&
        (structuredExperienceLines.length === 0 || structuredEducationLines.length === 0)),
    hasProfileDetail:
      asBoolean(record.has_profile_detail ?? metadata.has_profile_detail) || false,
    profileCaptureKind:
      pickFirstString(record, ["profile_capture_kind"]) ||
      pickFirstString(metadata, ["profile_capture_kind"]),
    focusAreas,
    matchReasons,
    education:
      structuredEducationLines.length > 0
        ? structuredEducationLines
        : asArray(record.education)
            .map((value) => asString(value))
            .filter(Boolean),
    experience:
      structuredExperienceLines.length > 0
        ? structuredExperienceLines
        : asArray(record.work_history ?? record.experience)
            .map((value) => asString(value))
            .filter(Boolean),
    evidence: evidenceItems,
  };
  return normalizeCandidateProfileStatus(
    mergeExternalLinks(candidate, contactLinkMap, pickFirstString(record, ["candidate_id", "id"])),
  );
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
  const candidateRecord = (record.candidate as Record<string, unknown>) || {};
  const synthesis = (metadata.manual_review_synthesis as Record<string, unknown>) || {};
  const sourceLinks = asArray(record.source_links ?? metadata.source_links).map((item) => (item as Record<string, unknown>) || {});
  const evidenceRecords = asArray(record.evidence).map((item) => (item as Record<string, unknown>) || {});
  const evidenceLabels = sourceLinks
    .map((item) => firstNonEmptyString([item.label, item.title, item.source_type]))
    .concat(
      evidenceRecords.map((item) => firstNonEmptyString([item.title, item.source_type, item.summary])),
    )
    .filter(Boolean);

  const candidateId =
    pickFirstString(record, ["candidate_id", "id"]) ||
    pickFirstString(candidateRecord, ["candidate_id", "id"]) ||
    pickFirstString(metadata, ["candidate_id"]);
  const candidateName =
    pickFirstString(record, ["display_name", "name_en", "full_name", "name"]) ||
    pickFirstString(candidateRecord, ["display_name", "name_en", "full_name", "name"]) ||
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
    pickFirstString(record, ["recommended_action", "next_action", "review_notes"]) ||
    pickFirstString(metadata, ["recommended_action"]) ||
    "Inspect the attached evidence and decide whether to resolve, reject, or continue research.";
  const rawStatus = pickFirstString(record, ["status"]) || "open";
  const normalizedStatus =
    rawStatus === "resolved" || rawStatus === "dismissed" || rawStatus === "escalated" ? rawStatus : "open";
  let candidate: Candidate | null = null;
  if (Object.keys(candidateRecord).length > 0) {
    try {
      candidate = deriveCandidate(candidateRecord);
    } catch {
      candidate = null;
    }
  }

  return {
    id: pickFirstString(record, ["review_item_id", "id"]) || candidateId || crypto.randomUUID(),
    reviewItemId: Number(record.review_item_id || 0) || undefined,
    candidateId,
    candidateName,
    candidate,
    reviewType,
    status: normalizedStatus,
    summary,
    recommendedAction,
    evidenceLabels: evidenceLabels.length ? evidenceLabels : ["Backlog item"],
    notes: pickFirstString(record, ["review_notes"]),
    synthesisSummary: pickFirstString(synthesis, ["summary"]),
  };
}

function deriveCandidateReviewRecord(record: Record<string, unknown>): CandidateReviewRecord {
  const metadata =
    record.metadata && typeof record.metadata === "object" && !Array.isArray(record.metadata)
      ? (record.metadata as Record<string, unknown>)
      : {};
  const rawStatus = pickFirstString(record, ["status"]) || "needs_review";
  const status: CandidateReviewStatus =
    rawStatus === "no_review_needed" ||
    rawStatus === "needs_profile_completion" ||
    rawStatus === "low_profile_richness" ||
    rawStatus === "verified_keep" ||
    rawStatus === "verified_exclude"
      ? rawStatus
      : "needs_review";
  const rawSource = pickFirstString(record, ["source"]) || "manual_review";
  const source =
    rawSource === "manual_add" || rawSource === "backend_override" ? rawSource : "manual_review";
  const sanitizedEmail = scrubCandidateEmail(
    pickFirstString(record, ["primary_email"]),
    pickEmailMetadata(record.primary_email_metadata, metadata.primary_email_metadata),
  );
  return {
    id: pickFirstString(record, ["id", "record_id"]) || crypto.randomUUID(),
    jobId: pickFirstString(record, ["job_id"]),
    historyId: pickFirstString(record, ["history_id"]),
    candidateId: pickFirstString(record, ["candidate_id"]),
    candidateName: pickFirstString(record, ["candidate_name", "display_name", "name"]),
    headline: pickFirstString(record, ["headline"]),
    currentCompany: pickFirstString(record, ["current_company"]),
    avatarUrl: pickCanonicalMediaSummaryAvatarUrl(record, metadata) || pickFirstString(record, ["avatar_url"]),
    linkedinUrl: pickFirstString(record, ["linkedin_url"]),
    primaryEmail: sanitizedEmail.email,
    primaryEmailMetadata: sanitizedEmail.metadata,
    status,
    comment: pickFirstString(record, ["comment"]),
    source,
    addedAt: pickFirstString(record, ["added_at", "created_at"]) || new Date().toISOString(),
    updatedAt: pickFirstString(record, ["updated_at"]) || new Date().toISOString(),
  };
}

function deriveTargetCandidateRecord(record: Record<string, unknown>): TargetCandidateRecord {
  const metadata =
    record.metadata && typeof record.metadata === "object" && !Array.isArray(record.metadata)
      ? (record.metadata as Record<string, unknown>)
      : {};
  const lastSourceSelection =
    metadata.last_source_selection &&
    typeof metadata.last_source_selection === "object" &&
    !Array.isArray(metadata.last_source_selection)
      ? (metadata.last_source_selection as Record<string, unknown>)
      : {};
  const rawStatus = pickFirstString(record, ["follow_up_status"]) || "pending_outreach";
  const followUpStatus: TargetCandidateFollowUpStatus =
    rawStatus === "contacted_waiting" ||
    rawStatus === "rejected" ||
    rawStatus === "accepted" ||
    rawStatus === "interview_completed"
      ? rawStatus
      : "pending_outreach";
  const rawQualityScore = record.quality_score;
  const qualityScore =
    typeof rawQualityScore === "number"
      ? rawQualityScore
      : typeof rawQualityScore === "string" && rawQualityScore.trim()
        ? Number(rawQualityScore)
        : null;
  const sanitizedEmail = scrubCandidateEmail(
    pickFirstString(record, ["primary_email"]),
    pickEmailMetadata(record.primary_email_metadata, metadata.primary_email_metadata),
  );
  return {
    id: pickFirstString(record, ["id", "record_id"]) || crypto.randomUUID(),
    workspaceId: pickFirstString(record, ["workspace_id", "workspaceId"]),
    candidateId: pickFirstString(record, ["candidate_id"]),
    candidateIdentityKey:
      pickFirstString(lastSourceSelection, ["candidate_identity_key"]) ||
      pickFirstString(record, ["candidate_identity_key"]),
    personIdentityKey: pickFirstString(record, ["person_identity_key"]),
    sourceProjectionId:
      pickFirstString(lastSourceSelection, ["projection_id"]) ||
      pickFirstString(record, ["source_projection_id"]),
    sourceMembershipRevision:
      pickFirstString(lastSourceSelection, ["membership_revision"]) ||
      pickFirstString(metadata, ["last_source_membership_revision", "source_membership_revision"]),
    sourceRunId: pickFirstString(record, ["source_run_id"]),
    sourceCollectionId: pickFirstString(record, ["source_collection_id"]),
    historyId: pickFirstString(record, ["history_id"]),
    jobId: pickFirstString(record, ["job_id"]),
    candidateName: pickFirstString(record, ["candidate_name", "display_name", "name"]),
    headline: pickFirstString(record, ["headline"]),
    currentCompany: pickFirstString(record, ["current_company"]),
    avatarUrl: pickCanonicalMediaSummaryAvatarUrl(record, metadata) || pickFirstString(record, ["avatar_url"]),
    linkedinUrl: pickFirstString(record, ["linkedin_url"]),
    primaryEmail: sanitizedEmail.email,
    primaryEmailMetadata: sanitizedEmail.metadata,
    followUpStatus,
    qualityScore: Number.isFinite(qualityScore ?? NaN) ? qualityScore : null,
    comment: pickFirstString(record, ["comment"]),
    addedAt: pickFirstString(record, ["added_at", "created_at"]) || new Date().toISOString(),
    updatedAt: pickFirstString(record, ["updated_at"]) || new Date().toISOString(),
  };
}

function deriveTargetCandidatePublicWebSearchState(record: Record<string, unknown>): TargetCandidatePublicWebSearchState {
  return {
    status: pickFirstString(record, ["status"]) || "ok",
    batches: asArray(record.batches)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebBatch(item)),
    runs: asArray(record.runs)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebRun(item)),
    phaseCommandsByRunId: asRecord(record.phase_commands_by_run_id),
  };
}

function deriveTargetCandidatePublicWebActionResult(record: Record<string, unknown>): TargetCandidatePublicWebActionResult {
  const state = deriveTargetCandidatePublicWebSearchState(record);
  const batch =
    record.batch && typeof record.batch === "object" && !Array.isArray(record.batch)
      ? deriveTargetCandidatePublicWebBatch(record.batch as Record<string, unknown>)
      : state.batches[0] || null;
  return {
    ...state,
    batch,
    summary:
      record.summary && typeof record.summary === "object" && !Array.isArray(record.summary)
        ? (record.summary as Record<string, unknown>)
        : {},
    workerSummary:
      record.worker_summary && typeof record.worker_summary === "object" && !Array.isArray(record.worker_summary)
        ? (record.worker_summary as Record<string, unknown>)
        : {},
    job:
      record.job && typeof record.job === "object" && !Array.isArray(record.job)
        ? (record.job as Record<string, unknown>)
        : {},
    reason: pickFirstString(record, ["reason"]),
  };
}

function deriveTargetCandidatePublicWebBatch(record: Record<string, unknown>): TargetCandidatePublicWebBatch {
  return {
    batchId: pickFirstString(record, ["batch_id", "batchId"]),
    workspaceId: pickFirstString(record, ["workspace_id", "workspaceId"]),
    status: normalizeTargetCandidatePublicWebStatus(pickFirstString(record, ["status"])),
    requestedRecordIds: asArray(record.requested_record_ids).map((item) => asString(item)).filter(Boolean),
    runIds: asArray(record.run_ids).map((item) => asString(item)).filter(Boolean),
    sourceFamilies: asArray(record.source_families).map((item) => asString(item)).filter(Boolean),
    summary:
      record.summary && typeof record.summary === "object" && !Array.isArray(record.summary)
        ? (record.summary as Record<string, unknown>)
        : {},
    createdAt: pickFirstString(record, ["created_at"]) || "",
    updatedAt: pickFirstString(record, ["updated_at"]) || "",
  };
}

function deriveTargetCandidatePublicWebRun(record: Record<string, unknown>): TargetCandidatePublicWebRun {
  const queryManifest = asArray(record.query_manifest)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .filter((item) => Object.keys(item).length > 0);
  return {
    runId: pickFirstString(record, ["run_id", "runId"]),
    workspaceId: pickFirstString(record, ["workspace_id", "workspaceId"]),
    batchId: pickFirstString(record, ["batch_id", "batchId"]),
    recordId: pickFirstString(record, ["record_id", "recordId"]),
    candidateId: pickFirstString(record, ["candidate_id", "candidateId"]),
    candidateName: pickFirstString(record, ["candidate_name", "candidateName"]),
    currentCompany: pickFirstString(record, ["current_company", "currentCompany"]),
    linkedinUrl: pickFirstString(record, ["linkedin_url", "linkedinUrl"]),
    status: normalizeTargetCandidatePublicWebStatus(pickFirstString(record, ["status"])),
    phase: pickFirstString(record, ["phase"]),
    sourceFamilies: asArray(record.source_families).map((item) => asString(item)).filter(Boolean),
    summary:
      record.summary && typeof record.summary === "object" && !Array.isArray(record.summary)
        ? (record.summary as Record<string, unknown>)
        : {},
    queryManifest,
    searchCheckpoint:
      record.search_checkpoint && typeof record.search_checkpoint === "object" && !Array.isArray(record.search_checkpoint)
        ? (record.search_checkpoint as Record<string, unknown>)
        : {},
    analysisCheckpoint:
      record.analysis_checkpoint && typeof record.analysis_checkpoint === "object" && !Array.isArray(record.analysis_checkpoint)
        ? (record.analysis_checkpoint as Record<string, unknown>)
        : {},
    phaseCommands: asRecord(record.phase_commands),
    phaseCommandDisplayLine: pickFirstString(record, ["phase_command_display_line", "phaseCommandDisplayLine"]),
    runControlState: asRecord(record.run_control_state || record.runControlState),
    runDisplayContract: asRecord(record.run_display_contract || record.runDisplayContract),
    artifactRoot: pickFirstString(record, ["artifact_root", "artifactRoot"]),
    lastError: pickFirstString(record, ["last_error", "lastError"]),
    createdAt: pickFirstString(record, ["created_at", "createdAt"]),
    startedAt: pickFirstString(record, ["started_at", "startedAt"]),
    completedAt: pickFirstString(record, ["completed_at", "completedAt"]),
    updatedAt: pickFirstString(record, ["updated_at", "updatedAt"]),
  };
}

function deriveTargetCandidatePublicWebDetail(record: Record<string, unknown>): TargetCandidatePublicWebDetail {
  return {
    status: pickFirstString(record, ["status"]) || "ok",
    recordId: pickFirstString(record, ["record_id", "recordId"]),
    targetCandidate:
      record.target_candidate && typeof record.target_candidate === "object" && !Array.isArray(record.target_candidate)
        ? (record.target_candidate as Record<string, unknown>)
        : null,
    latestRun:
      record.latest_run && typeof record.latest_run === "object" && !Array.isArray(record.latest_run)
        ? (record.latest_run as Record<string, unknown>)
        : null,
    phaseCommands: asRecord(record.phase_commands),
    personAsset:
      record.person_asset && typeof record.person_asset === "object" && !Array.isArray(record.person_asset)
        ? (record.person_asset as Record<string, unknown>)
        : null,
    signals: asArray(record.signals)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebSignal(item)),
    emailCandidates: asArray(record.email_candidates)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebSignal(item)),
    profileLinks: asArray(record.profile_links)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebSignal(item)),
    groupedSignals:
      record.grouped_signals && typeof record.grouped_signals === "object" && !Array.isArray(record.grouped_signals)
        ? (record.grouped_signals as Record<string, unknown>)
        : {},
    evidenceLinks: asArray(record.evidence_links)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebEvidenceLink(item)),
    promotions: asArray(record.promotions)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebPromotion(item)),
    promotionSummary:
      record.promotion_summary && typeof record.promotion_summary === "object" && !Array.isArray(record.promotion_summary)
        ? (record.promotion_summary as Record<string, unknown>)
        : {},
    rawAssetPolicy:
      record.raw_asset_policy && typeof record.raw_asset_policy === "object" && !Array.isArray(record.raw_asset_policy)
        ? (record.raw_asset_policy as Record<string, unknown>)
        : {},
  };
}

function deriveTargetCandidateProfileDetail(record: Record<string, unknown>): TargetCandidateProfileDetail {
  const profile =
    record.profile && typeof record.profile === "object" && !Array.isArray(record.profile)
      ? deriveTargetCandidateComposedProfile(record.profile as Record<string, unknown>)
      : null;
  return {
    status: pickFirstString(record, ["status"]) || "ok",
    recordId: pickFirstString(record, ["record_id", "recordId"]),
    targetCandidate:
      record.target_candidate && typeof record.target_candidate === "object" && !Array.isArray(record.target_candidate)
        ? (record.target_candidate as Record<string, unknown>)
        : null,
    profile,
    publicWebDetail:
      record.public_web_detail && typeof record.public_web_detail === "object" && !Array.isArray(record.public_web_detail)
        ? deriveTargetCandidatePublicWebDetail(record.public_web_detail as Record<string, unknown>)
        : null,
    rawAssetPolicy:
      record.raw_asset_policy && typeof record.raw_asset_policy === "object" && !Array.isArray(record.raw_asset_policy)
        ? (record.raw_asset_policy as Record<string, unknown>)
        : {},
  };
}

function deriveTargetCandidateComposedProfile(record: Record<string, unknown>): TargetCandidateComposedProfile {
  const rawAssetPolicy =
    record.raw_asset_policy && typeof record.raw_asset_policy === "object" && !Array.isArray(record.raw_asset_policy)
      ? (record.raw_asset_policy as Record<string, unknown>)
      : {};
  return {
    schemaVersion: Number(record.schema_version || record.schemaVersion || 1),
    identity:
      record.identity && typeof record.identity === "object" && !Array.isArray(record.identity)
        ? (record.identity as Record<string, unknown>)
        : {},
    contact:
      record.contact && typeof record.contact === "object" && !Array.isArray(record.contact)
        ? (record.contact as TargetCandidateComposedProfile["contact"])
        : {},
    public_web:
      record.public_web && typeof record.public_web === "object" && !Array.isArray(record.public_web)
        ? (record.public_web as TargetCandidateComposedProfile["public_web"])
        : {},
    review:
      record.review && typeof record.review === "object" && !Array.isArray(record.review)
        ? (record.review as TargetCandidateComposedProfile["review"])
        : {},
    export_readiness:
      record.export_readiness && typeof record.export_readiness === "object" && !Array.isArray(record.export_readiness)
        ? (record.export_readiness as TargetCandidateComposedProfile["export_readiness"])
        : {},
    completeness:
      record.completeness && typeof record.completeness === "object" && !Array.isArray(record.completeness)
        ? (record.completeness as TargetCandidateComposedProfile["completeness"])
        : {},
    evidence_sources: asArray(record.evidence_sources)
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .filter((item) => Object.keys(item).length > 0)
      .map((item) => deriveTargetCandidatePublicWebEvidenceLink(item)),
    raw_asset_policy: rawAssetPolicy,
  };
}

function deriveTargetCandidatePublicWebSignal(record: Record<string, unknown>): TargetCandidatePublicWebSignal {
  return {
    signalId: pickFirstString(record, ["signal_id", "signalId"]),
    runId: pickFirstString(record, ["run_id", "runId"]),
    signalKind: pickFirstString(record, ["signal_kind", "signalKind"]),
    signalType: pickFirstString(record, ["signal_type", "signalType"]),
    emailType: pickFirstString(record, ["email_type", "emailType"]),
    value: pickFirstString(record, ["value"]),
    normalizedValue: pickFirstString(record, ["normalized_value", "normalizedValue"]),
    url: pickFirstString(record, ["url"]),
    sourceUrl: pickFirstString(record, ["source_url", "sourceUrl"]),
    sourceDomain: pickFirstString(record, ["source_domain", "sourceDomain"]),
    sourceFamily: pickFirstString(record, ["source_family", "sourceFamily"]),
    sourceTitle: pickFirstString(record, ["source_title", "sourceTitle"]),
    confidenceLabel: pickFirstString(record, ["confidence_label", "confidenceLabel"]),
    confidenceScore: asNumber(record.confidence_score ?? record.confidenceScore),
    identityMatchLabel: pickFirstString(record, ["identity_match_label", "identityMatchLabel"]),
    identityMatchScore: asNumber(record.identity_match_score ?? record.identityMatchScore),
    publishable: asBoolean(record.publishable) ?? false,
    promotionStatus: pickFirstString(record, ["promotion_status", "promotionStatus"]),
    promotionId: pickFirstString(record, ["promotion_id", "promotionId"]) || undefined,
    promotionAction: pickFirstString(record, ["promotion_action", "promotionAction"]) || undefined,
    promotedField: pickFirstString(record, ["promoted_field", "promotedField"]) || undefined,
    promotedValue: pickFirstString(record, ["promoted_value", "promotedValue"]) || undefined,
    previousValue: pickFirstString(record, ["previous_value", "previousValue"]) || undefined,
    promotedBy: pickFirstString(record, ["promoted_by", "promotedBy"]) || undefined,
    promotedAt: pickFirstString(record, ["promoted_at", "promotedAt"]) || undefined,
    promotionNote: pickFirstString(record, ["promotion_note", "promotionNote"]) || undefined,
    promotionOverrideReason:
      pickFirstString(record, ["promotion_override_reason", "promotionOverrideReason"]) || undefined,
    promotionOverrideValidationReason:
      pickFirstString(record, ["promotion_override_validation_reason", "promotionOverrideValidationReason"]) || undefined,
    promotionRequiresManualOverride: asBoolean(
      record.promotion_requires_manual_override ?? record.promotionRequiresManualOverride,
    ),
    suppressionReason: pickFirstString(record, ["suppression_reason", "suppressionReason"]),
    evidenceExcerpt: pickFirstString(record, ["evidence_excerpt", "evidenceExcerpt"]),
    linkShapeWarnings: asArray(record.link_shape_warnings ?? record.linkShapeWarnings)
      .map((item) => asString(item))
      .filter(Boolean),
    cleanProfileLink: asBoolean(record.clean_profile_link ?? record.cleanProfileLink) ?? true,
    artifactRefs:
      record.artifact_refs && typeof record.artifact_refs === "object" && !Array.isArray(record.artifact_refs)
        ? (record.artifact_refs as Record<string, unknown>)
        : {},
    metadata:
      record.metadata && typeof record.metadata === "object" && !Array.isArray(record.metadata)
        ? (record.metadata as Record<string, unknown>)
        : {},
    createdAt: pickFirstString(record, ["created_at", "createdAt"]),
    updatedAt: pickFirstString(record, ["updated_at", "updatedAt"]),
  };
}

function deriveTargetCandidatePublicWebPromotion(record: Record<string, unknown>): TargetCandidatePublicWebPromotion {
  return {
    promotionId: pickFirstString(record, ["promotion_id", "promotionId"]),
    signalId: pickFirstString(record, ["signal_id", "signalId"]),
    runId: pickFirstString(record, ["run_id", "runId"]),
    recordId: pickFirstString(record, ["record_id", "recordId"]),
    signalKind: pickFirstString(record, ["signal_kind", "signalKind"]),
    signalType: pickFirstString(record, ["signal_type", "signalType"]),
    emailType: pickFirstString(record, ["email_type", "emailType"]),
    value: pickFirstString(record, ["value"]),
    normalizedValue: pickFirstString(record, ["normalized_value", "normalizedValue"]),
    url: pickFirstString(record, ["url"]),
    newValue: pickFirstString(record, ["new_value", "newValue"]),
    previousValue: pickFirstString(record, ["previous_value", "previousValue"]),
    sourceUrl: pickFirstString(record, ["source_url", "sourceUrl"]),
    sourceDomain: pickFirstString(record, ["source_domain", "sourceDomain"]),
    sourceFamily: pickFirstString(record, ["source_family", "sourceFamily"]),
    sourceTitle: pickFirstString(record, ["source_title", "sourceTitle"]),
    confidenceLabel: pickFirstString(record, ["confidence_label", "confidenceLabel"]),
    confidenceScore: asNumber(record.confidence_score ?? record.confidenceScore),
    identityMatchLabel: pickFirstString(record, ["identity_match_label", "identityMatchLabel"]),
    identityMatchScore: asNumber(record.identity_match_score ?? record.identityMatchScore),
    publishable: asBoolean(record.publishable) ?? false,
    cleanProfileLink: asBoolean(record.clean_profile_link ?? record.cleanProfileLink) ?? true,
    linkShapeWarnings: asArray(record.link_shape_warnings ?? record.linkShapeWarnings)
      .map((item) => asString(item))
      .filter(Boolean),
    action: pickFirstString(record, ["action"]),
    promotionStatus: pickFirstString(record, ["promotion_status", "promotionStatus"]),
    operator: pickFirstString(record, ["operator"]),
    note: pickFirstString(record, ["note"]),
    overrideReason: pickFirstString(record, ["override_reason", "overrideReason"]),
    overrideValidationReason: pickFirstString(record, ["override_validation_reason", "overrideValidationReason"]),
    requiresManualOverride: asBoolean(record.requires_manual_override ?? record.requiresManualOverride) ?? false,
    evidenceExcerpt: pickFirstString(record, ["evidence_excerpt", "evidenceExcerpt"]),
    metadata:
      record.metadata && typeof record.metadata === "object" && !Array.isArray(record.metadata)
        ? (record.metadata as Record<string, unknown>)
        : {},
    createdAt: pickFirstString(record, ["created_at", "createdAt"]),
    updatedAt: pickFirstString(record, ["updated_at", "updatedAt"]),
  };
}

function deriveTargetCandidatePublicWebEvidenceLink(
  record: Record<string, unknown>,
): TargetCandidatePublicWebEvidenceLink {
  return {
    sourceUrl: pickFirstString(record, ["source_url", "sourceUrl"]),
    sourceDomain: pickFirstString(record, ["source_domain", "sourceDomain"]),
    sourceFamily: pickFirstString(record, ["source_family", "sourceFamily"]),
    sourceTitle: pickFirstString(record, ["source_title", "sourceTitle"]),
    signalIds: asArray(record.signal_ids ?? record.signalIds).map((item) => asString(item)).filter(Boolean),
    signalKinds: asArray(record.signal_kinds ?? record.signalKinds).map((item) => asString(item)).filter(Boolean),
    signalTypes: asArray(record.signal_types ?? record.signalTypes).map((item) => asString(item)).filter(Boolean),
    identityMatchLabels: asArray(record.identity_match_labels ?? record.identityMatchLabels)
      .map((item) => asString(item))
      .filter(Boolean),
    maxConfidenceScore: asNumber(record.max_confidence_score ?? record.maxConfidenceScore),
  };
}

function normalizeTargetCandidatePublicWebStatus(value: string): TargetCandidatePublicWebStatus {
  const normalized = value.trim().toLowerCase();
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

    const groups = ["All", ...new Set(candidates.map((candidate) => normalizeTeam(candidate.team)).filter(Boolean))];
    const registryAssets = assetRegistry["assets"];
    const assetEntries = Array.isArray(registryAssets)
      ? registryAssets.length
      : Array.isArray((assetRegistry as { entries?: unknown[] }).entries)
        ? ((assetRegistry as { entries?: unknown[] }).entries || []).length
        : 0;

    return {
      title: "Local Company Asset View",
      snapshotId: indexPayload.snapshotId,
      queryLabel: "Recovered local company artifacts for frontend demo and testing",
      targetCompany: "",
      intentKeywords: [],
      resultMode: "asset_population",
      resultModeLabel: "公司级资产视图",
      rankedCandidateCount: candidates.length,
      assetPopulationCount: candidates.length,
      totalCandidates: candidates.length,
      totalEvidence: assetEntries,
      manualReviewCount: manualReview.length,
      layers: buildLayeredSegmentationOptions(candidates),
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
      const canonicalAvatarUrl = pickCanonicalMediaSummaryAvatarUrl(candidateRecord, materializedRecord || {});
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
        avatarUrl: canonicalAvatarUrl || profilePhoto || enrichedBase.avatarUrl,
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

    const candidateDocuments = extractCandidateArray(candidateDocumentsPayload);
    const candidateSource = normalizedCandidates.length > 0 ? normalizedCandidates : candidateDocuments;
    const candidates = candidateSource
      .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
      .map((item) => (normalizedCandidates.length > 0 ? deriveCandidate(item) : deriveCandidateFromDocument(item)));

    const currentCount = candidates.filter((candidate) => candidate.employmentStatus === "current").length;
    const formerCount = candidates.filter((candidate) => candidate.employmentStatus === "former").length;

    const sourceLabel =
      indexPayload.source === "object_storage" ? "R2 asset pull" : "local runtime import";
    const bundleLabel = indexPayload.bundleId ? `Bundle ${indexPayload.bundleId}` : "Imported artifact set";

    return {
      jobId: indexPayload.bundleId || `tml_${indexPayload.snapshotId}`,
      status: "completed",
      currentStage: "Asset Recovery Ready",
      startedAt: indexPayload.snapshotId,
      metrics: [
        { label: "总候选人数量", value: String(candidates.length) },
        { label: "需人工审核候选人", value: String(manualReview.length) },
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
