import { mockCandidateDetails, mockDashboard, mockManualReviewItems, mockPlan, mockRunStatus } from "../data/mockData";
import type {
  Candidate,
  CandidateDetail,
  CandidateConfidence,
  CandidateEmailMetadata,
  CandidateExternalLink,
  CandidateReviewRecord,
  CandidateReviewStatus,
  DashboardData,
  DemoPlan,
  ExcelIntakeResponse,
  ManualReviewItem,
  PlanReviewDecision,
  PlanReviewEditableField,
  PlanReviewGate,
  RunStatusData,
  SupplementOperationResult,
  TargetCompanyIdentityPreview,
  TargetCandidateFollowUpStatus,
  TargetCandidatePublicWebBatch,
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
const PROFILE_COMPLETION_TIMEOUT_MS = 180_000;
const DASHBOARD_INITIAL_CANDIDATE_CHUNK_SIZE = 96;
const DASHBOARD_BACKGROUND_CANDIDATE_CHUNK_SIZE = 160;
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
  hasMore: boolean;
  nextOffset: number | null;
  candidates: Candidate[];
}

export function dashboardHasRenderableCandidates(dashboard: DashboardData | null | undefined): boolean {
  return Boolean(dashboard && Math.max(dashboard.totalCandidates || 0, dashboard.candidates.length) > 0);
}

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

function dashboardCandidatePageCacheKey(jobId: string, offset: number, limit: number): string {
  return `${jobId}::${offset}::${limit}`;
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
): Promise<{ blob: Blob; filename: string; contentType: string }> {
  const apiBaseUrl = getConfiguredApiBaseUrl();
  if (!apiBaseUrl) {
    throw new Error(
      "API base URL is not configured. Please set a backend URL or enable same-origin API proxy mode before starting a search.",
    );
  }
  const fetchFromApiBaseUrl = async (
    resolvedApiBaseUrl: string,
  ): Promise<{ blob: Blob; filename: string; contentType: string }> => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    const requestHeaders = new Headers(options?.headers || {});
    if (options?.body && !requestHeaders.has("Content-Type") && !isFormDataBody(options.body)) {
      requestHeaders.set("Content-Type", "application/json");
    }
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
  if (
    targetScope === "full_company_asset" ||
    effectiveAcquisitionMode === "full_local_asset_reuse" ||
    defaultResultsMode === "asset_population"
  ) {
    return "目标公司全量范围";
  }
  if (strategyType === "scoped_search_roster") {
    return "目标公司定向搜索范围";
  }
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
]);

function filterDisplayIntentKeywords(values: string[]): string[] {
  const deduped = dedupeStrings(values);
  const specific = deduped.filter((value) => !GENERIC_RECALL_KEYWORD_TOKENS.has(normalizeKeywordToken(value)));
  return specific.length > 0 ? specific : deduped;
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

function buildLayeredSegmentationOptions(candidates: Candidate[]): DashboardData["layers"] {
  return [0, 1, 2, 3].map((layer) => ({
    id: `layer_${layer}`,
    label: `Layer ${layer}`,
    count: candidates.filter((candidate) => candidate.outreachLayer === layer).length,
  }));
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
  const planKeywords = explainKeywords(explain);
  const planStrategyType = pickFirstString(acquisitionStrategy, ["strategy_type"]);
  const acquisitionMode = pickFirstString(organizationExecutionProfile, ["default_acquisition_mode"]);
  const effectiveAcquisitionMode = pickFirstString(effectiveExecutionSemantics, ["effective_acquisition_mode"]);
  const backendExecutionStrategyLabel = pickFirstString(effectiveExecutionSemantics, [
    "execution_strategy_label",
    "strategy_label",
  ]);
  const dispatchStrategy = pickFirstString(dispatchPreview, ["strategy"]);
  const currentLaneBehavior = pickFirstString(currentLane, ["planned_behavior"]);
  const formerLaneBehavior = pickFirstString(formerLane, ["planned_behavior"]);
  const reviewGate = mapPlanReviewGate(payload, requestPreview);
  const targetCompanyIdentity = mapTargetCompanyIdentity(requestPreview.target_company_identity);
  const plannerMode = pickFirstString(assetReusePlan, ["planner_mode"]);
  const baselinePopulationDefaultReuse = Boolean(
    (assetReusePlan as Record<string, unknown>).baseline_population_default_reuse_sufficient
  );
  const fullLocalReuseResolved =
    effectiveAcquisitionMode === "full_local_asset_reuse" ||
    ((dispatchStrategy === "reuse_snapshot" || plannerMode === "reuse_snapshot_only")
      && Boolean(assetReusePlan.baseline_reuse_available)
      && !Boolean(assetReusePlan.requires_delta_acquisition)
      && (
        baselinePopulationDefaultReuse
        || pickFirstString(organizationExecutionProfile, ["current_lane_default"]) === "reuse_baseline"
      ));
  const resolvedAcquisitionStrategy =
    backendExecutionStrategyLabel
      ? backendExecutionStrategyLabel
      : fullLocalReuseResolved
      ? "全量本地资产复用"
      : planStrategyType === "scoped_search_roster"
      ? (
        dispatchStrategy === "delta_from_snapshot" || Boolean(assetReusePlan.requires_delta_acquisition)
          ? "Scoped search + Baseline 复用增量"
          : dispatchStrategy === "reuse_snapshot"
            ? "Scoped search（baseline 已覆盖）"
            : translateAcquisitionModeLabel(planStrategyType)
      )
      : effectiveAcquisitionMode
      ? translateEffectiveAcquisitionModeLabel(effectiveAcquisitionMode) || translateAcquisitionModeLabel(acquisitionMode)
      : dispatchStrategy === "reuse_snapshot"
        ? "全量本地资产复用"
        : dispatchStrategy === "delta_from_snapshot"
          ? "Baseline 复用 + 缺口增量"
          : acquisitionMode
            ? translateAcquisitionModeLabel(acquisitionMode)
            : inferStrategyLabel(queryText, payload);
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
    reviewGate,
    reviewDecisionDefaults: mapPlanReviewDecisionDefaults(payload, requestPreview, reviewGate),
  };
}

export async function getPlan(queryText = ""): Promise<DemoPlan> {
  const envelope = await getPlanEnvelope(queryText);
  return envelope.plan;
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

export async function getPlanEnvelope(
  queryText: string,
  historyId = "",
): Promise<{ plan: DemoPlan; reviewId: string; raw: any; explain: any }> {
  const payload = await fetchJson<any>("/api/plan", {
    method: "POST",
    body: JSON.stringify({
      raw_user_request: queryText,
      history_id: historyId || undefined,
      planning_mode: "model_assisted",
      ...DEFAULT_RECALL_LIMITS,
    }),
  }, PLAN_API_TIMEOUT_MS);
  return {
    plan: mapPlanPayloadToDemoPlan(payload, queryText, payload),
    reviewId: String(payload.plan_review_session?.review_id || ""),
    raw: payload,
    explain: payload,
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
  const rawOverallStatus = pickFirstString(payload, ["status"]) || "running";
  const overallStatus: RunStatusData["status"] =
    rawOverallStatus === "completed" ||
    rawOverallStatus === "running" ||
    rawOverallStatus === "queued" ||
    rawOverallStatus === "blocked" ||
    rawOverallStatus === "failed"
      ? rawOverallStatus
      : "running";
  const completedAtFallback = normalizeBackendProgressTimestamp(pickFirstString(payload, ["updated_at"]));

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

  const stageSummaryRoot = (payload.workflow_stage_summaries as Record<string, unknown>) || {};
  const stageSummaryMap = ((stageSummaryRoot.summaries as Record<string, unknown>) || {}) as Record<string, Record<string, unknown>>;
  const canonicalWorkflowStages = [
    {
      id: "linkedin_stage_1",
      title: pickFirstString(stageSummaryMap.linkedin_stage_1 || {}, ["title"]) || "LinkedIn Stage 1",
    },
    {
      id: "stage_1_preview",
      title: pickFirstString(stageSummaryMap.stage_1_preview || {}, ["title"]) || "Stage 1 Preview",
    },
    {
      id: "public_web_stage_2",
      title: pickFirstString(stageSummaryMap.public_web_stage_2 || {}, ["title"]) || "Public Web Stage 2",
    },
    {
      id: "stage_2_final",
      title: pickFirstString(stageSummaryMap.stage_2_final || {}, ["title"]) || "Final Results",
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
  const candidateCountMetric =
    asNumber(payload.progress?.counters?.candidate_count) ??
    asNumber(payload.progress?.counters?.results_count) ??
    asNumber(stage2FinalCandidateSource.candidate_count) ??
    asNumber(stage1PreviewCandidateSource.candidate_count) ??
    asNumber(retrievingCandidateSource.candidate_count) ??
    asNumber(acquiringSync.candidate_count) ??
    0;
  const evidenceCountMetric =
    asNumber(payload.progress?.counters?.evidence_count) ??
    asNumber(acquiringSync.evidence_count) ??
    0;
  const manualReviewCountMetric =
    asNumber(payload.progress?.counters?.manual_review_count) ??
    asNumber(stage2FinalSummary.manual_review_queue_count) ??
    asNumber(stage1PreviewSummary.manual_review_queue_count) ??
    0;

  const summaryTime = (stageId: string, ...fieldNames: string[]): string =>
    normalizeStageSummaryTimestamp(pickFirstString(stageSummaryMap[stageId] || {}, fieldNames));
  const backendTime = (value: string): string => normalizeBackendProgressTimestamp(value);

  const currentCanonicalStageId = (): string => {
    if (overallStatus === "completed") {
      return "stage_2_final";
    }
    if (overallStatus === "failed") {
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
    if (stageId === "linkedin_stage_1") {
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
      const manualReviewCount = Number(summary.manual_review_queue_count || 0);
      if (returnedMatches > 0) {
        return `Stage 1 preview 已生成，当前返回 ${returnedMatches} 位候选人，待审核 ${manualReviewCount} 条。`;
      }
      return pickFirstString(summary, ["text"]) || "正在根据 LinkedIn Stage 1 数据生成 preview。";
    }
    if (stageId === "public_web_stage_2") {
      return pickFirstString(summary, ["text"]) || "正在补充公开网页、论文与外部证据。";
    }
    if (stageId === "stage_2_final") {
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
          if (
            explicitStatus === "completed" ||
            explicitStatus === "running" ||
            explicitStatus === "queued" ||
            explicitStatus === "blocked" ||
            explicitStatus === "failed"
          ) {
            status = normalizeEventStatus(explicitStatus, stage.id);
          } else if (overallStatus === "completed" && stage.id === "stage_2_final") {
            status = "completed";
          } else if (index < currentCanonicalStageIndex) {
            status = "completed";
          } else if (index === currentCanonicalStageIndex) {
            status = overallStatus === "queued" ? "queued" : overallStatus === "failed" ? "failed" : "running";
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
      canonicalWorkflowStages.find((stage) => stage.id === currentCanonicalStage)?.title ||
      payload.stage ||
      payload.progress?.current_stage ||
      "Workflow",
    startedAt: backendTime(payload.started_at || payload.updated_at || "") || "unknown",
    currentMessage: pickFirstString(payload, ["current_message"]),
    awaitingUserAction: pickFirstString(payload, ["awaiting_user_action"]),
    metrics: [
      { label: "Candidates", value: String(candidateCountMetric) },
      { label: "Evidence", value: String(evidenceCountMetric) },
      { label: "Manual Review", value: String(manualReviewCountMetric) },
      {
        label: "Workers",
        value: String(
          Object.values((workerSummary.by_status as Record<string, number>) || {}).reduce(
            (sum, value) => sum + Number(value || 0),
            0,
          ),
        ),
      },
    ],
    timeline:
      summarizedTimeline.length > 0
        ? summarizedTimeline
        : progressEvents.length > 0
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
            const status = normalizeEventStatus(pickFirstString(milestone, ["status"]) || "running", stage);
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
        currentMessage: fallbackPayload.summary?.message || "",
        awaitingUserAction: fallbackPayload.summary?.awaiting_user_action || "",
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
  const rankedResultRecords = asArray(payload.results)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .filter((item) => Object.keys(item).length > 0);
  const assetPopulationPayload = (payload.asset_population as Record<string, unknown>) || {};
  const assetPopulationRecords = asArray(assetPopulationPayload.candidates)
    .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
    .filter((item) => Object.keys(item).length > 0);
  const effectiveExecutionSemantics = (payload.effective_execution_semantics as Record<string, unknown>) || {};
  const rankedResultCount =
    typeof payload.ranked_result_count === "number"
      ? Number(payload.ranked_result_count || 0)
      : rankedResultRecords.length;
  const assetPopulationCount =
    Number(assetPopulationPayload.candidate_count || 0) || assetPopulationRecords.length;
  const preferredResultsMode = pickFirstString(effectiveExecutionSemantics, ["default_results_mode"]);
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
  const manualReviewCount =
    typeof payload.manual_review_count === "number"
      ? Number(payload.manual_review_count || 0)
      : asArray(payload.manual_review_items).length;
  return {
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
    assetPopulationCount,
    totalCandidates: useAssetPopulation ? assetPopulationCount : rankedResultCount,
    totalEvidence: activeRecords.reduce((sum, record) => sum + asArray(record.evidence).length, 0),
    manualReviewCount,
    layers: buildLayeredSegmentationOptions(candidates),
    groups,
    candidates,
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

export function mergeDashboardCandidatePage(
  dashboard: DashboardData,
  page: DashboardCandidatePage,
): DashboardData {
  const mergedCandidates = mergeDashboardCandidatesInPageOrder(dashboard.candidates, page.candidates, {
    offset: page.offset,
  });
  const totalCandidates = Math.max(page.totalCandidates, mergedCandidates.length, dashboard.totalCandidates);
  return {
    ...dashboard,
    resultMode: page.resultMode,
    resultModeLabel: page.resultMode === "asset_population" ? "公司级资产视图" : "检索排序结果",
    rankedCandidateCount:
      page.resultMode === "ranked_results"
        ? Math.max(page.totalCandidates, dashboard.rankedCandidateCount)
        : dashboard.rankedCandidateCount,
    assetPopulationCount:
      page.resultMode === "asset_population"
        ? Math.max(page.totalCandidates, dashboard.assetPopulationCount)
        : dashboard.assetPopulationCount,
    totalCandidates,
    layers: buildLayeredSegmentationOptions(mergedCandidates),
    groups: Array.from(new Set(["All", ...mergedCandidates.map((candidate) => candidate.team || "Unknown")])),
    candidates: mergedCandidates,
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
  const cached = forceRefresh ? null : readFreshCacheValue(dashboardCache, cacheKey, DASHBOARD_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  if (forceRefresh) {
    dashboardPromiseCache.delete(cacheKey);
  }
  let fetchPromise = dashboardPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(`/api/jobs/${jobId}/dashboard`, undefined, RESULTS_API_TIMEOUT_MS)
      .catch((error) => {
        const message = error instanceof Error ? error.message : "";
        if (message.includes("Request failed: 404")) {
          return fetchJson<any>(`/api/jobs/${jobId}/results`, undefined, RESULTS_API_TIMEOUT_MS);
        }
        throw error;
      })
      .then(async (payload) => {
        const summaryDashboard = mapJobResultsToDashboard(payload);
        const jobStatus = asString(payload?.job?.status).toLowerCase();
        const shouldCacheDashboard =
          ["completed", "failed"].includes(jobStatus) && dashboardHasRenderableCandidates(summaryDashboard);
        const initialChunkTarget = Math.min(
          Math.max(summaryDashboard.totalCandidates || 0, 0),
          DASHBOARD_INITIAL_CANDIDATE_CHUNK_SIZE,
        );
        const shouldBackfillFirstPage =
          summaryDashboard.totalCandidates > 0
          && summaryDashboard.candidates.length < initialChunkTarget;
        if (shouldBackfillFirstPage) {
          const firstPage = await getDashboardCandidatePage(jobId, {
            offset: 0,
            limit: DASHBOARD_INITIAL_CANDIDATE_CHUNK_SIZE,
          }).catch(() => null);
          if (firstPage) {
            const mergedDashboard = mergeDashboardCandidatePage(summaryDashboard, firstPage);
            return shouldCacheDashboard ? storeDashboardCache(cacheKey, mergedDashboard) : mergedDashboard;
          }
        }
        if (!shouldCacheDashboard) {
          return summaryDashboard;
        }
        return storeDashboardCache(cacheKey, summaryDashboard);
      })
      .finally(() => {
        dashboardPromiseCache.delete(cacheKey);
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

export async function getDashboardCandidatePage(
  jobId: string,
  options?: {
    offset?: number;
    limit?: number;
  },
): Promise<DashboardCandidatePage> {
  const offset = Math.max(Number(options?.offset || 0), 0);
  const limit = Math.max(Number(options?.limit || DASHBOARD_BACKGROUND_CANDIDATE_CHUNK_SIZE), 1);
  const cacheKey = dashboardCandidatePageCacheKey(jobId, offset, limit);
  let fetchPromise: Promise<DashboardCandidatePage> | undefined = dashboardCandidatePagePromiseCache.get(cacheKey);
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
        hasMore: offset + mockCandidates.length < mockDashboard.totalCandidates,
        nextOffset: offset + mockCandidates.length < mockDashboard.totalCandidates ? offset + mockCandidates.length : null,
        candidates: mockCandidates,
      } satisfies DashboardCandidatePage);
      dashboardCandidatePagePromiseCache.set(cacheKey, fetchPromise);
      return fetchPromise;
    }
    fetchPromise = fetchJson<any>(
      `/api/jobs/${jobId}/candidates${buildApiQueryString({
        offset,
        limit,
        lightweight: 1,
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
          hasMore: Boolean(payload.has_more),
          nextOffset:
            typeof payload.next_offset === "number" && Number.isFinite(payload.next_offset)
              ? Number(payload.next_offset)
              : null,
          candidates: asArray(payload.candidates)
            .map((item) => ((item && typeof item === "object" ? item : {}) as Record<string, unknown>))
            .filter((item) => Object.keys(item).length > 0)
            .map((item) => deriveCandidate(item)),
        };
      })
      .finally(() => {
        dashboardCandidatePagePromiseCache.delete(cacheKey);
      });
    if (fetchPromise) {
      dashboardCandidatePagePromiseCache.set(cacheKey, fetchPromise);
    }
  }
  return fetchPromise as Promise<DashboardCandidatePage>;
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
}): Promise<TargetCandidateRecord[]> {
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    historyId: options?.historyId || "",
    candidateId: options?.candidateId || "",
    followUpStatus: options?.followUpStatus || "",
  });
  const cached = readFreshCacheValue(targetCandidatesCache, cacheKey, LIST_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }
  let fetchPromise = targetCandidatesPromiseCache.get(cacheKey);
  if (!fetchPromise) {
    fetchPromise = fetchJson<any>(
      `/api/target-candidates${buildApiQueryString({
        job_id: options?.jobId,
        history_id: options?.historyId,
        candidate_id: options?.candidateId,
        follow_up_status: options?.followUpStatus,
      })}`,
    )
      .then((payload) => {
        const items = Array.isArray(payload.target_candidates) ? payload.target_candidates : [];
        return writeCacheValue(
          targetCandidatesCache,
          cacheKey,
          items.map((item: Record<string, unknown>) => deriveTargetCandidateRecord(item)),
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
}): TargetCandidateRecord[] {
  const cacheKey = JSON.stringify({
    jobId: options?.jobId || "",
    historyId: options?.historyId || "",
    candidateId: options?.candidateId || "",
    followUpStatus: options?.followUpStatus || "",
  });
  return readFreshCacheValue(targetCandidatesCache, cacheKey, LIST_CACHE_TTL_MS) || [];
}

export async function upsertTargetCandidate(payload: {
  id?: string;
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
  const response = await fetchJson<any>("/api/target-candidates", {
    method: "POST",
    body: JSON.stringify({
      record_id: payload.id,
      candidate_id: payload.candidateId,
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
  return deriveTargetCandidateRecord((response.target_candidate || {}) as Record<string, unknown>);
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
}): Promise<{ blob: Blob; filename: string; contentType: string }> {
  return fetchBinary(
    "/api/target-candidates/export",
    {
      method: "POST",
      body: JSON.stringify({
        record_ids: Array.from(new Set((payload?.recordIds || []).map((value) => value.trim()).filter(Boolean))),
        job_id: payload?.jobId || "",
        history_id: payload?.historyId || "",
        candidate_id: payload?.candidateId || "",
        follow_up_status: payload?.followUpStatus || "",
      }),
    },
    RESULTS_API_TIMEOUT_MS,
  );
}

export async function exportTargetCandidatePublicWebArchive(payload?: {
  recordIds?: string[];
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
  mode?: "promoted_only" | "promoted_and_publishable";
}): Promise<{ blob: Blob; filename: string; contentType: string }> {
  return fetchBinary(
    "/api/target-candidates/public-web-export",
    {
      method: "POST",
      body: JSON.stringify({
        record_ids: Array.from(new Set((payload?.recordIds || []).map((value) => value.trim()).filter(Boolean))),
        job_id: payload?.jobId || "",
        history_id: payload?.historyId || "",
        candidate_id: payload?.candidateId || "",
        follow_up_status: payload?.followUpStatus || "",
        mode: payload?.mode || "promoted_only",
      }),
    },
    RESULTS_API_TIMEOUT_MS,
  );
}

export async function getTargetCandidatePublicWebSearches(options?: {
  batchId?: string;
  recordId?: string;
  status?: string;
  limit?: number;
}): Promise<TargetCandidatePublicWebSearchState> {
  const response = await fetchJson<any>(
    `/api/target-candidates/public-web-search${buildApiQueryString({
      batch_id: options?.batchId,
      record_id: options?.recordId,
      status: options?.status,
      limit: options?.limit,
    })}`,
  );
  return deriveTargetCandidatePublicWebSearchState(response);
}

export async function getTargetCandidatePublicWebDetail(recordId: string): Promise<TargetCandidatePublicWebDetail> {
  const response = await fetchJson<any>(
    `/api/target-candidates/${encodeURIComponent(recordId)}/public-web-search`,
  );
  return deriveTargetCandidatePublicWebDetail(response);
}

export async function startTargetCandidatePublicWebSearch(payload: {
  recordIds: string[];
  options?: Record<string, unknown>;
  forceRefresh?: boolean;
  requestedBy?: string;
}): Promise<TargetCandidatePublicWebStartResult> {
  const response = await fetchJson<any>("/api/target-candidates/public-web-search", {
    method: "POST",
    body: JSON.stringify({
      record_ids: Array.from(new Set((payload.recordIds || []).map((value) => value.trim()).filter(Boolean))),
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

export async function promoteTargetCandidatePublicWebSignal(payload: {
  recordId: string;
  signalId: string;
  action?: "promote" | "reject";
  operator?: string;
  note?: string;
  allowUnpublishable?: boolean;
}): Promise<{
  status: string;
  promotion: TargetCandidatePublicWebPromotion | null;
  detail: TargetCandidatePublicWebDetail | null;
}> {
  const response = await fetchJson<any>(
    `/api/target-candidates/${encodeURIComponent(payload.recordId)}/public-web-promotions`,
    {
      method: "POST",
      body: JSON.stringify({
        signal_id: payload.signalId,
        action: payload.action || "promote",
        operator: payload.operator || "frontend",
        note: payload.note || "",
        allow_unpublishable: Boolean(payload.allowUnpublishable),
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

  const avatarUrl = firstNonEmptyString([
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
    outreachLayer: Number(record.outreach_layer ?? metadata.outreach_layer ?? 0) || 0,
    outreachLayerKey:
      pickFirstString(record, ["outreach_layer_key"]) ||
      pickFirstString(metadata, ["outreach_layer_key"]),
    matchedKeywords: distinctMatchedKeywords(matchedFieldRecords),
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
      pickProfilePhoto(profilePhotoMap, pickFirstString(record, ["candidate_id", "id"])) ||
      pickFirstString(record, ["avatar_url", "photo_url", "media_url"]),
    team: normalizeTeam(pickFirstString(record, ["team", "group_name", "group"])),
    employmentStatus:
      employmentStatus === "current" || employmentStatus === "former" ? employmentStatus : "lead",
    confidence: toConfidence(pickFirstString(record, ["confidence_label"])),
    summary:
      pickFirstString(record, ["summary", "notes", "description"]) ||
      "Recovered from candidate documents.",
    outreachLayer: Number(record.outreach_layer ?? metadata.outreach_layer ?? 0) || 0,
    outreachLayerKey:
      pickFirstString(record, ["outreach_layer_key"]) ||
      pickFirstString(metadata, ["outreach_layer_key"]),
    matchedKeywords: distinctMatchedKeywords(matchedFieldRecords),
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
    avatarUrl: pickFirstString(record, ["avatar_url"]),
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
    candidateId: pickFirstString(record, ["candidate_id"]),
    historyId: pickFirstString(record, ["history_id"]),
    jobId: pickFirstString(record, ["job_id"]),
    candidateName: pickFirstString(record, ["candidate_name", "display_name", "name"]),
    headline: pickFirstString(record, ["headline"]),
    currentCompany: pickFirstString(record, ["current_company"]),
    avatarUrl: pickFirstString(record, ["avatar_url"]),
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
  };
}

function deriveTargetCandidatePublicWebBatch(record: Record<string, unknown>): TargetCandidatePublicWebBatch {
  return {
    batchId: pickFirstString(record, ["batch_id", "batchId"]),
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
    artifactRoot: pickFirstString(record, ["artifact_root", "artifactRoot"]),
    lastError: pickFirstString(record, ["last_error", "lastError"]),
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
    newValue: pickFirstString(record, ["new_value", "newValue"]),
    previousValue: pickFirstString(record, ["previous_value", "previousValue"]),
    sourceUrl: pickFirstString(record, ["source_url", "sourceUrl"]),
    sourceDomain: pickFirstString(record, ["source_domain", "sourceDomain"]),
    confidenceLabel: pickFirstString(record, ["confidence_label", "confidenceLabel"]),
    identityMatchLabel: pickFirstString(record, ["identity_match_label", "identityMatchLabel"]),
    action: pickFirstString(record, ["action"]),
    promotionStatus: pickFirstString(record, ["promotion_status", "promotionStatus"]),
    operator: pickFirstString(record, ["operator"]),
    note: pickFirstString(record, ["note"]),
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
    normalized === "analyzing" ||
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
