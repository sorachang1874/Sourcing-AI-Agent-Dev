import { useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Avatar } from "./Avatar";
import { FacetMultiSelect } from "./FacetMultiSelect";
import {
  type CandidateFacetOption,
  buildEmploymentOptions,
  buildLocationOptions,
  buildRecallBucketOptions,
  computeCandidateIntentKeywordHits,
  defaultRecallSelection,
  filterCandidatesByFacets,
  normalizeFacetSelection,
  preserveEditedFacetSelection,
  summarizeSelectedFacet,
  toggleFacetSelection,
} from "../lib/candidateFilters";
import {
  candidateAutoReviewStatus,
  candidateNeedsProfileCompletion,
  employmentStatusLabel,
  extractPrimaryEmail,
  LAYER_DEFINITIONS,
  pickCandidateRoleLine,
  reviewStatusLabel,
  resolveCandidateLinkedinUrl,
  sanitizeCandidateText,
} from "../lib/candidatePresentation";
import { addCandidateToReviewRegistry } from "../lib/reviewRegistry";
import {
  addTargetCandidate,
  addTargetCandidates,
  readTargetCandidates,
  targetCandidatesUpdatedEventName,
} from "../lib/targetCandidatesStore";
import { getFormattedEducationExperience, getFormattedWorkExperience } from "../lib/profileFormatting";
import {
  agreeCanonicalFacetSummaryScope,
  dashboardCandidatePageFilterSignature,
  dashboardCandidatePageRevisionMatches,
  exportProjectionCandidatesArchive,
  getCandidateDetailsBatch,
  getDashboardCandidatePage,
  getProjectionCandidatePage,
  triggerJobCandidateProfileCompletion,
  type DashboardCandidatePage,
  type DashboardCandidatePageFilter,
} from "../lib/api";
import { buildCandidateSyncSummary } from "../lib/candidateSyncSummary";
import {
  dashboardCandidateHydrationBannerVisible,
  dashboardExpectedCandidateCount,
  dashboardLoadedCandidateRowCount,
  dashboardRowHydrationTargetCount,
} from "../lib/dashboardHydration";
import { buildWorkflowRoute } from "../lib/workflowContext";
import type {
  Candidate,
  CandidateDetail,
  CandidateReviewStatus,
  DashboardData,
} from "../types";

interface ResultsBoardPanelProps {
  dashboard: DashboardData;
  historyId?: string;
  jobId?: string;
  projectionId?: string;
  collectionId?: string;
  initialCandidateId?: string;
  isHydratingCandidates?: boolean;
  candidateHydrationError?: string;
  reviewStatusMap?: Record<string, CandidateReviewStatus>;
  onSelectedCandidateChange?: (candidateId: string) => void;
  onOpenManualReview?: (candidateId: string) => void;
  onReviewStateChanged?: () => void;
  onHydrationWindowChange?: (window: { requiredCandidateCount: number; backgroundCandidateCount: number }) => void;
}

type LayerSelectionState = "include" | "exclude" | "neutral";

const RESULTS_PAGE_SIZE = 24;

const AUDIT_STATUS_OPTIONS: Array<{ id: CandidateReviewStatus; label: string }> = [
  { id: "no_review_needed", label: "无需审核" },
  { id: "needs_review", label: "待审核" },
  { id: "needs_profile_completion", label: "信息不完整待补全" },
  { id: "low_profile_richness", label: "LinkedIn信息丰富度低" },
  { id: "verified_keep", label: "已核实候选人" },
  { id: "verified_exclude", label: "已核实可排除候选人" },
];

function downloadBlobFile(filename: string, blob: Blob): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

interface ResultsBoardFacetSessionState {
  keyword: string;
  selectedLayerStates: Record<string, LayerSelectionState>;
  selectedRecallBuckets: string[];
  selectedEmploymentStatuses: string[];
  selectedLocations: string[];
  selectedFunctionBuckets: string[];
  selectedAuditStatuses: string[];
  userEditedFacets: {
    recall: boolean;
    employment: boolean;
    locations: boolean;
    functions: boolean;
    audit: boolean;
  };
}

const resultsBoardFacetSessionState = new Map<string, ResultsBoardFacetSessionState>();

function cloneLayerSelectionStates(value: Record<string, LayerSelectionState>): Record<string, LayerSelectionState> {
  return { ...value };
}

function cloneUserEditedFacetState(value: ResultsBoardFacetSessionState["userEditedFacets"]) {
  return { ...value };
}

function cloneResultsBoardFacetSessionState(
  value: ResultsBoardFacetSessionState,
): ResultsBoardFacetSessionState {
  return {
    keyword: value.keyword,
    selectedLayerStates: cloneLayerSelectionStates(value.selectedLayerStates),
    selectedRecallBuckets: [...value.selectedRecallBuckets],
    selectedEmploymentStatuses: [...value.selectedEmploymentStatuses],
    selectedLocations: [...value.selectedLocations],
    selectedFunctionBuckets: [...value.selectedFunctionBuckets],
    selectedAuditStatuses: [...value.selectedAuditStatuses],
    userEditedFacets: cloneUserEditedFacetState(value.userEditedFacets),
  };
}

function readResultsBoardFacetSessionState(contextKey: string): ResultsBoardFacetSessionState | null {
  const value = resultsBoardFacetSessionState.get(contextKey);
  return value ? cloneResultsBoardFacetSessionState(value) : null;
}

function writeResultsBoardFacetSessionState(contextKey: string, value: ResultsBoardFacetSessionState): void {
  resultsBoardFacetSessionState.set(contextKey, cloneResultsBoardFacetSessionState(value));
}

function hasOutreachLayer(candidate: Candidate): candidate is Candidate & { outreachLayer: number } {
  return typeof candidate.outreachLayer === "number" && Number.isFinite(candidate.outreachLayer);
}

function layerLabel(layer: number | null): string {
  return typeof layer === "number" && Number.isFinite(layer) ? `Layer ${layer}` : "未分层";
}

function pickCandidateKeywords(candidate: Candidate, intentKeywords: string[]): string[] {
  return computeCandidateIntentKeywordHits(candidate, intentKeywords).slice(0, 4);
}

function buildLayerOptionsFromCandidates(candidates: Candidate[]): CandidateFacetOption[] {
  const counts = candidates.reduce<Record<string, number>>((accumulator, candidate) => {
    if (!hasOutreachLayer(candidate)) {
      return accumulator;
    }
    const key = `layer_${candidate.outreachLayer}`;
    accumulator[key] = (accumulator[key] || 0) + 1;
    return accumulator;
  }, {});
  const hasLayerMetadata = candidates.some(hasOutreachLayer);
  return LAYER_DEFINITIONS.map((item) => ({
    id: item.id,
    label: item.label,
    count: item.id === "layer_0" ? (hasLayerMetadata ? candidates.length : 0) : counts[item.id] || 0,
  }));
}

function canonicalBoardFacetOptions(
  hasCanonicalSummary: boolean,
  options: CandidateFacetOption[] | undefined,
  fallback: CandidateFacetOption[],
  hasBoardRuntimeState: boolean,
): CandidateFacetOption[] {
  if (hasCanonicalSummary) {
    return options && options.length > 0 ? options : [];
  }
  // For canonical backend paging, partial rows are not a valid global count
  // source. Legacy/no-board pages may still use local fallback options.
  return hasBoardRuntimeState ? [] : fallback;
}

function defaultOpenSelection(options: CandidateFacetOption[]): string[] {
  return options.map((option) => option.id);
}

function selectedBackendFacetFilterIds(
  selectedIds: string[],
  options: CandidateFacetOption[],
  allIds: string[] = options.map((option) => option.id),
): string[] {
  const normalizedSelected = Array.from(new Set(selectedIds.map((item) => item.trim()).filter(Boolean))).sort();
  const normalizedAll = Array.from(new Set(allIds.map((item) => item.trim()).filter(Boolean))).sort();
  if (normalizedSelected.length === 0) {
    return [];
  }
  if (
    normalizedAll.length > 0 &&
    normalizedSelected.length === normalizedAll.length &&
    normalizedSelected.every((item, index) => item === normalizedAll[index])
  ) {
    return [];
  }
  return normalizedSelected;
}

/**
 * Does a serialized backend page filter NARROW the served population?
 * (FT2 fixed-forward r5, rerun4 review finding 3) Every axis — keyword,
 * recall, employment, locations, functions, audit, and layer include/exclude
 * — is read from the ONE filter contract, so no per-facet hand-written
 * predicate can silently omit an axis. An all-selected default resolves to
 * an empty axis upstream (INACTIVE no-op), so any non-empty axis is a real
 * narrowing. The layer axes are compared against the DEFAULT applied layer
 * filter (include `layer_0`, no excludes): the default layer view is the
 * product baseline, not user narrowing.
 */
function dashboardCandidatePageFilterNarrows(filterSignature: string): boolean {
  if (!filterSignature) {
    return false;
  }
  try {
    const filter = JSON.parse(filterSignature) as DashboardCandidatePageFilter;
    const layerIncludes = filter.layerIncludes || [];
    const layerExcludes = filter.layerExcludes || [];
    // The layer axes narrow only when they filter BEYOND the default
    // applied layer view (include `layer_0`, no excludes): an exclude, or
    // an include outside `layer_0`. An all-neutral layer state (empty
    // include AND exclude lists) is the UN-narrowed baseline — and it is
    // also what the gap-collapsed empty filter looks like, so it must not
    // read as narrowing intent.
    const layerNarrows =
      layerExcludes.length > 0 ||
      (layerIncludes.length > 0 &&
        (layerIncludes.length !== 1 || layerIncludes[0] !== "layer_0"));
    return Boolean(
      String(filter.searchKeyword || "").trim() !== "" ||
        (filter.recallBuckets || []).length > 0 ||
        (filter.employmentStatuses || []).length > 0 ||
        (filter.locations || []).length > 0 ||
        (filter.functionBuckets || []).length > 0 ||
        layerNarrows ||
        (filter.auditStatuses || []).length > 0,
    );
  } catch {
    return false;
  }
}

function hasCanonicalFacetSummaryForServedPopulation(
  dashboard: DashboardData,
  expectedCandidateCount: number,
): boolean {
  const summary = dashboard.candidateFacetSummary;
  // The summary scope is consumed ONLY from its own backend owners (the
  // top-level mapping and the board-runtime mirror). The filter contract's
  // `facetCountScope` is a SEPARATE contract and is never summary-scope
  // evidence (FT2 fixed-forward r4, rerun3 review finding 2; r5 hardening
  // per rerun4 review finding 2). EVERY applicable mirror must carry a
  // valid closed-vocabulary value and all must agree byte-exactly — a
  // missing, padded, or disagreeing mirror disables facet consumption
  // instead of letting one remaining mirror become authoritative.
  const facetSummaryScope = agreeCanonicalFacetSummaryScope([
    dashboard.candidateFacetSummaryScope,
    ...(dashboard.boardRuntimeState ? [dashboard.boardRuntimeState.facetSummaryScope] : []),
  ]);
  const canonicalScope =
    facetSummaryScope === "global_full_population" || facetSummaryScope === "exact_projection";
  const summaryCandidateCount = Math.max(0, Number(summary?.candidateCount || 0));
  const layerZeroCount = Math.max(
    0,
    Number((summary?.layers || []).find((option) => option.id === "layer_0")?.count || 0),
  );
  const summaryMatchesExpected = Boolean(
    canonicalScope &&
      // The canonical contract requires EXACT equality to the served
      // membership N (rerun5 review finding 4): an oversized summary (3
      // candidates over a 2-member projection) is contradictory evidence,
      // not an exact canonical summary.
      summaryCandidateCount === expectedCandidateCount &&
      (
        facetSummaryScope === "exact_projection" ||
        (summary?.layers || []).length === 0 ||
        layerZeroCount === expectedCandidateCount
      ),
  );
  if (dashboard.boardRuntimeState) {
    return (
      summaryMatchesExpected &&
      dashboard.boardRuntimeState.facetSummaryStatus === "complete" &&
      (
        dashboard.boardRuntimeState.facetSummaryScope === "global_full_population" ||
        dashboard.boardRuntimeState.facetSummaryScope === "exact_projection"
      ) &&
      dashboard.boardRuntimeState.facetSummaryCandidateCount === expectedCandidateCount
    );
  }
  if (
    !summaryMatchesExpected ||
    !summary?.candidateCount
  ) {
    return false;
  }
  const lifecycle = dashboard.resultViewLifecycle;
  if (!lifecycle) {
    return true;
  }
  const terminalServingState =
    lifecycle.state === "current_snapshot_serving" ||
    lifecycle.state === "post_result_layering";
  const boardVisibleServingState =
    lifecycle.state === "partial_board_visible" ||
    lifecycle.state === "delta_applying" ||
    lifecycle.state === "current_snapshot_materializing" ||
    lifecycle.servingProjectionPhase === "partial_delta_overlay" ||
    lifecycle.servingProjectionPhase === "partial_delta_board_visible_overlay";
  const servedCount = Math.max(0, Number(lifecycle.servedCandidateCount || 0));
  const expectedCount = Math.max(0, Number(lifecycle.expectedCandidateCount || 0), expectedCandidateCount);
  const requiredDelta = Math.max(0, Number(lifecycle.deltaProfileRequiredCount || 0));
  const materializedDelta = Math.max(
    0,
    Number(lifecycle.deltaProfileBoardVisibleCount ?? lifecycle.deltaProfileMaterializedCount ?? 0),
  );
  return (
    (terminalServingState || boardVisibleServingState) &&
    servedCount >= expectedCount &&
    (!lifecycle.deltaProfileProgressApplicable || requiredDelta === 0 || materializedDelta >= requiredDelta)
  );
}

function defaultLayerSelectionStates(): Record<string, LayerSelectionState> {
  return {
    layer_0: "include",
    layer_1: "neutral",
    layer_2: "neutral",
    layer_3: "neutral",
  };
}

function nextLayerSelectionState(current: LayerSelectionState): LayerSelectionState {
  if (current === "neutral") {
    return "include";
  }
  if (current === "include") {
    return "exclude";
  }
  return "neutral";
}

function summarizeLayerSelection(selection: Record<string, LayerSelectionState>, options: CandidateFacetOption[]): string {
  const included = options.filter((option) => selection[option.id] === "include").map((option) => option.label);
  const excluded = options.filter((option) => selection[option.id] === "exclude").map((option) => option.label);
  if (included.length === 1 && included[0] === "Layer 0" && excluded.length === 0) {
    return "全量";
  }
  if (included.length === 0 && excluded.length === 0) {
    return "未设置";
  }
  if (included.length > 0 && excluded.length === 0) {
    return included.join("、");
  }
  if (included.length === 0 && excluded.length > 0) {
    return `排除 ${excluded.join("、")}`;
  }
  return `包含 ${included.join("、")}；排除 ${excluded.join("、")}`;
}

function updateLayerSelection(
  current: Record<string, LayerSelectionState>,
  optionId: string,
): Record<string, LayerSelectionState> {
  const next = {
    ...current,
    [optionId]: nextLayerSelectionState(current[optionId] || "neutral"),
  };
  if (optionId !== "layer_0" && next[optionId] === "include") {
    next.layer_0 = "neutral";
  }
  if (optionId === "layer_0" && next.layer_0 === "include") {
    Object.keys(next).forEach((key) => {
      if (key !== "layer_0" && next[key] === "include") {
        next[key] = "neutral";
      }
    });
  }
  return next;
}

function matchesLayerSelection(candidate: Candidate, selection: Record<string, LayerSelectionState>): boolean {
  const exactLayerId = hasOutreachLayer(candidate) ? `layer_${candidate.outreachLayer}` : "";
  const excluded = Object.entries(selection)
    .filter(([, state]) => state === "exclude")
    .map(([id]) => id);
  if (exactLayerId && excluded.includes(exactLayerId)) {
    return false;
  }
  const includedExactLayers = Object.entries(selection)
    .filter(([id, state]) => id !== "layer_0" && state === "include")
    .map(([id]) => id);
  if (includedExactLayers.length > 0) {
    return exactLayerId ? includedExactLayers.includes(exactLayerId) : false;
  }
  if (selection.layer_0 === "include") {
    return true;
  }
  if (excluded.length > 0) {
    return true;
  }
  return true;
}

function buildAuditStatusOptions(
  candidates: Candidate[],
  reviewStatusMap: Record<string, CandidateReviewStatus>,
): CandidateFacetOption[] {
  const counts = candidates.reduce<Record<string, number>>((accumulator, candidate) => {
    const key = reviewStatusMap[candidate.id] || candidateAutoReviewStatus(candidate) || "no_review_needed";
    accumulator[key] = (accumulator[key] || 0) + 1;
    return accumulator;
  }, {});
  return AUDIT_STATUS_OPTIONS.map((item) => ({
    id: item.id,
    label: item.label,
    count: counts[item.id] || 0,
  }));
}

function sameStringArray(left: string[], right: string[]): boolean {
  if (left === right) {
    return true;
  }
  if (left.length !== right.length) {
    return false;
  }
  return left.every((value, index) => value === right[index]);
}

function defaultAuditStatusSelection(): string[] {
  // Keep the main board visible by default. Open-review rows stay surfaced
  // alongside card-ready rows and can still be narrowed via the audit filter.
  return AUDIT_STATUS_OPTIONS.map((item) => item.id);
}

function filterByAuditStatus(
  candidates: Candidate[],
  selectedAuditStatuses: string[],
  reviewStatusMap: Record<string, CandidateReviewStatus>,
): Candidate[] {
  if (selectedAuditStatuses.length === 0) {
    return candidates;
  }
  return candidates.filter((candidate) =>
    selectedAuditStatuses.includes(reviewStatusMap[candidate.id] || candidateAutoReviewStatus(candidate) || "no_review_needed"),
  );
}

function groupCandidatesByAuditStatus(
  candidates: Candidate[],
  reviewStatusMap: Record<string, CandidateReviewStatus>,
): Array<{ status: CandidateReviewStatus; label: string; candidates: Candidate[] }> {
  const labelsByStatus = new Map(AUDIT_STATUS_OPTIONS.map((option) => [option.id, option.label] as const));
  const groups = new Map<CandidateReviewStatus, { status: CandidateReviewStatus; label: string; candidates: Candidate[] }>();
  candidates.forEach((candidate) => {
    const status = reviewStatusMap[candidate.id] || candidateAutoReviewStatus(candidate) || "no_review_needed";
    const existing = groups.get(status);
    if (existing) {
      existing.candidates.push(candidate);
      return;
    }
    groups.set(status, {
      status,
      label: labelsByStatus.get(status) || reviewStatusLabel(status),
      candidates: [candidate],
    });
  });
  return Array.from(groups.values());
}

function LayerTriStateFilter({
  options,
  selection,
  onToggle,
  disabled = false,
  disabledLabel = "分层未生成",
}: {
  options: CandidateFacetOption[];
  selection: Record<string, LayerSelectionState>;
  onToggle: (optionId: string) => void;
  disabled?: boolean;
  disabledLabel?: string;
}) {
  return (
    <details className="facet-dropdown facet-dropdown-wide">
      <summary className="facet-dropdown-trigger">
        <div className="facet-label-with-help">
          <span className="field-label">华人线索信息分层</span>
          <span className="help-badge" aria-label="查看分层定义">
            ?
            <span className="help-tooltip">
              {LAYER_DEFINITIONS.map((item) => (
                <span key={item.id}>{item.label}：{item.description}</span>
              ))}
            </span>
          </span>
        </div>
        <strong>{disabled ? disabledLabel : summarizeLayerSelection(selection, options)}</strong>
      </summary>
      <div className="facet-dropdown-menu">
        {disabled ? <p className="muted">当前结果尚未提供可筛选的华人线索分层，Layer 0 不代表已完成分层。</p> : null}
        {options.map((option) => {
          const state = selection[option.id] || "neutral";
          return (
            <button
              key={option.id}
              type="button"
              className={`layer-tristate-option state-${state}`}
              disabled={disabled}
              onClick={() => onToggle(option.id)}
            >
              <span className="layer-tristate-indicator" aria-hidden="true">
                {state === "include" ? "✓" : state === "exclude" ? "×" : ""}
              </span>
              <span className="layer-tristate-copy">
                <strong>{option.label}</strong>
                <span>{LAYER_DEFINITIONS.find((item) => item.id === option.id)?.description || ""}</span>
              </span>
              <em>{option.count}</em>
            </button>
          );
        })}
      </div>
    </details>
  );
}

export function ResultsBoardPanel({
  dashboard,
  historyId = "",
  jobId = "",
  projectionId = "",
  collectionId = "",
  initialCandidateId = "",
  isHydratingCandidates = false,
  candidateHydrationError = "",
  reviewStatusMap = {},
  onSelectedCandidateChange,
  onOpenManualReview,
  onReviewStateChanged,
  onHydrationWindowChange,
}: ResultsBoardPanelProps) {
  const resolvedProjectionId = String(
    projectionId || dashboard.projectionId || dashboard.resultViewLifecycle?.servingProjectionId || "",
  ).trim();
  const membershipRevision = String(dashboard.boardRuntimeState?.rowPublicationRevision || "").trim();
  const projectionMutationReady = Boolean(resolvedProjectionId && membershipRevision);
  const [keyword, setKeyword] = useState("");
  const [focusedCandidateId, setFocusedCandidateId] = useState("");
  const [selectedLayerStates, setSelectedLayerStates] = useState<Record<string, LayerSelectionState>>(
    defaultLayerSelectionStates,
  );
  const [selectedRecallBuckets, setSelectedRecallBuckets] = useState<string[]>([]);
  const [selectedEmploymentStatuses, setSelectedEmploymentStatuses] = useState<string[]>([]);
  const [selectedLocations, setSelectedLocations] = useState<string[]>([]);
  const [selectedFunctionBuckets, setSelectedFunctionBuckets] = useState<string[]>([]);
  const [selectedAuditStatuses, setSelectedAuditStatuses] = useState<string[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [targetCandidateIds, setTargetCandidateIds] = useState<string[]>([]);
  const [selectedCandidates, setSelectedCandidates] = useState<Record<string, Candidate>>({});
  const [batchActionBusy, setBatchActionBusy] = useState<"" | "review" | "profile_completion" | "target" | "export">("");
  const [batchActionMessage, setBatchActionMessage] = useState("");
  const [targetActionCompleted, setTargetActionCompleted] = useState(false);
  const [singleTargetActionCandidateId, setSingleTargetActionCandidateId] = useState("");
  const [candidateDetailsById, setCandidateDetailsById] = useState<Record<string, CandidateDetail | null>>({});
  const [candidateDetailLoadingIds, setCandidateDetailLoadingIds] = useState<string[]>([]);
  const [backendCandidatePage, setBackendCandidatePage] = useState<DashboardCandidatePage | null>(null);
  const [backendCandidatePageRequestSignature, setBackendCandidatePageRequestSignature] = useState("");
  const [backendCandidatePageLoading, setBackendCandidatePageLoading] = useState(false);
  const [backendCandidatePageError, setBackendCandidatePageError] = useState("");
  // Preserve last-known filtered count so pagination does not collapse to
  // page 1 while a backend page request is pending. Cleared on filter change.
  const [lastKnownFilteredCandidateCount, setLastKnownFilteredCandidateCount] = useState(0);
  const appliedContextCandidateRef = useRef("");
  const pendingDefaultFacetContextRef = useRef("");
  const userEditedFacetRefs = useRef({
    recall: false,
    employment: false,
    locations: false,
    functions: false,
    audit: false,
  });
  const resultsContextKey = useMemo(
    () => [historyId || "no-history", jobId || "no-job", resolvedProjectionId || "no-projection"].join(":"),
    [historyId, jobId, resolvedProjectionId],
  );
  const projectionOnlyReadOnly = Boolean(resolvedProjectionId && !jobId);
  const projectionOnlyReadOnlyMessage =
    "当前从本地公司资产打开，可浏览候选人并加入目标候选人；人工审核和资料补全需要从具体任务进入。";
  const targetCandidatesRoute = collectionId.trim()
    ? `/targets?collection=${encodeURIComponent(collectionId.trim())}`
    : "/targets";
  const skipNextFacetSessionSaveRef = useRef(false);
  // Last backend filter signature RESOLVED outside a canonical-summary gap,
  // keyed to its projection context (FT2 fixed-forward r5, rerun4 review
  // finding 3; r6 hardening per rerun5 review finding 3): the generic record
  // of the user's narrowing intent over every filter axis. The context key
  // is part of the record because `SearchFlow` does not remount this
  // component when the projection changes inside the same job — a stale
  // signature from a previous projection must never leak into the new
  // context's gap preservation.
  const lastResolvedFilterSignatureRef = useRef<{ contextKey: string; signature: string }>({
    contextKey: "",
    signature: "",
  });

  const candidateFacetSummary = dashboard.candidateFacetSummary;
  const expectedCandidateCount = dashboardExpectedCandidateCount(dashboard);
  const profileDetailCandidateCount = Math.max(
    0,
    Number(dashboard.boardRuntimeState?.profileDetailCandidateCount || 0),
  );
  const profileDetailsIncomplete = Boolean(
    resolvedProjectionId && expectedCandidateCount > 0 && profileDetailCandidateCount < expectedCandidateCount,
  );
  const hasGlobalFacetSummary = hasCanonicalFacetSummaryForServedPopulation(dashboard, expectedCandidateCount);
  const hasBoardRuntimeState = Boolean(dashboard.boardRuntimeState);
  const canonicalFacetUnavailable = hasBoardRuntimeState && !hasGlobalFacetSummary;
  const canonicalFacetUnavailableMessage =
    "筛选索引还在准备中。当前可以分页浏览候选人，但暂不启用结果内搜索、分层、地区和职能筛选。";
  const filterControlsAvailable = !canonicalFacetUnavailable;
  const disabledFilterSummary = "索引准备中";
  const facetSummaryLabel = (options: CandidateFacetOption[], selectedIds: string[], fallbackLabel: string) =>
    options.length > 0 ? summarizeSelectedFacet(selectedIds, options, fallbackLabel) : "统计未生成";
  const layerOptions = useMemo(
    () =>
      canonicalBoardFacetOptions(
        hasGlobalFacetSummary,
        candidateFacetSummary?.layers,
        buildLayerOptionsFromCandidates(dashboard.candidates),
        hasBoardRuntimeState,
      ),
    [candidateFacetSummary?.layers, dashboard.candidates, hasBoardRuntimeState, hasGlobalFacetSummary],
  );
  const layerOptionCount = useMemo(
    () => layerOptions.reduce((sum, option) => sum + Math.max(0, Number(option.count || 0)), 0),
    [layerOptions],
  );
  const hasLayerMetadata = layerOptionCount > 0;
  const outreachLayeringStatus = (
    dashboard.boardRuntimeState?.layeringStatus ||
    (!dashboard.boardRuntimeState ? dashboard.resultViewLifecycle?.outreachLayeringStatus : "") ||
    ""
  ).toLowerCase();
  const outreachLayeringInProgress = ["scheduled", "running", "deferred"].includes(outreachLayeringStatus);
  const layerDisabledLabel = outreachLayeringInProgress ? "分层生成中" : "分层未生成";
  const recallOptions = useMemo(
    () =>
      canonicalBoardFacetOptions(
        hasGlobalFacetSummary,
        candidateFacetSummary?.recall,
        buildRecallBucketOptions(dashboard.candidates, dashboard.intentKeywords),
        hasBoardRuntimeState,
      ),
    [candidateFacetSummary?.recall, dashboard.candidates, dashboard.intentKeywords, hasBoardRuntimeState, hasGlobalFacetSummary],
  );
  const employmentOptions = useMemo(() => buildEmploymentOptions(dashboard.candidates), [dashboard.candidates]);
  const employmentFacetOptions = useMemo(
    () =>
      canonicalBoardFacetOptions(
        hasGlobalFacetSummary,
        candidateFacetSummary?.employment,
        employmentOptions,
        hasBoardRuntimeState,
      ),
    [candidateFacetSummary?.employment, employmentOptions, hasBoardRuntimeState, hasGlobalFacetSummary],
  );
  const locationOptions = useMemo(
    () =>
      canonicalBoardFacetOptions(
        hasGlobalFacetSummary,
        candidateFacetSummary?.locations,
        buildLocationOptions(dashboard.candidates),
        hasBoardRuntimeState,
      ),
    [candidateFacetSummary?.locations, dashboard.candidates, hasBoardRuntimeState, hasGlobalFacetSummary],
  );
  // Function facet options have ONE source — the canonical backend facet
  // summary (FT2 fixed-forward r2, review finding 5). When that summary is
  // unavailable the facet is DISABLED (empty options); it is never rebuilt
  // from candidate rows, and stale selections below are gated off so they
  // cannot reach the backend filter while the facet is disabled.
  const functionOptions = useMemo(
    () =>
      canonicalBoardFacetOptions(
        hasGlobalFacetSummary,
        candidateFacetSummary?.functions,
        [],
        hasBoardRuntimeState,
      ),
    [candidateFacetSummary?.functions, hasBoardRuntimeState, hasGlobalFacetSummary],
  );
  const functionFacetAvailable = functionOptions.length > 0;
  const auditStatusOptions = useMemo(
    () => buildAuditStatusOptions(dashboard.candidates, reviewStatusMap),
    [dashboard.candidates, reviewStatusMap],
  );

  useEffect(() => {
    appliedContextCandidateRef.current = "";
    skipNextFacetSessionSaveRef.current = true;
    const cachedFacetState = readResultsBoardFacetSessionState(resultsContextKey);
    if (cachedFacetState) {
      pendingDefaultFacetContextRef.current = "";
      userEditedFacetRefs.current = cloneUserEditedFacetState(cachedFacetState.userEditedFacets);
      setKeyword(cachedFacetState.keyword);
      setSelectedLayerStates(cloneLayerSelectionStates(cachedFacetState.selectedLayerStates));
      setSelectedRecallBuckets([...cachedFacetState.selectedRecallBuckets]);
      setSelectedEmploymentStatuses([...cachedFacetState.selectedEmploymentStatuses]);
      setSelectedLocations([...cachedFacetState.selectedLocations]);
      setSelectedFunctionBuckets([...cachedFacetState.selectedFunctionBuckets]);
      setSelectedAuditStatuses([...cachedFacetState.selectedAuditStatuses]);
    } else {
      pendingDefaultFacetContextRef.current = resultsContextKey;
      userEditedFacetRefs.current = {
        recall: false,
        employment: false,
        locations: false,
        functions: false,
        audit: false,
      };
      setKeyword("");
      setSelectedLayerStates(defaultLayerSelectionStates());
      setSelectedRecallBuckets([]);
      setSelectedEmploymentStatuses([]);
      setSelectedLocations([]);
      setSelectedFunctionBuckets([]);
      setSelectedAuditStatuses(defaultAuditStatusSelection());
    }
    setFocusedCandidateId("");
    setCurrentPage(1);
    setSelectedCandidates({});
    setBatchActionMessage("");
    setTargetActionCompleted(false);
    setSingleTargetActionCandidateId("");
    setCandidateDetailsById({});
    setCandidateDetailLoadingIds([]);
    setBackendCandidatePage(null);
    setBackendCandidatePageRequestSignature("");
    setBackendCandidatePageError("");
  }, [resultsContextKey]);

  useEffect(() => {
    if (pendingDefaultFacetContextRef.current !== resultsContextKey || dashboard.candidates.length === 0) {
      return;
    }
    pendingDefaultFacetContextRef.current = "";
    skipNextFacetSessionSaveRef.current = true;
    setSelectedLayerStates(defaultLayerSelectionStates());
    setSelectedRecallBuckets(defaultRecallSelection(recallOptions));
    setSelectedEmploymentStatuses(defaultOpenSelection(employmentFacetOptions));
    setSelectedLocations(defaultOpenSelection(locationOptions));
    setSelectedFunctionBuckets(defaultOpenSelection(functionOptions));
    setSelectedAuditStatuses(defaultAuditStatusSelection());
    setCurrentPage(1);
  }, [
    dashboard.candidates.length,
    employmentFacetOptions,
    functionOptions,
    locationOptions,
    recallOptions,
    resultsContextKey,
  ]);

  useEffect(() => {
    if (pendingDefaultFacetContextRef.current === resultsContextKey) {
      return;
    }
    if (skipNextFacetSessionSaveRef.current) {
      skipNextFacetSessionSaveRef.current = false;
      return;
    }
    writeResultsBoardFacetSessionState(resultsContextKey, {
      keyword,
      selectedLayerStates,
      selectedRecallBuckets,
      selectedEmploymentStatuses,
      selectedLocations,
      selectedFunctionBuckets,
      selectedAuditStatuses,
      userEditedFacets: cloneUserEditedFacetState(userEditedFacetRefs.current),
    });
  }, [
    functionOptions,
    keyword,
    locationOptions,
    recallOptions,
    resultsContextKey,
    selectedAuditStatuses,
    selectedEmploymentStatuses,
    selectedFunctionBuckets,
    selectedLayerStates,
    selectedLocations,
    selectedRecallBuckets,
  ]);

  useEffect(() => {
    if (pendingDefaultFacetContextRef.current === resultsContextKey) {
      return;
    }
    // Canonical-summary gap (FT2 fixed-forward r4, rerun3 review finding 4):
    // while the canonical facet summary is transiently unavailable the
    // options list collapses to []; reconciling now would silently wipe the
    // user's selection and let a widened backend filter through. Selections
    // are preserved inertly keyed to the membership revision — they
    // reconcile only when canonical options return (a revision change
    // reconciles visibly at that point, never during the gap).
    if (canonicalFacetUnavailable) {
      return;
    }
    if (userEditedFacetRefs.current.recall) {
      setSelectedRecallBuckets((current) => {
        const preserved = preserveEditedFacetSelection(current, recallOptions);
        return sameStringArray(current, preserved) ? current : preserved;
      });
      return;
    }
    setSelectedRecallBuckets((current) => {
      const normalized = normalizeFacetSelection(current, recallOptions, defaultRecallSelection(recallOptions));
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [canonicalFacetUnavailable, recallOptions, resultsContextKey]);

  useEffect(() => {
    if (pendingDefaultFacetContextRef.current === resultsContextKey) {
      return;
    }
    if (canonicalFacetUnavailable) {
      return;
    }
    if (userEditedFacetRefs.current.employment) {
      setSelectedEmploymentStatuses((current) => {
        const preserved = preserveEditedFacetSelection(current, employmentFacetOptions);
        return sameStringArray(current, preserved) ? current : preserved;
      });
      return;
    }
    setSelectedEmploymentStatuses((current) => {
      const normalized = normalizeFacetSelection(
        current,
        employmentFacetOptions,
        defaultOpenSelection(employmentFacetOptions),
      );
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [canonicalFacetUnavailable, employmentFacetOptions, resultsContextKey]);

  useEffect(() => {
    if (pendingDefaultFacetContextRef.current === resultsContextKey) {
      return;
    }
    if (canonicalFacetUnavailable) {
      return;
    }
    if (userEditedFacetRefs.current.locations) {
      setSelectedLocations((current) => {
        const preserved = preserveEditedFacetSelection(current, locationOptions);
        return sameStringArray(current, preserved) ? current : preserved;
      });
      return;
    }
    setSelectedLocations((current) => {
      const normalized = normalizeFacetSelection(current, locationOptions, defaultOpenSelection(locationOptions));
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [canonicalFacetUnavailable, locationOptions, resultsContextKey]);

  useEffect(() => {
    if (pendingDefaultFacetContextRef.current === resultsContextKey) {
      return;
    }
    if (canonicalFacetUnavailable) {
      return;
    }
    if (userEditedFacetRefs.current.functions) {
      setSelectedFunctionBuckets((current) => {
        const preserved = preserveEditedFacetSelection(current, functionOptions);
        return sameStringArray(current, preserved) ? current : preserved;
      });
      return;
    }
    setSelectedFunctionBuckets((current) => {
      const normalized = normalizeFacetSelection(current, functionOptions, defaultOpenSelection(functionOptions));
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [canonicalFacetUnavailable, functionOptions, resultsContextKey]);

  useEffect(() => {
    if (pendingDefaultFacetContextRef.current === resultsContextKey) {
      return;
    }
    if (userEditedFacetRefs.current.audit) {
      setSelectedAuditStatuses((current) => {
        const preserved = preserveEditedFacetSelection(current, auditStatusOptions);
        return sameStringArray(current, preserved) ? current : preserved;
      });
      return;
    }
    setSelectedAuditStatuses((current) => {
      const normalized = normalizeFacetSelection(current, auditStatusOptions, defaultAuditStatusSelection());
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [auditStatusOptions, resultsContextKey]);

  useEffect(() => {
    const syncTargets = () => {
      void readTargetCandidates()
        .then((records) => {
          setTargetCandidateIds(
            Array.from(
              new Set(
                records.flatMap((record) =>
                  [record.candidateId, record.candidateIdentityKey, record.personIdentityKey].filter(Boolean) as string[],
                ),
              ),
            ),
          );
        })
        .catch(() => {
          setTargetCandidateIds([]);
        });
    };
    syncTargets();
    window.addEventListener(targetCandidatesUpdatedEventName(), syncTargets);
    window.addEventListener("storage", syncTargets);
    return () => {
      window.removeEventListener(targetCandidatesUpdatedEventName(), syncTargets);
      window.removeEventListener("storage", syncTargets);
    };
  }, []);

  // Canonical-summary gap with preserved narrowing intent (FT2 fixed-forward
  // r4, rerun3 review finding 4; r5 hardening per rerun4 review finding 3):
  // while the canonical facet summary is transiently unavailable, a
  // user-narrowed selection stays preserved inertly. Narrowing intent is
  // derived GENERICALLY from the last resolved/applied backend filter
  // signature — every axis (keyword, recall, employment, locations,
  // functions, audit, and layer include/exclude) is covered by the one
  // filter contract, so audit-only or layer-only narrowing can never slip
  // through a partial hand-written predicate and let a widened request
  // through. The current keyword / layer state and the user-edit flags
  // additionally cover the restored-session edge (a first render that lands
  // inside the gap before any filter could be resolved or applied).
  const layerSelectionNarrows =
    Object.values(selectedLayerStates).some((state) => state === "exclude") ||
    Object.entries(selectedLayerStates).some(([id, state]) => state === "include" && id !== "layer_0");
  // The last resolved signature counts ONLY when it belongs to THIS
  // projection context (rerun5 review finding 3): a signature recorded
  // under a different resultsContextKey is stale cross-context evidence.
  const lastResolvedFilterSignature =
    lastResolvedFilterSignatureRef.current.contextKey === resultsContextKey
      ? lastResolvedFilterSignatureRef.current.signature
      : "";
  const preservedFacetIntentDuringGap = Boolean(
    canonicalFacetUnavailable &&
      (dashboardCandidatePageFilterNarrows(backendCandidatePageRequestSignature) ||
        dashboardCandidatePageFilterNarrows(lastResolvedFilterSignature) ||
        keyword.trim() !== "" ||
        layerSelectionNarrows ||
        userEditedFacetRefs.current.recall ||
        userEditedFacetRefs.current.employment ||
        userEditedFacetRefs.current.locations ||
        userEditedFacetRefs.current.functions ||
        userEditedFacetRefs.current.audit),
  );

  const fallbackBaseVisibleCandidates = useMemo(
    () =>
      filterCandidatesByFacets(
        dashboard.candidates,
        {
          layers: [],
          recallBuckets: filterControlsAvailable ? selectedRecallBuckets : [],
          employmentStatuses: filterControlsAvailable ? selectedEmploymentStatuses : [],
          locations: filterControlsAvailable ? selectedLocations : [],
          functionBuckets: filterControlsAvailable && functionFacetAvailable ? selectedFunctionBuckets : [],
          searchKeyword: filterControlsAvailable ? keyword : "",
        },
        dashboard.intentKeywords,
      ),
    [
      dashboard.candidates,
      dashboard.intentKeywords,
      filterControlsAvailable,
      functionFacetAvailable,
      keyword,
      selectedFunctionBuckets,
      selectedEmploymentStatuses,
      selectedLocations,
      selectedRecallBuckets,
    ],
  );

  const fallbackVisibleCandidates = useMemo(() => {
    const afterAudit = filterControlsAvailable
      ? filterByAuditStatus(fallbackBaseVisibleCandidates, selectedAuditStatuses, reviewStatusMap)
      : fallbackBaseVisibleCandidates;
    return filterControlsAvailable
      ? afterAudit.filter((candidate) => matchesLayerSelection(candidate, selectedLayerStates))
      : afterAudit;
  }, [fallbackBaseVisibleCandidates, filterControlsAvailable, reviewStatusMap, selectedAuditStatuses, selectedLayerStates]);

  const includedLayerIds = useMemo(
    () =>
      Object.entries(selectedLayerStates)
        .filter(([, state]) => state === "include")
        .map(([id]) => id),
    [selectedLayerStates],
  );
  const excludedLayerIds = useMemo(
    () =>
      Object.entries(selectedLayerStates)
        .filter(([, state]) => state === "exclude")
        .map(([id]) => id),
    [selectedLayerStates],
  );
  const backendPageFilter = useMemo<DashboardCandidatePageFilter>(
    () =>
      filterControlsAvailable
        ? {
            searchKeyword: keyword,
            recallBuckets: selectedBackendFacetFilterIds(selectedRecallBuckets, recallOptions, ["all"]),
            employmentStatuses: selectedBackendFacetFilterIds(selectedEmploymentStatuses, employmentFacetOptions),
            locations: selectedBackendFacetFilterIds(selectedLocations, locationOptions),
            functionBuckets: functionFacetAvailable
              ? selectedBackendFacetFilterIds(selectedFunctionBuckets, functionOptions)
              : [],
            layerIncludes: includedLayerIds,
            layerExcludes: excludedLayerIds,
            auditStatuses: selectedBackendFacetFilterIds(
              selectedAuditStatuses,
              auditStatusOptions,
              AUDIT_STATUS_OPTIONS.map((item) => item.id),
            ),
          }
        : {
            searchKeyword: "",
            recallBuckets: [],
            employmentStatuses: [],
            locations: [],
            functionBuckets: [],
            layerIncludes: [],
            layerExcludes: [],
            auditStatuses: [],
          },
    [
      auditStatusOptions,
      employmentFacetOptions,
      excludedLayerIds,
      filterControlsAvailable,
      functionFacetAvailable,
      functionOptions,
      includedLayerIds,
      keyword,
      locationOptions,
      recallOptions,
      selectedAuditStatuses,
      selectedEmploymentStatuses,
      selectedFunctionBuckets,
      selectedLocations,
      selectedRecallBuckets,
    ],
  );
  const backendFilterSignature = useMemo(
    () => dashboardCandidatePageFilterSignature(backendPageFilter),
    [backendPageFilter],
  );
  useEffect(() => {
    // Record the last filter signature RESOLVED outside a canonical-summary
    // gap, keyed to the CURRENT projection context; during a gap the
    // resolved filter collapses to the empty object, so the recorded
    // signature is the only generic record of the user's narrowing intent
    // over every axis.
    if (!canonicalFacetUnavailable && backendFilterSignature) {
      lastResolvedFilterSignatureRef.current = {
        contextKey: resultsContextKey,
        signature: backendFilterSignature,
      };
    }
  }, [backendFilterSignature, canonicalFacetUnavailable, resultsContextKey]);
  const backendFilteredPagingSupported = Boolean(
    dashboard.boardRuntimeState?.filterContract?.backendFilteredPagingSupported,
  );
  const backendPageOffset = Math.max(0, (currentPage - 1) * RESULTS_PAGE_SIZE);
  // The kept gap page is bound to its FULL identity tuple (FT2 fixed-forward
  // r5, rerun4 review finding 4): same projection membership revision AND
  // the exact preserved filter signature AND the current offset/limit. An
  // older page from the same revision but a different filter (e.g. the
  // completed unfiltered page while the narrowed request was still in
  // flight) is NOT a valid "recent filtered result" — the board falls back
  // to the blocking empty state instead of presenting it as one.
  const preservedFilterSignature =
    lastResolvedFilterSignature || backendCandidatePageRequestSignature;
  const gapKeptBackendPage =
    preservedFacetIntentDuringGap &&
    backendCandidatePage &&
    dashboardCandidatePageRevisionMatches(dashboard, backendCandidatePage) &&
    backendCandidatePageRequestSignature !== "" &&
    backendCandidatePageRequestSignature === preservedFilterSignature &&
    backendCandidatePage.offset === backendPageOffset &&
    backendCandidatePage.limit <= RESULTS_PAGE_SIZE
      ? backendCandidatePage
      : null;
  const backendPageReady = Boolean(
    backendFilteredPagingSupported &&
      backendCandidatePage &&
      backendCandidatePageRequestSignature === backendFilterSignature &&
      backendCandidatePage.offset === backendPageOffset &&
      backendCandidatePage.limit <= RESULTS_PAGE_SIZE &&
      dashboardCandidatePageRevisionMatches(dashboard, backendCandidatePage),
  );
  const waitingForBackendPage = backendFilteredPagingSupported && !backendPageReady;
  const freshFilteredCandidateCount = backendPageReady
    ? Math.max(0, backendCandidatePage?.filteredCandidateCount || 0)
    : 0;
  const visibleCandidateCount = preservedFacetIntentDuringGap
    ? Math.max(0, gapKeptBackendPage?.filteredCandidateCount || 0)
    : backendPageReady
      ? freshFilteredCandidateCount
      : waitingForBackendPage
        ? lastKnownFilteredCandidateCount > 0
          ? lastKnownFilteredCandidateCount
          : fallbackVisibleCandidates.length
      : fallbackVisibleCandidates.length;
  const totalPages = Math.max(1, Math.ceil(visibleCandidateCount / RESULTS_PAGE_SIZE));
  const visibleCandidates = preservedFacetIntentDuringGap
    ? gapKeptBackendPage?.candidates || []
    : backendPageReady
      ? backendCandidatePage?.candidates || []
      : fallbackVisibleCandidates;
  const pagedCandidates = useMemo(() => {
    if (preservedFacetIntentDuringGap) {
      return gapKeptBackendPage?.candidates || [];
    }
    if (backendPageReady) {
      return backendCandidatePage?.candidates || [];
    }
    const start = Math.max(0, (currentPage - 1) * RESULTS_PAGE_SIZE);
    return fallbackVisibleCandidates.slice(start, start + RESULTS_PAGE_SIZE);
  }, [backendCandidatePage, backendPageReady, currentPage, fallbackVisibleCandidates, gapKeptBackendPage, preservedFacetIntentDuringGap, waitingForBackendPage]);
  const pagedDisplayCandidates = useMemo(
    () =>
      pagedCandidates.map((candidate) => ({
        ...candidate,
        ...(candidateDetailsById[candidate.id] || {}),
        id: candidate.id,
      })),
    [candidateDetailsById, pagedCandidates],
  );
  const pagedCandidateGroups = useMemo(
    () => groupCandidatesByAuditStatus(pagedDisplayCandidates, reviewStatusMap),
    [pagedDisplayCandidates, reviewStatusMap],
  );
  const currentPageDetailLoading = candidateDetailLoadingIds.some((candidateId) =>
    pagedCandidates.some((candidate) => candidate.id === candidateId),
  );
  const selectedCandidateIds = useMemo(() => Object.keys(selectedCandidates), [selectedCandidates]);
  const loadedCandidateRowCount = dashboardLoadedCandidateRowCount(dashboard);
  const hydrationBannerVisible = dashboardCandidateHydrationBannerVisible(dashboard, Boolean(isHydratingCandidates));
  const resultViewLifecycle = dashboard.resultViewLifecycle;
  const linkedinStage1Progress = dashboard.linkedinStage1Progress;
  const candidateSyncSummary = buildCandidateSyncSummary({
    loadedCandidateCount: loadedCandidateRowCount,
    expectedCandidateCount,
    resultViewLifecycle,
    boardRuntimeState: dashboard.boardRuntimeState,
    linkedinStage1Progress,
    effectiveExecutionSemantics: dashboard.effectiveExecutionSemantics,
    recallOptions,
  });
  const rowHydrationTargetCount = dashboard.boardRuntimeState ? dashboardRowHydrationTargetCount(dashboard) : 0;
  const localRowWindowComplete = rowHydrationTargetCount > 0 && loadedCandidateRowCount >= rowHydrationTargetCount;
  const visibleCandidateHitLabel =
    backendFilteredPagingSupported || localRowWindowComplete ? "当前筛选命中" : "已加载窗口筛选命中";
  const loadedRowWindowText =
    !backendPageReady && rowHydrationTargetCount > 0 && loadedCandidateRowCount < rowHydrationTargetCount
      ? `，已加载候选行 ${loadedCandidateRowCount}/${rowHydrationTargetCount}`
      : "";
  const selectedProfileCompletionCandidates = useMemo(
    () =>
      Object.values(selectedCandidates).filter((candidate) => {
        const status = reviewStatusMap[candidate.id];
        if (status) {
          return status === "needs_profile_completion";
        }
        return candidateAutoReviewStatus(candidate) === "needs_profile_completion";
      }),
    [reviewStatusMap, selectedCandidates],
  );
  const allPagedSelected =
    pagedDisplayCandidates.length > 0 &&
    pagedDisplayCandidates.every((candidate) => selectedCandidates[candidate.id]);
  const deferredCurrentPage = useDeferredValue(currentPage);

  useEffect(() => {
    if (backendPageReady) {
      setLastKnownFilteredCandidateCount(freshFilteredCandidateCount);
    }
  }, [backendPageReady, freshFilteredCandidateCount]);

  useEffect(() => {
    // Do not collapse to a smaller page while a backend page request is
    // pending — last-known totals keep the user's pagination position stable.
    if (waitingForBackendPage) {
      return;
    }
    if (currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages, waitingForBackendPage]);

  useEffect(() => {
    setCurrentPage(1);
    // Filter changed: clear last-known totals so the new filter doesn't
    // inherit stale pagination width. `filterControlsAvailable` is
    // deliberately NOT a trigger: entering/leaving a canonical-summary gap
    // is not a user filter change, and the preserved gap page is bound to
    // its {revision, filterSignature, offset, limit} tuple — the user's
    // page position must survive the gap for that binding to work (FT2
    // fixed-forward r5, rerun4 review finding 4).
    setLastKnownFilteredCandidateCount(0);
  }, [
    keyword,
    selectedRecallBuckets,
    selectedEmploymentStatuses,
    selectedLocations,
    selectedFunctionBuckets,
    selectedAuditStatuses,
    selectedLayerStates,
  ]);

  useEffect(() => {
    if ((!jobId && !resolvedProjectionId) || !backendFilteredPagingSupported) {
      setBackendCandidatePage(null);
      setBackendCandidatePageRequestSignature("");
      setBackendCandidatePageLoading(false);
      return;
    }
    if (preservedFacetIntentDuringGap) {
      // A canonical-summary gap with preserved narrowing intent must never
      // submit a widened backend filter (FT2 fixed-forward r4, rerun3
      // review finding 4). Keep the last same-revision page; the next
      // request fires only after the summary returns and the preserved
      // selection has reconciled against the restored canonical options.
      // The in-flight pre-gap request is cancelled by this effect's cleanup,
      // so its response can never be misread as the kept page (rerun4
      // finding 4) — clear the loading flag it left behind.
      setBackendCandidatePageLoading(false);
      return;
    }
    let cancelled = false;
    setBackendCandidatePageLoading(true);
    setBackendCandidatePageError("");
    const pageRequest = resolvedProjectionId
      ? getProjectionCandidatePage(resolvedProjectionId, {
          offset: backendPageOffset,
          limit: RESULTS_PAGE_SIZE,
          forceRefresh: true,
          filter: backendPageFilter,
        })
      : getDashboardCandidatePage(jobId, {
          offset: backendPageOffset,
          limit: RESULTS_PAGE_SIZE,
          lightweight: true,
          forceRefresh: true,
          filter: backendPageFilter,
        });
    void pageRequest
      .then((page) => {
        if (cancelled) {
          return;
        }
        if (!dashboardCandidatePageRevisionMatches(dashboard, page)) {
          setBackendCandidatePage(null);
          setBackendCandidatePageRequestSignature("");
          return;
        }
        setBackendCandidatePage(page);
        setBackendCandidatePageRequestSignature(backendFilterSignature);
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setBackendCandidatePageError(error instanceof Error ? error.message : "候选人筛选分页加载失败。");
      })
      .finally(() => {
        if (!cancelled) {
          setBackendCandidatePageLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    backendFilteredPagingSupported,
    backendFilterSignature,
    backendPageFilter,
    backendPageOffset,
    dashboard,
    dashboard.boardRuntimeState?.rowPublicationRevision,
    dashboard.boardRuntimeState?.rowPublicationWatermark,
    jobId,
    preservedFacetIntentDuringGap,
    resolvedProjectionId,
  ]);

  useEffect(() => {
    if (!initialCandidateId || appliedContextCandidateRef.current === initialCandidateId) {
      return;
    }
    const candidateIndex = visibleCandidates.findIndex((candidate) => candidate.id === initialCandidateId);
    if (candidateIndex < 0) {
      return;
    }
    appliedContextCandidateRef.current = initialCandidateId;
    setCurrentPage(Math.floor(candidateIndex / RESULTS_PAGE_SIZE) + 1);
    setFocusedCandidateId(initialCandidateId);
  }, [initialCandidateId, visibleCandidates]);

  useEffect(() => {
    const requiredCandidateCount = Math.min(
      expectedCandidateCount,
      Math.max(RESULTS_PAGE_SIZE, deferredCurrentPage * RESULTS_PAGE_SIZE + RESULTS_PAGE_SIZE),
    );
    const backgroundCandidateCount = Math.min(
      expectedCandidateCount,
      Math.max(requiredCandidateCount, deferredCurrentPage * RESULTS_PAGE_SIZE + RESULTS_PAGE_SIZE * 3),
    );
    onHydrationWindowChange?.({
      requiredCandidateCount,
      backgroundCandidateCount,
    });
  }, [deferredCurrentPage, expectedCandidateCount, onHydrationWindowChange]);

  useEffect(() => {
    if (!jobId || resolvedProjectionId || pagedCandidates.length === 0) {
      setCandidateDetailLoadingIds([]);
      return;
    }
    const candidateIds = pagedCandidates.map((candidate) => candidate.id).filter(Boolean);
    const missingCandidateIds = candidateIds.filter((candidateId) => !(candidateId in candidateDetailsById));
    if (missingCandidateIds.length === 0) {
      setCandidateDetailLoadingIds([]);
      return;
    }
    let cancelled = false;
    setCandidateDetailLoadingIds(missingCandidateIds);
    void getCandidateDetailsBatch(missingCandidateIds, jobId)
      .then((details) => {
        if (cancelled) {
          return;
        }
        setCandidateDetailsById((current) => ({
          ...current,
          ...details,
        }));
        setSelectedCandidates((current) => {
          let changed = false;
          const next = { ...current };
          Object.entries(details).forEach(([candidateId, detail]) => {
            if (!detail || !next[candidateId]) {
              return;
            }
            next[candidateId] = {
              ...next[candidateId],
              ...detail,
              id: candidateId,
            };
            changed = true;
          });
          return changed ? next : current;
        });
      })
      .catch(() => undefined)
      .finally(() => {
        if (!cancelled) {
          setCandidateDetailLoadingIds([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [candidateDetailsById, jobId, pagedCandidates, resolvedProjectionId, resultsContextKey]);

  const toggleCandidateSelection = (candidate: Candidate) => {
    setSelectedCandidates((current) => {
      if (current[candidate.id]) {
        const next = { ...current };
        delete next[candidate.id];
        return next;
      }
      return {
        ...current,
        [candidate.id]: candidate,
      };
    });
  };

  const toggleCurrentPageSelection = () => {
    setSelectedCandidates((current) => {
      const next = { ...current };
      if (pagedDisplayCandidates.every((candidate) => next[candidate.id])) {
        pagedDisplayCandidates.forEach((candidate) => {
          delete next[candidate.id];
        });
        return next;
      }
      pagedDisplayCandidates.forEach((candidate) => {
        next[candidate.id] = candidate;
      });
      return next;
    });
  };

  const addSelectedToReview = async () => {
    const candidates = Object.values(selectedCandidates);
    if (projectionOnlyReadOnly) {
      setBatchActionMessage(projectionOnlyReadOnlyMessage);
      setTargetActionCompleted(false);
      return;
    }
    if (candidates.length === 0 || batchActionBusy) {
      return;
    }
    setBatchActionMessage("");
    setTargetActionCompleted(false);
    setBatchActionBusy("review");
    try {
      await Promise.all(
        candidates.map((candidate) => addCandidateToReviewRegistry(jobId, historyId, candidate)),
      );
      onReviewStateChanged?.();
      setSelectedCandidates({});
      setBatchActionMessage(`已将 ${candidates.length} 位候选人加入审核视图。`);
    } catch (error) {
      setBatchActionMessage(error instanceof Error ? error.message : "批量加入审核视图失败。");
    } finally {
      setBatchActionBusy("");
    }
  };

  const addSelectedToTargets = async () => {
    const candidates = Object.values(selectedCandidates);
    if (candidates.length === 0 || batchActionBusy) {
      return;
    }
    setBatchActionMessage("");
    setTargetActionCompleted(false);
    setBatchActionBusy("target");
    try {
      const result = await addTargetCandidates(candidates, {
        projectionId: resolvedProjectionId,
        membershipRevision,
      });
      if (result.failedWriteCount > 0) {
        setBatchActionMessage(
          `目标候选人写入部分完成：成功 ${result.successfulWriteCount}/${result.requestedCandidateCount}，失败 ${result.failedWriteCount}。`,
        );
      } else {
        setSelectedCandidates({});
        setBatchActionMessage(`已将 ${result.successfulWriteCount} 位候选人加入目标候选人。`);
        setTargetActionCompleted(true);
      }
    } catch (error) {
      setBatchActionMessage(error instanceof Error ? error.message : "批量加入目标候选人失败。");
      setTargetActionCompleted(false);
    } finally {
      setBatchActionBusy("");
    }
  };

  const completeSelectedProfiles = async () => {
    if (projectionOnlyReadOnly) {
      setBatchActionMessage(projectionOnlyReadOnlyMessage);
      setTargetActionCompleted(false);
      return;
    }
    if (!jobId || selectedProfileCompletionCandidates.length === 0 || batchActionBusy) {
      return;
    }
    setBatchActionMessage("");
    setTargetActionCompleted(false);
    setBatchActionBusy("profile_completion");
    try {
      await triggerJobCandidateProfileCompletion({
        jobId,
        candidateIds: selectedProfileCompletionCandidates.map((candidate) => candidate.id),
        forceRefresh: true,
      });
      onReviewStateChanged?.();
      setSelectedCandidates({});
      setBatchActionMessage(`已触发 ${selectedProfileCompletionCandidates.length} 位候选人的 LinkedIn 信息补全。`);
    } catch (error) {
      setBatchActionMessage(error instanceof Error ? error.message : "批量补全 LinkedIn 信息失败。");
    } finally {
      setBatchActionBusy("");
    }
  };

  const exportProjectionArchive = async () => {
    if (!projectionMutationReady || batchActionBusy) {
      return;
    }
    setBatchActionMessage("");
    setTargetActionCompleted(false);
    setBatchActionBusy("export");
    try {
      const download = await exportProjectionCandidatesArchive({
        projectionId: resolvedProjectionId,
        expectedMembershipRevision: membershipRevision,
      });
      downloadBlobFile(download.filename || "projection-candidates.zip", download.blob);
      const stats = download.exportStats;
      setBatchActionMessage(
        `Projection 导出完成：${stats.exportedRecordCount}/${stats.recordCount} 位候选人，跳过 ${stats.skippedAssertionCount} 条未默认导出的 assertion。`,
      );
    } catch (error) {
      setBatchActionMessage(error instanceof Error ? error.message : "Projection 导出失败。");
    } finally {
      setBatchActionBusy("");
    }
  };

  const addSingleCandidateToTargets = async (candidate: Candidate) => {
    const identityKeys = [candidate.id, candidate.candidateIdentityKey || "", candidate.personIdentityKey || ""].filter(Boolean);
    if (singleTargetActionCandidateId || identityKeys.some((key) => targetCandidateIds.includes(key))) {
      return;
    }
    setBatchActionMessage("");
    setTargetActionCompleted(false);
    setSingleTargetActionCandidateId(candidate.id);
    setTargetCandidateIds((current) => (current.includes(candidate.id) ? current : [...current, candidate.id]));
    try {
      await addTargetCandidate(candidate, {
        historyId,
        jobId,
        projectionId: resolvedProjectionId,
        membershipRevision,
      });
      setBatchActionMessage(`已将 ${candidate.name} 加入目标候选人。`);
      setTargetActionCompleted(true);
    } catch (error) {
      setTargetCandidateIds((current) => current.filter((item) => item !== candidate.id));
      setBatchActionMessage(error instanceof Error ? error.message : "加入目标候选人失败。");
      setTargetActionCompleted(false);
    } finally {
      setSingleTargetActionCandidateId("");
    }
  };

  return (
    <div className="results-board-panel" data-testid="results-board-panel">
      <section className="panel results-filter-panel">
        <div className="results-filter-topline">
          <div className="results-search results-search-wide">
            <label className="field-label" htmlFor="results-keyword">
              结果内搜索
            </label>
            <input
              id="results-keyword"
              className="text-input"
              value={keyword}
              disabled={!filterControlsAvailable}
              onChange={(event) => setKeyword(event.target.value)}
              placeholder={filterControlsAvailable ? "按姓名、方向、团队、工作经历或教育经历筛选" : "筛选索引准备中"}
            />
          </div>
          <div className="metric-card metric-card-compact candidate-sync-card">
            <span className="candidate-sync-headline">
              <span className="muted">候选人同步</span>
              <strong data-testid="results-visible-count">
                {candidateSyncSummary.syncedCandidateCount}/{candidateSyncSummary.expectedCandidateCount}
              </strong>
            </span>
            {candidateSyncSummary.noteText ? (
              <span className="metric-card-note">{candidateSyncSummary.noteText}</span>
            ) : null}
          </div>
        </div>
        {hydrationBannerVisible ? (
          <p className="muted">
            正在装载已发布候选人行；业务同步状态以后端 board runtime 为准。
          </p>
        ) : null}
        {candidateHydrationError ? <p className="muted">{candidateHydrationError}</p> : null}
        {projectionOnlyReadOnly ? <p className="muted">{projectionOnlyReadOnlyMessage}</p> : null}
        {canonicalFacetUnavailable ? (
          <div className="asset-readiness-notice">
            <strong>筛选索引准备中</strong>
            <span>{canonicalFacetUnavailableMessage}</span>
          </div>
        ) : null}
        {preservedFacetIntentDuringGap ? (
          <div className="asset-readiness-notice" data-testid="facet-gap-preserved-notice">
            <strong>已保留你的筛选设置</strong>
            <span>
              筛选索引暂时不可用；为避免展示未筛选的扩大结果，当前列表
              {gapKeptBackendPage
                ? "保持索引不可用前最近一次同修订的筛选结果"
                : "暂不显示候选人"}
              ，索引恢复后将自动重新应用你的筛选。
            </span>
          </div>
        ) : null}
        {profileDetailsIncomplete ? (
          <div className="asset-readiness-notice">
            <strong>候选人详情待补齐</strong>
            <span>
              当前资产有 {profileDetailCandidateCount}/{expectedCandidateCount} 位候选人具备完整 Profile；
              未补齐前，部分卡片只会显示姓名、职位、教育或 LinkedIn 摘要。
            </span>
          </div>
        ) : null}
        {currentPageDetailLoading ? (
          <p className="muted">
            正在加载当前页候选人的完整卡片信息；筛选、计数与分页继续使用已物化的 canonical 看板行。
          </p>
        ) : null}

        <div className="facet-dropdown-row facet-dropdown-row-wide">
          <LayerTriStateFilter
            options={layerOptions}
            selection={selectedLayerStates}
            disabled={!filterControlsAvailable || !hasLayerMetadata}
            disabledLabel={!filterControlsAvailable ? disabledFilterSummary : layerDisabledLabel}
            onToggle={(optionId) => setSelectedLayerStates((current) => updateLayerSelection(current, optionId))}
          />
          <FacetMultiSelect
            label="召回排序"
            summary={facetSummaryLabel(recallOptions, selectedRecallBuckets, "全量")}
            options={recallOptions}
            selectedIds={selectedRecallBuckets}
            disabled={!filterControlsAvailable}
            disabledSummary={disabledFilterSummary}
            showCounts={hasGlobalFacetSummary}
            emptyMessage={canonicalFacetUnavailable ? canonicalFacetUnavailableMessage : "当前没有召回排序筛选项。"}
            onToggle={(optionId) => {
              userEditedFacetRefs.current.recall = true;
              setSelectedRecallBuckets((current) =>
                toggleFacetSelection(current, optionId, {
                  allId: "all",
                  fallback: defaultRecallSelection(recallOptions),
                }),
              );
            }}
          />
          <FacetMultiSelect
            label="在职状态"
            summary={facetSummaryLabel(employmentFacetOptions, selectedEmploymentStatuses, "在职、已离职")}
            options={employmentFacetOptions}
            selectedIds={selectedEmploymentStatuses}
            disabled={!filterControlsAvailable}
            disabledSummary={disabledFilterSummary}
            showCounts={hasGlobalFacetSummary}
            emptyMessage={canonicalFacetUnavailable ? canonicalFacetUnavailableMessage : "当前没有在职状态筛选项。"}
            onToggle={(optionId) => {
              userEditedFacetRefs.current.employment = true;
              setSelectedEmploymentStatuses((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultOpenSelection(employmentFacetOptions),
                }),
              );
            }}
          />
          <FacetMultiSelect
            label="地区"
            summary={facetSummaryLabel(locationOptions, selectedLocations, "全量")}
            options={locationOptions}
            selectedIds={selectedLocations}
            disabled={!filterControlsAvailable}
            disabledSummary={disabledFilterSummary}
            showCounts={hasGlobalFacetSummary}
            emptyMessage={canonicalFacetUnavailable ? canonicalFacetUnavailableMessage : "当前没有地区筛选项。"}
            onToggle={(optionId) => {
              userEditedFacetRefs.current.locations = true;
              setSelectedLocations((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultOpenSelection(locationOptions),
                }),
              );
            }}
          />
          <FacetMultiSelect
            label="职能"
            summary={facetSummaryLabel(functionOptions, selectedFunctionBuckets, "全量")}
            options={functionOptions}
            selectedIds={selectedFunctionBuckets}
            disabled={!filterControlsAvailable || !functionFacetAvailable}
            disabledSummary={!filterControlsAvailable ? disabledFilterSummary : "统计未生成"}
            showCounts={hasGlobalFacetSummary}
            emptyMessage={canonicalFacetUnavailable ? canonicalFacetUnavailableMessage : "当前没有职能筛选项。"}
            onToggle={(optionId) => {
              userEditedFacetRefs.current.functions = true;
              setSelectedFunctionBuckets((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultOpenSelection(functionOptions),
                }),
              );
            }}
          />
          <FacetMultiSelect
            label="审核状态"
            summary={summarizeSelectedFacet(selectedAuditStatuses, auditStatusOptions, "全部")}
            options={auditStatusOptions}
            selectedIds={selectedAuditStatuses}
            disabled={!filterControlsAvailable}
            disabledSummary={disabledFilterSummary}
            emptyMessage="当前没有审核状态筛选项。"
            onToggle={(optionId) => {
              userEditedFacetRefs.current.audit = true;
              setSelectedAuditStatuses((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultAuditStatusSelection(),
                }),
              );
            }}
          />
        </div>
      </section>

      <section className="panel candidate-panel">
        <div className="panel-header">
          <div>
            <h3>候选人看板</h3>
            <p className="muted">
              看板同步 {candidateSyncSummary.syncedCandidateCount} / {candidateSyncSummary.expectedCandidateCount} 位候选人
              {visibleCandidateCount > 0
                ? `，${visibleCandidateHitLabel} ${visibleCandidateCount} 位，本页显示 ${backendPageOffset + 1}-${Math.min(backendPageOffset + pagedCandidates.length, visibleCandidateCount)} 位`
                : ""}
              {loadedRowWindowText}
            </p>
          </div>
          <div className="results-batch-toolbar">
            <span className="muted">已选 {selectedCandidateIds.length} 位</span>
            <button type="button" className="ghost-button small-button" onClick={toggleCurrentPageSelection}>
              {allPagedSelected ? "取消本页全选" : "选择本页全部"}
            </button>
            <button
              type="button"
              className="ghost-button small-button"
              onClick={() => setSelectedCandidates({})}
              disabled={selectedCandidateIds.length === 0}
            >
              清空已选
            </button>
            <button
              type="button"
              className="ghost-button small-button"
              onClick={() => {
                void completeSelectedProfiles();
              }}
              disabled={projectionOnlyReadOnly || selectedProfileCompletionCandidates.length === 0 || batchActionBusy !== ""}
            >
              {batchActionBusy === "profile_completion"
                ? "Web Search补全中..."
                : "Web Search批量补全LinkedIn信息"}
            </button>
            <button
              type="button"
              className="ghost-button small-button"
              onClick={() => {
                void addSelectedToReview();
              }}
              disabled={projectionOnlyReadOnly || selectedCandidateIds.length === 0 || batchActionBusy !== ""}
            >
              {batchActionBusy === "review" ? "加入审核视图中..." : "批量加入审核视图"}
            </button>
            <button
              type="button"
              className="ghost-button small-button"
              onClick={() => {
                void addSelectedToTargets();
              }}
              disabled={!projectionMutationReady || selectedCandidateIds.length === 0 || batchActionBusy !== ""}
            >
              {batchActionBusy === "target" ? "加入目标候选人中..." : "批量加入目标候选人"}
            </button>
            {resolvedProjectionId ? (
              <button
                type="button"
                className="ghost-button small-button"
                onClick={() => {
                  void exportProjectionArchive();
                }}
                disabled={!projectionMutationReady || batchActionBusy !== ""}
              >
                {batchActionBusy === "export" ? "正在导出..." : "导出 Projection"}
              </button>
            ) : null}
          </div>
        </div>
        {batchActionMessage ? (
          <p className="muted results-batch-message">
            <span>{batchActionMessage}</span>
            {targetActionCompleted ? (
              <Link className="link-chip small-button" to={targetCandidatesRoute}>
                查看目标候选人
              </Link>
            ) : null}
          </p>
        ) : null}
        {backendCandidatePageLoading && backendFilteredPagingSupported && pagedDisplayCandidates.length === 0 ? (
          <p className="muted">正在按后端 canonical 筛选条件装载当前页。</p>
        ) : null}
        {backendCandidatePageError ? <p className="form-error">{backendCandidatePageError}</p> : null}

        {pagedDisplayCandidates.length > 0 ? (
          <>
            <div className="results-group-stack">
              {pagedCandidateGroups.map((group) => (
                <section key={group.status} className="results-status-group">
                  <div className="results-status-group-header">
                    <h4>{group.label}</h4>
                    <span className="muted">
                      当前页 {group.candidates.length} 位
                      {backendPageReady ? "" : `，筛选结果共 ${
                        visibleCandidates.filter(
                          (candidate) =>
                            (reviewStatusMap[candidate.id] || candidateAutoReviewStatus(candidate) || "no_review_needed") ===
                            group.status,
                        ).length
                      } 位`}
                    </span>
                  </div>
                  <div className="candidate-board-grid results-candidate-grid">
                    {group.candidates.map((candidate) => {
                      const workExperience = getFormattedWorkExperience(candidate, 4);
                      const educationExperience = getFormattedEducationExperience(candidate, 3);
                      const displayKeywords = pickCandidateKeywords(candidate, dashboard.intentKeywords);
                      const manualReviewRoute = buildWorkflowRoute("/manual-review", {
                        historyId,
                        jobId,
                        candidateId: candidate.id,
                      });
                      const reviewStatus = reviewStatusMap[candidate.id] || "no_review_needed";
                      const email = extractPrimaryEmail(candidate);
                      const emailMetadata = candidate.primaryEmailMetadata;
                      const linkedinUrl = resolveCandidateLinkedinUrl(candidate);
                      const isFocused = focusedCandidateId === candidate.id;
                      const isTargetCandidate = [candidate.id, candidate.candidateIdentityKey || "", candidate.personIdentityKey || ""]
                        .filter(Boolean)
                        .some((key) => targetCandidateIds.includes(key));
                      const isSelected = Boolean(selectedCandidates[candidate.id]);
                      const detailLoading = candidateDetailLoadingIds.includes(candidate.id);

                      return (
                        <article
                          key={candidate.id}
                          className={`candidate-card result-candidate-card${isFocused ? " selected-card" : ""}${isSelected ? " batch-selected-card" : ""}`}
                          data-testid="results-candidate-card"
                          onClick={() => {
                            setFocusedCandidateId(candidate.id);
                            onSelectedCandidateChange?.(candidate.id);
                          }}
                        >
                          <div className="candidate-card-top-block">
                            <div className="candidate-card-selection-row">
                              <label className="candidate-select-toggle" onClick={(event) => event.stopPropagation()}>
                                <input
                                  type="checkbox"
                                  checked={isSelected}
                                  onChange={() => toggleCandidateSelection(candidate)}
                                />
                                <span>{isSelected ? "已选中" : "选择候选人"}</span>
                              </label>
                            </div>
                            <div className="candidate-header candidate-header-with-avatar">
                              <Avatar name={candidate.name} src={candidate.avatarUrl} size="small" />
                              <div className="candidate-copy">
                                <div className="candidate-title-row">
                                  <div>
                                    <h4>{candidate.name}</h4>
                                    <p className="candidate-meta-line candidate-headline-scroll">
                                      {pickCandidateRoleLine(candidate)}
                                    </p>
                                    {email ? (
                                      <div className="candidate-email-line candidate-email-line-with-help">
                                        <span>{email}</span>
                                        {emailMetadata ? (
                                          <span className="help-badge" aria-label="查看邮箱来源说明">
                                            ?
                                            <span className="help-tooltip">
                                              <span>来源: {emailMetadata.source || "unknown"}</span>
                                              <span>状态: {emailMetadata.status || "unknown"}</span>
                                              <span>
                                                质量分:{" "}
                                                {typeof emailMetadata.qualityScore === "number"
                                                  ? emailMetadata.qualityScore
                                                  : "unknown"}
                                              </span>
                                              <span>
                                                LinkedIn Profile 标记:{" "}
                                                {emailMetadata.foundInLinkedInProfile === true
                                                  ? "是"
                                                  : emailMetadata.foundInLinkedInProfile === false
                                                    ? "否"
                                                    : "未提供"}
                                              </span>
                                            </span>
                                          </span>
                                        ) : null}
                                      </div>
                                    ) : null}
                                  </div>
                                </div>
                              </div>
                            </div>

                            <div className="candidate-keywords">
                              <span className="keyword-chip">{layerLabel(candidate.outreachLayer)}</span>
                              <span className="keyword-chip">{employmentStatusLabel(candidate.employmentStatus)}</span>
                              {reviewStatus !== "no_review_needed" ? (
                                <span className="keyword-chip">{reviewStatusLabel(reviewStatus)}</span>
                              ) : null}
                              {displayKeywords.map((keywordItem) => (
                                <span key={`${candidate.id}-${keywordItem}`} className="keyword-chip">
                                  {keywordItem}
                                </span>
                              ))}
                            </div>
                          </div>

                          <div className="candidate-card-content">
                            <div className="candidate-experience-grid">
                              <div className="candidate-experience-section">
                                <h5>工作经历</h5>
                                <div className="candidate-experience-scroll">
                                  {workExperience.length > 0 ? (
                                    <ul className="flat-list compact experience-preview-list">
                                      {workExperience.map((line) => (
                                        <li key={`${candidate.id}-work-${line}`}>{sanitizeCandidateText(line)}</li>
                                      ))}
                                    </ul>
                                  ) : detailLoading ? (
                                    <p>正在加载当前页完整工作经历。</p>
                                  ) : (
                                    <p>暂无可结构化提取的工作经历。</p>
                                  )}
                                </div>
                              </div>
                              <div className="candidate-experience-section">
                                <h5>教育经历</h5>
                                <div className="candidate-experience-scroll">
                                  {educationExperience.length > 0 ? (
                                    <ul className="flat-list compact experience-preview-list">
                                      {educationExperience.map((line) => (
                                        <li key={`${candidate.id}-edu-${line}`}>{sanitizeCandidateText(line)}</li>
                                      ))}
                                    </ul>
                                  ) : detailLoading ? (
                                    <p>正在加载当前页完整教育经历。</p>
                                  ) : (
                                    <p>暂无可结构化提取的教育经历。</p>
                                  )}
                                </div>
                              </div>
                            </div>
                          </div>

                          <div className="candidate-actions candidate-actions-compact candidate-actions-bottom">
                            {linkedinUrl ? (
                              <a
                                className="ghost-button candidate-action-button"
                                href={linkedinUrl}
                                target="_blank"
                                rel="noreferrer"
                                onClick={(event) => event.stopPropagation()}
                              >
                                <span className="linkedin-glyph" aria-hidden="true">in</span>
                                <span>打开 LinkedIn</span>
                              </a>
                            ) : null}
                            {onOpenManualReview ? (
                              <button
                                type="button"
                                className="ghost-button candidate-action-button"
                                disabled={projectionOnlyReadOnly}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  if (projectionOnlyReadOnly) {
                                    setBatchActionMessage(projectionOnlyReadOnlyMessage);
                                    return;
                                  }
                                  void addCandidateToReviewRegistry(jobId, historyId, candidate)
                                    .then(() => {
                                      onReviewStateChanged?.();
                                      onSelectedCandidateChange?.(candidate.id);
                                      onOpenManualReview(candidate.id);
                                    })
                                    .catch(() => undefined);
                                }}
                              >
                                加入审核视图
                              </button>
                            ) : projectionOnlyReadOnly ? (
                              <button
                                type="button"
                                className="ghost-button candidate-action-button"
                                disabled
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setBatchActionMessage(projectionOnlyReadOnlyMessage);
                                }}
                              >
                                加入审核视图
                              </button>
                            ) : (
                              <Link
                                className="ghost-button candidate-action-button"
                                to={manualReviewRoute}
                                onClick={() => {
                                  onSelectedCandidateChange?.(candidate.id);
                                  void addCandidateToReviewRegistry(jobId, historyId, candidate).catch(() => undefined);
                                }}
                              >
                                加入审核视图
                              </Link>
                            )}
                            <button
                              type="button"
                              className="ghost-button candidate-action-button"
                              onClick={(event) => {
                                event.stopPropagation();
                                void addSingleCandidateToTargets(candidate);
                              }}
                              disabled={
                                !projectionMutationReady ||
                                isTargetCandidate ||
                                singleTargetActionCandidateId === candidate.id
                              }
                            >
                              {isTargetCandidate
                                ? "已加入目标候选人"
                                : singleTargetActionCandidateId === candidate.id
                                  ? "加入中..."
                                  : "加入目标候选人"}
                            </button>
                          </div>
                        </article>
                      );
                    })}
                  </div>
                </section>
              ))}
            </div>

            {totalPages > 1 ? (
              <div className="results-pager">
                <button
                  type="button"
                  className="ghost-button"
                  onClick={() => setCurrentPage((page) => Math.max(1, page - 1))}
                  disabled={currentPage <= 1}
                >
                  上一页
                </button>
                <span className="muted">
                  第 {currentPage} / {totalPages} 页
                </span>
                <button
                  type="button"
                  className="ghost-button"
                  onClick={() => setCurrentPage((page) => Math.min(totalPages, page + 1))}
                  disabled={currentPage >= totalPages}
                >
                  下一页
                </button>
              </div>
            ) : null}
          </>
        ) : (
          <div className="empty-state">
            <p>
              {waitingForBackendPage
                ? "正在装载当前筛选条件下的候选人。"
                : hydrationBannerVisible
                  ? "已加载候选片段中暂时没有筛选命中。"
                  : "当前筛选条件下没有候选人。"}
            </p>
            <span>
              {waitingForBackendPage
                ? "筛选与分页由后端 canonical 候选人集合计算。"
                : hydrationBannerVisible
                  ? "系统正在继续装载剩余候选人；这不是最终空结果。"
                  : "可以放宽筛选条件后重试。"}
            </span>
          </div>
        )}
      </section>
    </div>
  );
}
