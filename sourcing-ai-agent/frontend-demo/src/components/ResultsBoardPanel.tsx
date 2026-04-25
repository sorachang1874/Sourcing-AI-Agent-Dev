import { useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Avatar } from "./Avatar";
import { FacetMultiSelect } from "./FacetMultiSelect";
import {
  type CandidateFacetOption,
  buildEmploymentOptions,
  buildFunctionOptions,
  buildLocationOptions,
  buildRecallBucketOptions,
  computeCandidateIntentKeywordHits,
  defaultEmploymentSelection,
  defaultFunctionSelection,
  defaultLocationSelection,
  defaultRecallSelection,
  filterCandidatesByFacets,
  normalizeFacetSelection,
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
import { addTargetCandidate, readTargetCandidates, targetCandidatesUpdatedEventName } from "../lib/targetCandidatesStore";
import { getFormattedEducationExperience, getFormattedWorkExperience } from "../lib/profileFormatting";
import { triggerJobCandidateProfileCompletion } from "../lib/api";
import { buildWorkflowRoute } from "../lib/workflowContext";
import type {
  Candidate,
  CandidateReviewStatus,
  DashboardData,
} from "../types";

interface ResultsBoardPanelProps {
  dashboard: DashboardData;
  historyId?: string;
  jobId?: string;
  initialCandidateId?: string;
  isHydratingCandidates?: boolean;
  candidateHydrationError?: string;
  totalCandidateCount?: number;
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

function layerLabel(layer: number): string {
  return `Layer ${layer}`;
}

function pickCandidateKeywords(candidate: Candidate, intentKeywords: string[]): string[] {
  return computeCandidateIntentKeywordHits(candidate, intentKeywords).slice(0, 4);
}

function buildLayerOptionsFromCandidates(candidates: Candidate[]): CandidateFacetOption[] {
  const counts = candidates.reduce<Record<string, number>>((accumulator, candidate) => {
    const key = `layer_${candidate.outreachLayer}`;
    accumulator[key] = (accumulator[key] || 0) + 1;
    return accumulator;
  }, {});
  return LAYER_DEFINITIONS.map((item) => ({
    id: item.id,
    label: item.label,
    count: item.id === "layer_0" ? candidates.length : counts[item.id] || 0,
  }));
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
  const exactLayerId = `layer_${candidate.outreachLayer}`;
  const excluded = Object.entries(selection)
    .filter(([, state]) => state === "exclude")
    .map(([id]) => id);
  if (excluded.includes(exactLayerId)) {
    return false;
  }
  const includedExactLayers = Object.entries(selection)
    .filter(([id, state]) => id !== "layer_0" && state === "include")
    .map(([id]) => id);
  if (includedExactLayers.length > 0) {
    return includedExactLayers.includes(exactLayerId);
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
    const key = reviewStatusMap[candidate.id] || "no_review_needed";
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
    selectedAuditStatuses.includes(reviewStatusMap[candidate.id] || "no_review_needed"),
  );
}

function groupCandidatesByAuditStatus(
  candidates: Candidate[],
  reviewStatusMap: Record<string, CandidateReviewStatus>,
): Array<{ status: CandidateReviewStatus; label: string; candidates: Candidate[] }> {
  const labelsByStatus = new Map(AUDIT_STATUS_OPTIONS.map((option) => [option.id, option.label] as const));
  const groups = new Map<CandidateReviewStatus, { status: CandidateReviewStatus; label: string; candidates: Candidate[] }>();
  candidates.forEach((candidate) => {
    const status = reviewStatusMap[candidate.id] || "no_review_needed";
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
}: {
  options: CandidateFacetOption[];
  selection: Record<string, LayerSelectionState>;
  onToggle: (optionId: string) => void;
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
        <strong>{summarizeLayerSelection(selection, options)}</strong>
      </summary>
      <div className="facet-dropdown-menu">
        {options.map((option) => {
          const state = selection[option.id] || "neutral";
          return (
            <button
              key={option.id}
              type="button"
              className={`layer-tristate-option state-${state}`}
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
  initialCandidateId = "",
  isHydratingCandidates = false,
  candidateHydrationError = "",
  totalCandidateCount = 0,
  reviewStatusMap = {},
  onSelectedCandidateChange,
  onOpenManualReview,
  onReviewStateChanged,
  onHydrationWindowChange,
}: ResultsBoardPanelProps) {
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
  const [batchActionBusy, setBatchActionBusy] = useState<"" | "review" | "profile_completion" | "target">("");
  const [batchActionMessage, setBatchActionMessage] = useState("");
  const [singleTargetActionCandidateId, setSingleTargetActionCandidateId] = useState("");
  const appliedContextCandidateRef = useRef("");
  const resultsContextKey = useMemo(
    () => [historyId || "no-history", jobId || "no-job", dashboard.snapshotId || "no-snapshot"].join(":"),
    [dashboard.snapshotId, historyId, jobId],
  );

  const layerOptions = useMemo(() => buildLayerOptionsFromCandidates(dashboard.candidates), [dashboard.candidates]);
  const recallOptions = useMemo(
    () => buildRecallBucketOptions(dashboard.candidates, dashboard.intentKeywords),
    [dashboard.candidates, dashboard.intentKeywords],
  );
  const employmentOptions = useMemo(() => buildEmploymentOptions(dashboard.candidates), [dashboard.candidates]);
  const locationOptions = useMemo(() => buildLocationOptions(dashboard.candidates), [dashboard.candidates]);
  const functionOptions = useMemo(() => buildFunctionOptions(dashboard.candidates), [dashboard.candidates]);
  const auditStatusOptions = useMemo(
    () => buildAuditStatusOptions(dashboard.candidates, reviewStatusMap),
    [dashboard.candidates, reviewStatusMap],
  );

  useEffect(() => {
    appliedContextCandidateRef.current = "";
    setKeyword("");
    setFocusedCandidateId("");
    setSelectedLayerStates(defaultLayerSelectionStates());
    setSelectedRecallBuckets(defaultRecallSelection(recallOptions));
    setSelectedEmploymentStatuses(defaultEmploymentSelection(employmentOptions));
    setSelectedLocations(defaultLocationSelection(locationOptions));
    setSelectedFunctionBuckets(defaultFunctionSelection(functionOptions));
    setSelectedAuditStatuses(defaultAuditStatusSelection());
    setCurrentPage(1);
    setSelectedCandidates({});
    setBatchActionMessage("");
    setSingleTargetActionCandidateId("");
  }, [resultsContextKey]);

  useEffect(() => {
    setSelectedRecallBuckets((current) => {
      const normalized = normalizeFacetSelection(current, recallOptions, defaultRecallSelection(recallOptions));
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [recallOptions]);

  useEffect(() => {
    setSelectedEmploymentStatuses((current) => {
      const normalized = normalizeFacetSelection(
        current,
        employmentOptions,
        defaultEmploymentSelection(employmentOptions),
      );
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [employmentOptions]);

  useEffect(() => {
    setSelectedLocations((current) => {
      const normalized = normalizeFacetSelection(current, locationOptions, defaultLocationSelection(locationOptions));
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [locationOptions]);

  useEffect(() => {
    setSelectedFunctionBuckets((current) => {
      const normalized = normalizeFacetSelection(current, functionOptions, defaultFunctionSelection(functionOptions));
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [functionOptions]);

  useEffect(() => {
    setSelectedAuditStatuses((current) => {
      const normalized = normalizeFacetSelection(current, auditStatusOptions, defaultAuditStatusSelection());
      return sameStringArray(current, normalized) ? current : normalized;
    });
  }, [auditStatusOptions]);

  useEffect(() => {
    const syncTargets = () => {
      void readTargetCandidates()
        .then((records) => {
          setTargetCandidateIds(records.map((record) => record.candidateId));
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

  const baseVisibleCandidates = useMemo(
    () =>
      filterCandidatesByFacets(
        dashboard.candidates,
        {
          layers: [],
          recallBuckets: selectedRecallBuckets,
          employmentStatuses: selectedEmploymentStatuses,
          locations: selectedLocations,
          functionBuckets: selectedFunctionBuckets,
          searchKeyword: keyword,
        },
        dashboard.intentKeywords,
      ),
    [
      dashboard.candidates,
      dashboard.intentKeywords,
      keyword,
      selectedFunctionBuckets,
      selectedEmploymentStatuses,
      selectedLocations,
      selectedRecallBuckets,
    ],
  );

  const visibleCandidates = useMemo(() => {
    const afterAudit = filterByAuditStatus(baseVisibleCandidates, selectedAuditStatuses, reviewStatusMap);
    return afterAudit.filter((candidate) => matchesLayerSelection(candidate, selectedLayerStates));
  }, [baseVisibleCandidates, reviewStatusMap, selectedAuditStatuses, selectedLayerStates]);

  const totalPages = Math.max(1, Math.ceil(visibleCandidates.length / RESULTS_PAGE_SIZE));
  const pagedCandidates = useMemo(() => {
    const start = Math.max(0, (currentPage - 1) * RESULTS_PAGE_SIZE);
    return visibleCandidates.slice(start, start + RESULTS_PAGE_SIZE);
  }, [currentPage, visibleCandidates]);
  const pagedCandidateGroups = useMemo(
    () => groupCandidatesByAuditStatus(pagedCandidates, reviewStatusMap),
    [pagedCandidates, reviewStatusMap],
  );
  const selectedCandidateIds = useMemo(() => Object.keys(selectedCandidates), [selectedCandidates]);
  const expectedCandidateCount = Math.max(totalCandidateCount, dashboard.totalCandidates, dashboard.candidates.length);
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
  const allPagedSelected = pagedCandidates.length > 0 && pagedCandidates.every((candidate) => selectedCandidates[candidate.id]);
  const deferredCurrentPage = useDeferredValue(currentPage);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages]);

  useEffect(() => {
    setCurrentPage(1);
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
      if (pagedCandidates.every((candidate) => next[candidate.id])) {
        pagedCandidates.forEach((candidate) => {
          delete next[candidate.id];
        });
        return next;
      }
      pagedCandidates.forEach((candidate) => {
        next[candidate.id] = candidate;
      });
      return next;
    });
  };

  const addSelectedToReview = async () => {
    const candidates = Object.values(selectedCandidates);
    if (candidates.length === 0 || batchActionBusy) {
      return;
    }
    setBatchActionMessage("");
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
    setBatchActionBusy("target");
    try {
      await Promise.all(
        candidates.map((candidate) => addTargetCandidate(candidate, { historyId, jobId })),
      );
      setSelectedCandidates({});
      setBatchActionMessage(`已将 ${candidates.length} 位候选人加入目标候选人。`);
    } catch (error) {
      setBatchActionMessage(error instanceof Error ? error.message : "批量加入目标候选人失败。");
    } finally {
      setBatchActionBusy("");
    }
  };

  const completeSelectedProfiles = async () => {
    if (!jobId || selectedProfileCompletionCandidates.length === 0 || batchActionBusy) {
      return;
    }
    setBatchActionMessage("");
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

  const addSingleCandidateToTargets = async (candidate: Candidate) => {
    if (singleTargetActionCandidateId || targetCandidateIds.includes(candidate.id)) {
      return;
    }
    setBatchActionMessage("");
    setSingleTargetActionCandidateId(candidate.id);
    setTargetCandidateIds((current) => (current.includes(candidate.id) ? current : [...current, candidate.id]));
    try {
      await addTargetCandidate(candidate, { historyId, jobId });
      setBatchActionMessage(`已将 ${candidate.name} 加入目标候选人。`);
    } catch (error) {
      setTargetCandidateIds((current) => current.filter((item) => item !== candidate.id));
      setBatchActionMessage(error instanceof Error ? error.message : "加入目标候选人失败。");
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
              onChange={(event) => setKeyword(event.target.value)}
              placeholder="按姓名、方向、团队、工作经历或教育经历筛选"
            />
          </div>
          <div className="metric-card metric-card-compact">
            <span className="muted">候选人同步</span>
            <strong data-testid="results-visible-count">
              {dashboard.candidates.length}/{expectedCandidateCount}
            </strong>
          </div>
        </div>
        {isHydratingCandidates ? (
          <p className="muted">正在分块同步候选人，筛选和分页会随已加载数据继续扩展。</p>
        ) : null}
        {candidateHydrationError ? <p className="muted">{candidateHydrationError}</p> : null}

        <div className="facet-dropdown-row facet-dropdown-row-wide">
          <LayerTriStateFilter
            options={layerOptions}
            selection={selectedLayerStates}
            onToggle={(optionId) => setSelectedLayerStates((current) => updateLayerSelection(current, optionId))}
          />
          <FacetMultiSelect
            label="召回排序"
            summary={summarizeSelectedFacet(selectedRecallBuckets, recallOptions, "全量")}
            options={recallOptions}
            selectedIds={selectedRecallBuckets}
            onToggle={(optionId) =>
              setSelectedRecallBuckets((current) =>
                toggleFacetSelection(current, optionId, {
                  allId: "all",
                  fallback: defaultRecallSelection(recallOptions),
                }),
              )
            }
          />
          <FacetMultiSelect
            label="在职状态"
            summary={summarizeSelectedFacet(selectedEmploymentStatuses, employmentOptions, "在职、已离职")}
            options={employmentOptions}
            selectedIds={selectedEmploymentStatuses}
            onToggle={(optionId) =>
              setSelectedEmploymentStatuses((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultEmploymentSelection(employmentOptions),
                }),
              )
            }
          />
          <FacetMultiSelect
            label="地区"
            summary={summarizeSelectedFacet(selectedLocations, locationOptions, "美国")}
            options={locationOptions}
            selectedIds={selectedLocations}
            onToggle={(optionId) =>
              setSelectedLocations((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultLocationSelection(locationOptions),
                }),
              )
            }
          />
          <FacetMultiSelect
            label="职能"
            summary={summarizeSelectedFacet(selectedFunctionBuckets, functionOptions, "Researcher、Engineer")}
            options={functionOptions}
            selectedIds={selectedFunctionBuckets}
            onToggle={(optionId) =>
              setSelectedFunctionBuckets((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultFunctionSelection(functionOptions),
                }),
              )
            }
          />
          <FacetMultiSelect
            label="审核状态"
            summary={summarizeSelectedFacet(selectedAuditStatuses, auditStatusOptions, "全部")}
            options={auditStatusOptions}
            selectedIds={selectedAuditStatuses}
            onToggle={(optionId) =>
              setSelectedAuditStatuses((current) =>
                toggleFacetSelection(current, optionId, {
                  fallback: defaultAuditStatusSelection(),
                }),
              )
            }
          />
        </div>
      </section>

      <section className="panel candidate-panel">
        <div className="panel-header">
          <div>
            <h3>候选人看板</h3>
            <p className="muted">
              已加载 {dashboard.candidates.length} / {expectedCandidateCount} 位候选人
              {visibleCandidates.length > 0
                ? `，当前筛选命中 ${visibleCandidates.length} 位，本页显示 ${(currentPage - 1) * RESULTS_PAGE_SIZE + 1}-${Math.min(currentPage * RESULTS_PAGE_SIZE, visibleCandidates.length)} 位`
                : ""}
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
              disabled={selectedProfileCompletionCandidates.length === 0 || batchActionBusy !== ""}
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
              disabled={selectedCandidateIds.length === 0 || batchActionBusy !== ""}
            >
              {batchActionBusy === "review" ? "加入审核视图中..." : "批量加入审核视图"}
            </button>
            <button
              type="button"
              className="ghost-button small-button"
              onClick={() => {
                void addSelectedToTargets();
              }}
              disabled={selectedCandidateIds.length === 0 || batchActionBusy !== ""}
            >
              {batchActionBusy === "target" ? "加入目标候选人中..." : "批量加入目标候选人"}
            </button>
          </div>
        </div>
        {batchActionMessage ? <p className="muted">{batchActionMessage}</p> : null}

        {visibleCandidates.length > 0 ? (
          <>
            <div className="results-group-stack">
              {pagedCandidateGroups.map((group) => (
                <section key={group.status} className="results-status-group">
                  <div className="results-status-group-header">
                    <h4>{group.label}</h4>
                    <span className="muted">
                      当前页 {group.candidates.length} 位，筛选结果共{" "}
                      {
                        visibleCandidates.filter(
                          (candidate) => (reviewStatusMap[candidate.id] || "no_review_needed") === group.status,
                        ).length
                      }{" "}
                      位
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
                      const isTargetCandidate = targetCandidateIds.includes(candidate.id);
                      const isSelected = Boolean(selectedCandidates[candidate.id]);

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
                                    <p className="candidate-meta-line">{pickCandidateRoleLine(candidate)}</p>
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
                                onClick={(event) => {
                                  event.stopPropagation();
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
                              disabled={singleTargetActionCandidateId === candidate.id}
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
            <p>{isHydratingCandidates ? "当前已加载片段中还没有命中候选人。" : "当前筛选条件下没有候选人。"}</p>
            <span>{isHydratingCandidates ? "系统正在继续同步剩余候选人，可以稍后再看。" : "可以放宽筛选条件后重试。"}</span>
          </div>
        )}
      </section>
    </div>
  );
}
