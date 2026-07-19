import type { Candidate, DashboardData } from "../types";

export const CURRENT_EXCEL_INTAKE_RECALL_BUCKET_ID = "job_scoped_marker:excel_intake:current_job";
const CURRENT_EXCEL_INTAKE_RECALL_LABEL = "本次Excel导入";

export interface CandidateFacetOption {
  id: string;
  label: string;
  count: number;
}

export interface CandidateFacetSelection {
  layers: string[];
  recallBuckets: string[];
  employmentStatuses: string[];
  locations: string[];
  functionBuckets: string[];
  searchKeyword: string;
}

function normalizeToken(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function normalizeSearchText(value: string): string {
  return value
    .toLowerCase()
    .replace(/[_/]+/g, " ")
    .replace(/[\-–—]+/g, " ")
    .replace(/[^\p{L}\p{N}\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function keywordSelectionId(keyword: string): string {
  return `keyword:${normalizeToken(keyword)}`;
}

function candidateKeywordCorpus(candidate: Candidate): string {
  return normalizeSearchText(
    [
      candidate.name,
      candidate.headline,
      candidate.summary,
      candidate.currentCompany || "",
      candidate.notesSnippet || "",
      candidate.team,
      ...candidate.focusAreas,
      ...candidate.matchReasons,
      ...candidate.education,
      ...candidate.experience,
      ...candidate.matchedKeywords,
    ].join(" "),
  );
}

function keywordMatchVariants(keyword: string): string[] {
  const normalized = normalizeSearchText(keyword);
  const variants = new Set<string>([normalized]);
  const aliasMap: Record<string, string[]> = {
    coding: ["coding", "programming", "code generation", "software development"],
    math: ["math", "mathematics", "mathematical"],
    text: ["text", "nlp", "natural language", "language model", "language models"],
    audio: ["audio", "speech", "voice"],
    vision: ["vision", "visual", "computer vision"],
    multimodal: ["multimodal", "multi modal", "multimodality"],
    reasoning: ["reasoning", "reasoning model", "reasoning models"],
    "pre train": ["pre train", "pre-train", "pretraining", "pre training"],
    "post train": ["post train", "post-train", "posttraining", "post training"],
    "world model": ["world model", "world models"],
  };
  for (const alias of aliasMap[normalized] || []) {
    variants.add(normalizeSearchText(alias));
  }
  return Array.from(variants).filter(Boolean);
}

function matchesAsciiKeyword(corpus: string, variant: string): boolean {
  if (!/[a-z0-9]/i.test(variant)) {
    return corpus.includes(variant);
  }
  const escaped = variant.split(" ").filter(Boolean).map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("\\s+");
  if (!escaped) {
    return false;
  }
  return new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, "i").test(corpus);
}

function candidateMatchesIntentKeyword(candidate: Candidate, keyword: string): boolean {
  const corpus = candidateKeywordCorpus(candidate);
  return keywordMatchVariants(keyword).some((variant) => {
    if (!variant) {
      return false;
    }
    if (/[a-z0-9]/i.test(variant)) {
      return matchesAsciiKeyword(corpus, variant);
    }
    return corpus.includes(variant);
  });
}

function candidateMatchesIntentKeywordProvenance(candidate: Candidate, keyword: string): boolean {
  const corpus = normalizeSearchText(candidate.matchedKeywords.join(" "));
  if (!corpus) {
    return false;
  }
  return keywordMatchVariants(keyword).some((variant) => {
    if (!variant) {
      return false;
    }
    if (/[a-z0-9]/i.test(variant)) {
      return matchesAsciiKeyword(corpus, variant);
    }
    return corpus.includes(variant);
  });
}

function recallProvenanceKeywordSet(candidates: Candidate[], intentKeywords: string[]): Set<string> {
  return new Set(
    intentKeywords.filter((keyword) =>
      candidates.some((candidate) => candidateMatchesIntentKeywordProvenance(candidate, keyword)),
    ),
  );
}

function candidateMatchesRecallKeyword(
  candidate: Candidate,
  keyword: string,
  provenanceKeywords: Set<string>,
): boolean {
  return provenanceKeywords.has(keyword)
    ? candidateMatchesIntentKeywordProvenance(candidate, keyword)
    : candidateMatchesIntentKeyword(candidate, keyword);
}

function candidateMatchesCurrentExcelIntake(candidate: Candidate): boolean {
  const normalizedLabel = normalizeSearchText(CURRENT_EXCEL_INTAKE_RECALL_LABEL);
  if (candidate.matchedKeywords.some((keyword) => normalizeSearchText(keyword) === normalizedLabel)) {
    return true;
  }
  return (candidate.sourceMatches || []).some((source) => {
    const sourceType = normalizeToken(String(source.source_type || source.marker_id || ""));
    const matchedOn = normalizeSearchText(String(source.matched_on || ""));
    return (
      sourceType === "excel_intake:current_job" ||
      (String(source.field || "") === "job_scoped_candidate_marker" && matchedOn === normalizedLabel)
    );
  });
}

function normalizeEmploymentStatus(value: Candidate["employmentStatus"]): string {
  return value === "current" || value === "former" ? value : "lead";
}

function normalizeLocationText(value: string): string {
  return normalizeSearchText(value);
}

function candidateLocationBucket(candidate: Candidate): string {
  const normalized = normalizeLocationText(candidate.location || "");
  if (!normalized) {
    return "unknown";
  }
  const nonUsSignals = [
    "china",
    "beijing",
    "shanghai",
    "shenzhen",
    "guangzhou",
    "hong kong",
    "taiwan",
    "singapore",
    "tokyo",
    "japan",
    "seoul",
    "korea",
    "london",
    "united kingdom",
    "england",
    "paris",
    "france",
    "berlin",
    "germany",
    "toronto",
    "vancouver",
    "canada",
    "zurich",
    "switzerland",
    "sydney",
    "australia",
    "india",
    "bangalore",
    "bengaluru",
  ];
  if (nonUsSignals.some((signal) => normalized.includes(signal))) {
    return "other";
  }
  return "us";
}

/**
 * Function facet membership is SERVER-computed (FT0 §5.1/§5.2): the frontend
 * consumes the per-candidate `functionBucketIds` verbatim (no case/whitespace
 * normalization) and must not carry a second local taxonomy. The deleted
 * local maps (ROLE_BUCKET_TO_FUNCTION_BUCKET / FUNCTION_BUCKET_KEYWORDS /
 * numeric function-id mapping / local option enum) may not return.
 *
 * Fail-visible posture (FT2 fixed-forward, review finding 7): a candidate
 * whose server pair is absent or incomplete has NO facet membership — the
 * function facet is unavailable for that row (null), and it is never
 * repaired into a synthesized `unknown` bucket. `unknown` membership exists
 * only when the backend explicitly emitted it with valid provenance.
 */
function candidateFunctionBuckets(candidate: Candidate): string[] | null {
  if (candidate.functionBucketIds === undefined || !candidate.functionBucketSource) {
    return null;
  }
  return candidate.functionBucketIds;
}

/**
 * Employment membership truth (FT0 §6): for Cohort-produced candidates the
 * server-owned `cohortEmploymentStatuses` set is the ONLY authoritative
 * membership — a dual-status candidate matches BOTH `current` and `former`
 * while its top-level display status stays single-valued. Legacy records
 * carry no membership and keep the documented display-status compatibility
 * semantic (`lead` matches only when both statuses are selected).
 */
function candidateEmploymentMembership(candidate: Candidate): string[] {
  const membership = (candidate.cohortEmploymentStatuses || []).filter(
    (status): status is "current" | "former" => status === "current" || status === "former",
  );
  if (membership.length > 0) {
    return Array.from(new Set(membership));
  }
  return [normalizeEmploymentStatus(candidate.employmentStatus)];
}

export function summarizeSelectedFacet(
  selectedIds: string[],
  options: CandidateFacetOption[],
  fallbackLabel: string,
): string {
  if (selectedIds.length === 0) {
    return fallbackLabel;
  }
  const selectedLabels = options
    .filter((option) => selectedIds.includes(option.id))
    .map((option) => option.label);
  if (selectedLabels.length === 0) {
    return fallbackLabel;
  }
  const concreteOptions = options.filter((option) => option.id !== "all");
  const concreteSelectedCount = concreteOptions.filter((option) => selectedIds.includes(option.id)).length;
  if (
    selectedLabels.length === options.length && options.length > 1 ||
    concreteOptions.length > 1 && concreteSelectedCount === concreteOptions.length
  ) {
    return fallbackLabel;
  }
  if (selectedIds.includes("all")) {
    return fallbackLabel;
  }
  const joined = selectedLabels.join("、");
  if (joined.length <= 16) {
    return joined;
  }
  if (selectedLabels.every((label) => /^Layer \d+$/.test(label))) {
    return `已选 ${selectedLabels.length} 层`;
  }
  return `已选 ${selectedLabels.length} 项`;
}

export function toggleFacetSelection(
  current: string[],
  optionId: string,
  {
    allId,
    fallback,
  }: {
  allId?: string;
  fallback: string[];
  },
): string[] {
  const active = current.includes(optionId);
  if (allId && optionId === allId) {
    return [allId];
  }
  const withoutAll = allId ? current.filter((item) => item !== allId) : [...current];
  const next = active ? withoutAll.filter((item) => item !== optionId) : [...withoutAll, optionId];
  return next.length > 0 ? next : [...fallback];
}

export function normalizeFacetSelection(
  current: string[],
  options: CandidateFacetOption[],
  fallback: string[],
): string[] {
  const validOptionIds = new Set(options.map((option) => option.id));
  const valid = current.filter((item) => validOptionIds.has(item));
  if (valid.length > 0) {
    return valid;
  }
  const normalizedFallback = fallback.filter((item) => validOptionIds.has(item));
  if (normalizedFallback.length > 0) {
    return normalizedFallback;
  }
  return [];
}

export function preserveEditedFacetSelection(current: string[], options: CandidateFacetOption[]): string[] {
  const validOptionIds = new Set(options.map((option) => option.id));
  return current.filter((item) => validOptionIds.has(item));
}

export function buildLayerOptions(dashboard: DashboardData): CandidateFacetOption[] {
  return dashboard.layers;
}

export function defaultLayerSelection(options: CandidateFacetOption[]): string[] {
  if (!options.some((option) => option.count > 0)) {
    return [];
  }
  const layerZero = options.find((option) => option.id === "layer_0" && option.count > 0);
  if (layerZero) {
    return ["layer_0"];
  }
  return options[0] ? [options[0].id] : ["layer_0"];
}

export function buildEmploymentOptions(candidates: Candidate[]): CandidateFacetOption[] {
  const counts = candidates.reduce<Record<string, number>>((accumulator, candidate) => {
    for (const status of candidateEmploymentMembership(candidate)) {
      if (status === "current" || status === "former") {
        accumulator[status] = (accumulator[status] || 0) + 1;
      }
    }
    return accumulator;
  }, {});
  const ordered: Array<{ id: string; label: string }> = [
    { id: "current", label: "在职" },
    { id: "former", label: "已离职" },
  ];
  return ordered
    .filter((item) => counts[item.id] > 0 || item.id === "current" || item.id === "former")
    .map((item) => ({ ...item, count: counts[item.id] || 0 }));
}

export function defaultEmploymentSelection(options: CandidateFacetOption[]): string[] {
  const baseline = options
    .filter((option) => (option.id === "current" || option.id === "former") && option.count > 0)
    .map((option) => option.id);
  if (baseline.length > 0) {
    return baseline;
  }
  const firstNonEmpty = options.find((option) => option.count > 0);
  return firstNonEmpty ? [firstNonEmpty.id] : options.slice(0, 1).map((option) => option.id);
}

export function buildLocationOptions(candidates: Candidate[]): CandidateFacetOption[] {
  const counts = candidates.reduce<Record<string, number>>((accumulator, candidate) => {
    const key = candidateLocationBucket(candidate);
    accumulator[key] = (accumulator[key] || 0) + 1;
    return accumulator;
  }, {});
  const ordered: Array<{ id: string; label: string }> = [
    { id: "us", label: "美国" },
    { id: "other", label: "其他" },
    { id: "unknown", label: "未提供地区信息" },
  ];
  return ordered.map((item) => ({ ...item, count: counts[item.id] || 0 }));
}

export function defaultLocationSelection(options: CandidateFacetOption[]): string[] {
  if (options.some((option) => option.id === "us" && option.count > 0)) {
    return ["us"];
  }
  if (options.some((option) => option.id === "unknown" && option.count > 0)) {
    return ["unknown"];
  }
  const firstNonEmpty = options.find((option) => option.count > 0);
  if (firstNonEmpty) {
    return [firstNonEmpty.id];
  }
  return options.slice(0, 1).map((option) => option.id);
}

export function buildFunctionOptions(candidates: Candidate[]): CandidateFacetOption[] {
  const counts = candidates.reduce<Record<string, number>>((accumulator, candidate) => {
    const buckets = candidateFunctionBuckets(candidate);
    if (!buckets) {
      // Server facet unavailable for this row: it contributes no membership
      // to any bucket (never a synthesized `unknown`).
      return accumulator;
    }
    for (const key of buckets) {
      accumulator[key] = (accumulator[key] || 0) + 1;
    }
    return accumulator;
  }, {});
  // Options are derived from server-provided bucket ids only; the canonical
  // board path prefers the backend facet summary (labels included) and this
  // local fallback intentionally keeps raw server ids as labels instead of
  // re-creating a local option enum.
  return Object.entries(counts)
    .sort((left, right) => left[0].localeCompare(right[0]))
    .map(([id, count]) => ({ id, label: id, count }));
}

export function defaultFunctionSelection(options: CandidateFacetOption[]): string[] {
  const nonEmpty = options.filter((option) => option.count > 0).map((option) => option.id);
  if (nonEmpty.length > 0) {
    return nonEmpty;
  }
  return options.slice(0, 1).map((option) => option.id);
}

export function computeCandidateIntentKeywordHits(candidate: Candidate, intentKeywords: string[]): string[] {
  if (intentKeywords.length === 0) {
    return [];
  }
  return intentKeywords.filter((keyword) => candidateMatchesIntentKeyword(candidate, keyword));
}

export function buildRecallBucketOptions(
  candidates: Candidate[],
  intentKeywords: string[],
): CandidateFacetOption[] {
  const options: CandidateFacetOption[] = [
    { id: "all", label: "全量", count: candidates.length },
  ];
  const provenanceKeywords = recallProvenanceKeywordSet(candidates, intentKeywords);
  for (const keyword of intentKeywords) {
    const bucketCount = candidates.filter((candidate) =>
      candidateMatchesRecallKeyword(candidate, keyword, provenanceKeywords),
    ).length;
    options.push({
      id: keywordSelectionId(keyword),
      label: keyword,
      count: bucketCount,
    });
  }
  const currentExcelIntakeCount = candidates.filter(candidateMatchesCurrentExcelIntake).length;
  if (currentExcelIntakeCount > 0) {
    options.push({
      id: CURRENT_EXCEL_INTAKE_RECALL_BUCKET_ID,
      label: CURRENT_EXCEL_INTAKE_RECALL_LABEL,
      count: currentExcelIntakeCount,
    });
  }
  return options;
}

export function defaultRecallSelection(options: CandidateFacetOption[]): string[] {
  return options.some((option) => option.id === "all") ? ["all"] : options.slice(0, 1).map((option) => option.id);
}

function matchesRecallSelection(
  candidate: Candidate,
  recallSelections: string[],
  intentKeywords: string[],
  provenanceKeywords: Set<string>,
): boolean {
  if (recallSelections.length === 0 || recallSelections.includes("all")) {
    return true;
  }
  const selectedKeywords = intentKeywords.filter((keyword) => recallSelections.includes(keywordSelectionId(keyword)));
  const currentExcelIntakeSelected = recallSelections.includes(CURRENT_EXCEL_INTAKE_RECALL_BUCKET_ID);
  if (selectedKeywords.length === 0 && !currentExcelIntakeSelected) {
    return true;
  }
  return (
    (currentExcelIntakeSelected && candidateMatchesCurrentExcelIntake(candidate)) ||
    selectedKeywords.some((keyword) => candidateMatchesRecallKeyword(candidate, keyword, provenanceKeywords))
  );
}

function matchesLayerSelection(candidate: Candidate, selectedLayers: string[]): boolean {
  if (selectedLayers.length === 0) {
    return true;
  }
  if (typeof candidate.outreachLayer !== "number" || !Number.isFinite(candidate.outreachLayer)) {
    return selectedLayers.includes("layer_0");
  }
  return selectedLayers.includes(`layer_${candidate.outreachLayer}`);
}

function matchesEmploymentSelection(candidate: Candidate, selectedEmploymentStatuses: string[]): boolean {
  if (selectedEmploymentStatuses.length === 0) {
    return true;
  }
  const memberships = candidateEmploymentMembership(candidate);
  if (memberships.length === 1 && memberships[0] === "lead") {
    // Documented `lead` compatibility semantic (FT0 §6): unknown/missing
    // employment matches only when both statuses are selected.
    return selectedEmploymentStatuses.includes("current") && selectedEmploymentStatuses.includes("former");
  }
  return memberships.some((status) => selectedEmploymentStatuses.includes(status));
}

function matchesLocationSelection(candidate: Candidate, selectedLocations: string[]): boolean {
  if (selectedLocations.length === 0) {
    return true;
  }
  return selectedLocations.includes(candidateLocationBucket(candidate));
}

function matchesFunctionSelection(candidate: Candidate, selectedFunctionBuckets: string[]): boolean {
  if (selectedFunctionBuckets.length === 0) {
    return true;
  }
  const buckets = candidateFunctionBuckets(candidate);
  if (!buckets) {
    // Server facet unavailable for this row: it cannot claim membership in
    // any selected bucket and is excluded from bucket-specific selections.
    return false;
  }
  return buckets.some((bucket) => selectedFunctionBuckets.includes(bucket));
}

function matchesKeywordSearch(candidate: Candidate, searchKeyword: string): boolean {
  const normalizedKeyword = searchKeyword.trim().toLowerCase();
  if (!normalizedKeyword) {
    return true;
  }
  const haystack = [
    candidate.name,
    candidate.headline,
    candidate.currentCompany || "",
    candidate.notesSnippet || "",
    candidate.team,
    ...candidate.focusAreas,
    ...candidate.matchReasons,
    ...candidate.education,
    ...candidate.experience,
    ...candidate.matchedKeywords,
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(normalizedKeyword);
}

export function filterCandidatesByFacets(
  candidates: Candidate[],
  selection: CandidateFacetSelection,
  intentKeywords: string[],
): Candidate[] {
  const provenanceKeywords = recallProvenanceKeywordSet(candidates, intentKeywords);
  return candidates.filter((candidate) => {
    return (
      matchesLayerSelection(candidate, selection.layers) &&
      matchesRecallSelection(candidate, selection.recallBuckets, intentKeywords, provenanceKeywords) &&
      matchesEmploymentSelection(candidate, selection.employmentStatuses) &&
      matchesLocationSelection(candidate, selection.locations) &&
      matchesFunctionSelection(candidate, selection.functionBuckets) &&
      matchesKeywordSearch(candidate, selection.searchKeyword)
    );
  });
}
