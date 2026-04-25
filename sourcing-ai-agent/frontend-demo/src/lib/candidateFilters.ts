import type { Candidate, DashboardData } from "../types";

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

function normalizeEmploymentStatus(value: Candidate["employmentStatus"]): string {
  return value === "lead" ? "lead" : value;
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

const ROLE_BUCKET_TO_FUNCTION_BUCKET: Record<string, string> = {
  research: "research",
  engineering: "engineering",
  infra_systems: "engineering",
  product_management: "product_management",
};

const FUNCTION_BUCKET_KEYWORDS: Record<string, string[]> = {
  research: [
    "research scientist",
    "applied scientist",
    "research engineer",
    "member of research staff",
    "researcher",
    "scientist",
    "research",
  ],
  engineering: [
    "member of technical staff",
    "technical staff",
    "software engineer",
    "machine learning engineer",
    "research engineer",
    "systems engineer",
    "devops engineer",
    "platform engineer",
    "engineer",
    "engineering",
    "developer",
    "devops",
    "infrastructure",
    "backend",
    "frontend",
    "full stack",
    "architect",
  ],
  product_management: [
    "product manager",
    "group product manager",
    "senior product manager",
    "director of product",
    "head of product",
    "product management",
  ],
};

function countKeywordOccurrences(corpus: string, pattern: string): number {
  const normalizedPattern = normalizeSearchText(pattern);
  if (!normalizedPattern) {
    return 0;
  }
  if (!/[a-z0-9]/i.test(normalizedPattern)) {
    return corpus.includes(normalizedPattern) ? 1 : 0;
  }
  const escaped = normalizedPattern
    .split(" ")
    .filter(Boolean)
    .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .join("\\s+");
  if (!escaped) {
    return 0;
  }
  return (corpus.match(new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, "gi")) || []).length;
}

function inferredFunctionBucketFromProfile(candidate: Candidate): string[] {
  const mappedRoleBucket = ROLE_BUCKET_TO_FUNCTION_BUCKET[normalizeToken(candidate.roleBucket || "")];
  const corpus = candidateKeywordCorpus(candidate);
  const scoredBuckets = Object.entries(FUNCTION_BUCKET_KEYWORDS).map(([bucket, patterns]) => {
    const keywordScore = patterns.reduce((total, pattern) => total + countKeywordOccurrences(corpus, pattern), 0);
    const roleBucketBoost = mappedRoleBucket === bucket ? 3 : 0;
    return {
      bucket,
      score: keywordScore + roleBucketBoost,
    };
  });
  const maxScore = Math.max(...scoredBuckets.map((entry) => entry.score), 0);
  if (maxScore > 0) {
    return [
      scoredBuckets
        .filter((entry) => entry.score === maxScore)
        .sort((left, right) => {
          const priority = ["research", "engineering", "product_management"];
          return priority.indexOf(left.bucket) - priority.indexOf(right.bucket);
        })[0]?.bucket || "other",
    ];
  }
  if (mappedRoleBucket) {
    return [mappedRoleBucket];
  }
  return ["unknown"];
}

function candidateFunctionBuckets(candidate: Candidate): string[] {
  const normalizedIds = (candidate.functionIds || [])
    .map((item) => normalizeToken(item))
    .filter(Boolean);
  if (normalizedIds.length !== 1) {
    return inferredFunctionBucketFromProfile(candidate);
  }
  const buckets = new Set<string>();
  if (normalizedIds.includes("24")) {
    buckets.add("research");
  }
  if (normalizedIds.includes("8")) {
    buckets.add("engineering");
  }
  if (normalizedIds.includes("19")) {
    buckets.add("product_management");
  }
  if (normalizedIds.some((item) => !["24", "8", "19"].includes(item))) {
    buckets.add("other");
  }
  if (buckets.size === 0) {
    buckets.add("other");
  }
  return Array.from(buckets);
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
  if (selectedLabels.length === options.length && options.length > 1) {
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
  const optionById = new Map(options.map((option) => [option.id, option]));
  const validOptionIds = new Set(options.map((option) => option.id));
  const valid = current.filter((item) => validOptionIds.has(item));
  const hasAnyPositiveOption = options.some((option) => option.count > 0);
  const selectedHasPositiveOption = valid.some((item) => (optionById.get(item)?.count || 0) > 0);
  if (valid.length > 0 && (selectedHasPositiveOption || !hasAnyPositiveOption)) {
    return valid;
  }
  const normalizedFallback = fallback.filter((item) => validOptionIds.has(item));
  if (normalizedFallback.length > 0) {
    return normalizedFallback;
  }
  if (valid.length > 0) {
    return valid;
  }
  return [];
}

export function buildLayerOptions(dashboard: DashboardData): CandidateFacetOption[] {
  return dashboard.layers;
}

export function defaultLayerSelection(options: CandidateFacetOption[]): string[] {
  const layerZero = options.find((option) => option.id === "layer_0" && option.count > 0);
  if (layerZero) {
    return ["layer_0"];
  }
  return options[0] ? [options[0].id] : ["layer_0"];
}

export function buildEmploymentOptions(candidates: Candidate[]): CandidateFacetOption[] {
  const counts = candidates.reduce<Record<string, number>>((accumulator, candidate) => {
    const key = normalizeEmploymentStatus(candidate.employmentStatus);
    accumulator[key] = (accumulator[key] || 0) + 1;
    return accumulator;
  }, {});
  const ordered: Array<{ id: string; label: string }> = [
    { id: "current", label: "在职" },
    { id: "former", label: "已离职" },
    { id: "lead", label: "线索" },
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
  return ordered
    .filter((item) => counts[item.id] > 0 || item.id === "us" || item.id === "other")
    .map((item) => ({ ...item, count: counts[item.id] || 0 }));
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
    for (const key of candidateFunctionBuckets(candidate)) {
      accumulator[key] = (accumulator[key] || 0) + 1;
    }
    return accumulator;
  }, {});
  const ordered: Array<{ id: string; label: string }> = [
    { id: "research", label: "Researcher" },
    { id: "engineering", label: "Engineer" },
    { id: "product_management", label: "Product Manager" },
    { id: "other", label: "其他" },
    { id: "unknown", label: "未提供职能信息" },
  ];
  return ordered
    .filter((item) => item.id !== "unknown" || counts[item.id] > 0)
    .map((item) => ({ ...item, count: counts[item.id] || 0 }));
}

export function defaultFunctionSelection(options: CandidateFacetOption[]): string[] {
  const baseline = options
    .filter((option) => (option.id === "research" || option.id === "engineering") && option.count > 0)
    .map((option) => option.id);
  if (baseline.length > 0) {
    return baseline;
  }
  if (options.some((option) => option.id === "unknown" && option.count > 0)) {
    return ["unknown"];
  }
  const firstNonEmpty = options.find((option) => option.count > 0);
  return firstNonEmpty ? [firstNonEmpty.id] : options.slice(0, 1).map((option) => option.id);
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
  for (const keyword of intentKeywords) {
    const bucketCount = candidates.filter((candidate) => candidateMatchesIntentKeyword(candidate, keyword)).length;
    options.push({
      id: keywordSelectionId(keyword),
      label: keyword,
      count: bucketCount,
    });
  }
  return options;
}

export function defaultRecallSelection(options: CandidateFacetOption[]): string[] {
  return options.some((option) => option.id === "all") ? ["all"] : options.slice(0, 1).map((option) => option.id);
}

function matchesRecallSelection(candidate: Candidate, recallSelections: string[], intentKeywords: string[]): boolean {
  if (recallSelections.length === 0 || recallSelections.includes("all")) {
    return true;
  }
  const selectedKeywords = intentKeywords.filter((keyword) => recallSelections.includes(keywordSelectionId(keyword)));
  if (selectedKeywords.length === 0) {
    return true;
  }
  return selectedKeywords.some((keyword) => candidateMatchesIntentKeyword(candidate, keyword));
}

function matchesLayerSelection(candidate: Candidate, selectedLayers: string[]): boolean {
  if (selectedLayers.length === 0) {
    return true;
  }
  return selectedLayers.includes(`layer_${candidate.outreachLayer}`);
}

function matchesEmploymentSelection(candidate: Candidate, selectedEmploymentStatuses: string[]): boolean {
  if (selectedEmploymentStatuses.length === 0) {
    return true;
  }
  return selectedEmploymentStatuses.includes(normalizeEmploymentStatus(candidate.employmentStatus));
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
  return candidateFunctionBuckets(candidate).some((bucket) => selectedFunctionBuckets.includes(bucket));
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
  return candidates.filter((candidate) => {
    return (
      matchesLayerSelection(candidate, selection.layers) &&
      matchesRecallSelection(candidate, selection.recallBuckets, intentKeywords) &&
      matchesEmploymentSelection(candidate, selection.employmentStatuses) &&
      matchesLocationSelection(candidate, selection.locations) &&
      matchesFunctionSelection(candidate, selection.functionBuckets) &&
      matchesKeywordSearch(candidate, selection.searchKeyword)
    );
  });
}
