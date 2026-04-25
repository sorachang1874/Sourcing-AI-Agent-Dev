import type { Candidate, CandidateDetail, CandidateReviewStatus } from "../types";

type CandidateLike = Candidate | CandidateDetail;

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

export const LAYER_DEFINITIONS: Array<{ id: string; label: string; description: string }> = [
  { id: "layer_0", label: "Layer 0", description: "全量候选人" },
  { id: "layer_1", label: "Layer 1", description: "疑似中文姓名" },
  { id: "layer_2", label: "Layer 2", description: "泛华人地区经历" },
  { id: "layer_3", label: "Layer 3", description: "大陆地区经历" },
];

export function sanitizeCandidateText(value: string): string {
  return value.replace(/\s{2,}/g, " ").trim();
}

function normalizeComparableText(value: string): string {
  return sanitizeCandidateText(value)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, "");
}

export function extractPrimaryEmail(candidate: CandidateLike): string {
  return candidate.primaryEmail?.trim() || "";
}

export function resolveCandidateLinkedinUrl(candidate: CandidateLike): string {
  const direct = sanitizeCandidateText(candidate.linkedinUrl || "");
  if (direct) {
    return direct;
  }
  const evidenceUrl =
    candidate.evidence.find((item) => item.type === "linkedin")?.url ||
    "";
  return sanitizeCandidateText(evidenceUrl);
}

export function pickCandidateRoleLine(candidate: CandidateLike): string {
  const headline = sanitizeCandidateText(candidate.headline || "");
  const company = sanitizeCandidateText(candidate.currentCompany || "");
  const normalizedCompany = normalizeComparableText(company);
  const headlineSegments = headline
    .split("·")
    .map((segment) => sanitizeCandidateText(segment))
    .filter(Boolean);
  const uniqueSegments = headlineSegments.filter((segment, index, list) => {
    const normalized = segment.toLowerCase();
    return list.findIndex((item) => item.toLowerCase() === normalized) === index;
  });
  const prunedSegments = uniqueSegments.filter((segment, index, list) => {
    const normalized = segment.toLowerCase();
    if (normalizedCompany && normalizeComparableText(segment) === normalizedCompany) {
      return false;
    }
    return !list.some((other, otherIndex) => {
      if (otherIndex === index) {
        return false;
      }
      const normalizedOther = other.toLowerCase();
      return normalizedOther.includes(normalized) && normalizedOther.length > normalized.length + 3;
    });
  });
  const dedupedHeadline = prunedSegments.join(" · ");
  if (dedupedHeadline && company && !normalizeComparableText(dedupedHeadline).includes(normalizedCompany)) {
    return `${dedupedHeadline} · ${company}`;
  }
  return dedupedHeadline || company || "候选资料待补充";
}

export function pickSurnameInitial(name: string): string {
  const trimmed = (name || "").trim();
  if (!trimmed) {
    return "U.";
  }
  const parts = trimmed.split(/\s+/).filter(Boolean);
  const token = parts[parts.length - 1] || trimmed;
  const first = token[0] || "U";
  return `${first.toUpperCase()}.`;
}

export function reviewStatusLabel(status: CandidateReviewStatus): string {
  if (status === "needs_review") {
    return "待审核";
  }
  if (status === "needs_profile_completion") {
    return "信息不完整待补全";
  }
  if (status === "low_profile_richness") {
    return "LinkedIn信息丰富度低";
  }
  if (status === "verified_keep") {
    return "已核实候选人";
  }
  if (status === "verified_exclude") {
    return "已核实可排除候选人";
  }
  return "无需审核";
}

export function employmentStatusLabel(value: CandidateLike["employmentStatus"]): string {
  if (value === "current") {
    return "在职";
  }
  if (value === "former") {
    return "已离职";
  }
  return "线索";
}

function normalizedTimelineLines(value: string[] | undefined): string[] {
  return Array.isArray(value) ? value.map((item) => sanitizeCandidateText(item)).filter(Boolean) : [];
}

function normalizeProfileCaptureKind(value: string | undefined): string {
  return sanitizeCandidateText(value || "").toLowerCase();
}

export function candidateHasCompleteProfileDetail(
  candidate: Pick<
    CandidateLike,
    "experience" | "education" | "hasProfileDetail" | "profileCaptureKind" | "headline" | "summary" | "primaryEmail"
  >,
): boolean {
  const captureKind = normalizeProfileCaptureKind(candidate.profileCaptureKind);
  if (PARTIAL_PROFILE_CAPTURE_KINDS.has(captureKind)) {
    return false;
  }
  if (FULL_PROFILE_CAPTURE_KINDS.has(captureKind)) {
    return true;
  }
  if (candidate.hasProfileDetail) {
    return true;
  }
  const experienceLines = normalizedTimelineLines(candidate.experience);
  const educationLines = normalizedTimelineLines(candidate.education);
  if (educationLines.length > 0) {
    return true;
  }
  if (experienceLines.length >= 2) {
    return true;
  }
  return Boolean(
    experienceLines.length > 0 &&
      (
        sanitizeCandidateText(candidate.summary || "") ||
        sanitizeCandidateText(candidate.headline || "") ||
        sanitizeCandidateText(candidate.primaryEmail || "")
      ),
  );
}

export function reviewTypeToCandidateStatus(reviewType: string): CandidateReviewStatus {
  const normalized = sanitizeCandidateText(reviewType).toLowerCase();
  if (normalized === "profile_completion_gap" || normalized === "missing_primary_profile") {
    return "needs_profile_completion";
  }
  if (normalized === "profile_enrichment_opportunity") {
    return "low_profile_richness";
  }
  return "needs_review";
}

export function isOpenReviewStatus(status: CandidateReviewStatus | null | undefined): boolean {
  return status === "needs_review" || status === "needs_profile_completion" || status === "low_profile_richness";
}

export function deriveBackendReviewStatus(reviewTypes: string[]): CandidateReviewStatus | null {
  if (reviewTypes.length === 0) {
    return null;
  }
  const normalized = reviewTypes.map((reviewType) => reviewTypeToCandidateStatus(reviewType));
  if (normalized.includes("needs_profile_completion")) {
    return "needs_profile_completion";
  }
  if (normalized.includes("low_profile_richness")) {
    return "low_profile_richness";
  }
  return "needs_review";
}

export function resolveEffectiveReviewStatus(options: {
  localStatus?: CandidateReviewStatus | null;
  backendReviewTypes?: string[];
  autoStatus?: CandidateReviewStatus | null;
}): CandidateReviewStatus | null {
  const localStatus = options.localStatus || null;
  const backendStatus = deriveBackendReviewStatus(options.backendReviewTypes || []);
  if (localStatus && !isOpenReviewStatus(localStatus)) {
    return localStatus;
  }
  if (
    backendStatus &&
    (backendStatus === "needs_profile_completion" || backendStatus === "low_profile_richness") &&
    !options.autoStatus
  ) {
    return localStatus && isOpenReviewStatus(localStatus) ? localStatus : null;
  }
  if (backendStatus) {
    return backendStatus;
  }
  if (localStatus) {
    return localStatus;
  }
  return options.autoStatus || null;
}

export function candidateNeedsProfileCompletion(
  candidate: Pick<
    CandidateLike,
    "linkedinUrl" | "experience" | "education" | "needsProfileCompletion" | "hasProfileDetail" | "profileCaptureKind" | "headline" | "summary" | "primaryEmail"
  >,
): boolean {
  const linkedinUrl = sanitizeCandidateText(candidate.linkedinUrl || "");
  if (!linkedinUrl) {
    return false;
  }
  const captureKind = normalizeProfileCaptureKind(candidate.profileCaptureKind);
  if (PARTIAL_PROFILE_CAPTURE_KINDS.has(captureKind)) {
    return true;
  }
  if (candidate.needsProfileCompletion) {
    return true;
  }
  return !candidateHasCompleteProfileDetail(candidate);
}

export function candidateHasLowProfileRichness(
  candidate: Pick<
    CandidateLike,
    "linkedinUrl" | "experience" | "education" | "needsProfileCompletion" | "hasProfileDetail" | "profileCaptureKind" | "lowProfileRichness" | "headline" | "summary" | "primaryEmail"
  >,
): boolean {
  const linkedinUrl = sanitizeCandidateText(candidate.linkedinUrl || "");
  if (!linkedinUrl || candidateNeedsProfileCompletion(candidate)) {
    return false;
  }
  if (candidate.lowProfileRichness) {
    return true;
  }
  const experienceLines = normalizedTimelineLines(candidate.experience);
  const educationLines = normalizedTimelineLines(candidate.education);
  const hasProfileDetail = candidateHasCompleteProfileDetail(candidate);
  return hasProfileDetail && (experienceLines.length === 0 || educationLines.length === 0);
}

export function candidateAutoReviewStatus(
  candidate: Pick<
    CandidateLike,
    "linkedinUrl" | "experience" | "education" | "needsProfileCompletion" | "hasProfileDetail" | "profileCaptureKind" | "lowProfileRichness" | "headline" | "summary" | "primaryEmail"
  >,
): CandidateReviewStatus | null {
  if (candidateNeedsProfileCompletion(candidate)) {
    return "needs_profile_completion";
  }
  if (candidateHasLowProfileRichness(candidate)) {
    return "low_profile_richness";
  }
  return null;
}
