import type { Candidate, TargetCandidateFollowUpStatus, TargetCandidateRecord } from "../types";
import { getTargetCandidates, upsertTargetCandidate as upsertTargetCandidateApi } from "./api";
import { extractPrimaryEmail } from "./candidatePresentation";

const UPDATED_EVENT = "redmatch-target-candidates-updated";

function emitUpdated(): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event(UPDATED_EVENT));
  }
}

export function targetCandidatesUpdatedEventName(): string {
  return UPDATED_EVENT;
}

export async function readTargetCandidates(): Promise<TargetCandidateRecord[]> {
  return getTargetCandidates();
}

export async function upsertTargetCandidate(
  patch: Omit<TargetCandidateRecord, "id" | "addedAt" | "updatedAt"> & {
    id?: string;
    updatedAt?: string;
    metadata?: Record<string, unknown>;
  },
): Promise<TargetCandidateRecord> {
  const record = await upsertTargetCandidateApi({
    id: patch.id,
    candidateId: patch.candidateId,
    historyId: patch.historyId,
    jobId: patch.jobId,
    candidateName: patch.candidateName,
    headline: patch.headline,
    currentCompany: patch.currentCompany,
    avatarUrl: patch.avatarUrl,
    linkedinUrl: patch.linkedinUrl,
    primaryEmail: patch.primaryEmail,
    followUpStatus: patch.followUpStatus,
    qualityScore: patch.qualityScore,
    comment: patch.comment,
  });
  emitUpdated();
  return record;
}

export async function addTargetCandidate(
  candidate: Candidate,
  options?: {
    jobId?: string;
    historyId?: string;
  },
): Promise<TargetCandidateRecord> {
  return upsertTargetCandidate({
    candidateId: candidate.id,
    historyId: options?.historyId || "",
    jobId: options?.jobId || "",
    candidateName: candidate.name,
    headline: candidate.headline,
    currentCompany: candidate.currentCompany,
    avatarUrl: candidate.avatarUrl,
    linkedinUrl: candidate.linkedinUrl,
    primaryEmail: extractPrimaryEmail(candidate),
    metadata: candidate.primaryEmailMetadata
      ? { primary_email_metadata: candidate.primaryEmailMetadata }
      : {},
    followUpStatus: "pending_outreach",
    qualityScore: null,
    comment: "",
  });
}

export async function updateTargetCandidate(
  candidateId: string,
  patch: Partial<
    Pick<
      TargetCandidateRecord,
      | "followUpStatus"
      | "qualityScore"
      | "comment"
      | "headline"
      | "currentCompany"
      | "avatarUrl"
      | "linkedinUrl"
      | "primaryEmail"
      | "primaryEmailMetadata"
    >
  > & {
    metadata?: Record<string, unknown>;
  },
): Promise<TargetCandidateRecord | null> {
  const current = await readTargetCandidates();
  const matched = current.find((item) => item.id === candidateId || item.candidateId === candidateId);
  if (!matched) {
    return null;
  }
  return upsertTargetCandidate({
    ...matched,
    ...patch,
  });
}

export function followUpStatusLabel(status: TargetCandidateFollowUpStatus): string {
  if (status === "contacted_waiting") {
    return "已沟通待回复";
  }
  if (status === "rejected") {
    return "已拒绝邀约";
  }
  if (status === "accepted") {
    return "已接受邀约";
  }
  if (status === "interview_completed") {
    return "已完成访谈";
  }
  return "待沟通";
}
