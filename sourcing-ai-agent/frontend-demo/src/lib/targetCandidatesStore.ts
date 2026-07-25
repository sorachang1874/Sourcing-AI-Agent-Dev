import type { Candidate, TargetCandidateFollowUpStatus, TargetCandidateRecord } from "../types";
import {
  addProjectionCandidateToCrm,
  addProjectionCandidatesToCrm,
  getTargetCandidates,
  upsertTargetCandidate as upsertTargetCandidateApi,
} from "./api";

const UPDATED_EVENT = "redmatch-target-candidates-updated";

export interface TargetCandidateReadOptions {
  jobId?: string;
  historyId?: string;
  candidateId?: string;
  followUpStatus?: TargetCandidateFollowUpStatus;
  sourceProjectionId?: string;
  sourceCollectionId?: string;
}

function emitUpdated(): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event(UPDATED_EVENT));
  }
}

export function targetCandidatesUpdatedEventName(): string {
  return UPDATED_EVENT;
}

export async function readTargetCandidates(options?: TargetCandidateReadOptions): Promise<TargetCandidateRecord[]> {
  return getTargetCandidates(options);
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
    workspaceId: patch.workspaceId || "default",
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
    projectionId?: string;
    membershipRevision?: string;
  },
): Promise<TargetCandidateRecord> {
  const projectionId = (options?.projectionId || "").trim();
  const membershipRevision = (options?.membershipRevision || "").trim();
  const candidateIdentityKey = (candidate.candidateIdentityKey || candidate.personIdentityKey || "").trim();
  if (!projectionId || !membershipRevision || !candidateIdentityKey) {
    throw new Error("加入目标候选人需要 canonical projection revision 和 candidate_identity_key。");
  }
  const record = await addProjectionCandidateToCrm({
    projectionId,
    candidateIdentityKey,
    expectedMembershipRevision: membershipRevision,
    stage: "outreach_ready",
    sourceReason: "operator_selected_from_projection",
    idempotencyKey: `crm:add-target:${projectionId}:${membershipRevision}:${candidateIdentityKey}`,
  });
  emitUpdated();
  return record;
}

export async function addTargetCandidates(
  candidates: Candidate[],
  options: {
    projectionId: string;
    membershipRevision: string;
  },
): Promise<{
  records: TargetCandidateRecord[];
  requestedCandidateCount: number;
  successfulWriteCount: number;
  failedWriteCount: number;
}> {
  const projectionId = String(options.projectionId || "").trim();
  const membershipRevision = String(options.membershipRevision || "").trim();
  const candidateIdentityKeys = candidates
    .map((candidate) => candidate.candidateIdentityKey || candidate.personIdentityKey || "")
    .map((value) => value.trim())
    .filter(Boolean);
  if (!projectionId || !membershipRevision || candidateIdentityKeys.length === 0) {
    throw new Error("批量加入目标候选人需要 canonical projection revision 和候选人标识。");
  }
  const result = await addProjectionCandidatesToCrm({
    projectionId,
    candidateIdentityKeys,
    expectedMembershipRevision: membershipRevision,
    stage: "outreach_ready",
    sourceReason: "operator_selected_from_projection",
    idempotencyKey: `crm:add-target-bulk:${projectionId}:${membershipRevision}`,
  });
  emitUpdated();
  return result;
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
  options?: TargetCandidateReadOptions,
): Promise<TargetCandidateRecord | null> {
  const current = await readTargetCandidates(options);
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
