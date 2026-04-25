import type { Candidate, CandidateReviewRecord, CandidateReviewStatus } from "../types";
import { getCandidateReviewRecords, upsertCandidateReviewRecord as upsertCandidateReviewRecordApi } from "./api";
import { candidateAutoReviewStatus, extractPrimaryEmail } from "./candidatePresentation";

const UPDATED_EVENT = "redmatch-candidate-review-registry-updated";

function emitUpdated(): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event(UPDATED_EVENT));
  }
}

export function candidateReviewRegistryUpdatedEventName(): string {
  return UPDATED_EVENT;
}

export async function listCandidateReviewRecords(jobId: string): Promise<CandidateReviewRecord[]> {
  if (!jobId) {
    return [];
  }
  return getCandidateReviewRecords({ jobId });
}

export async function upsertCandidateReviewRecord(
  patch: Omit<CandidateReviewRecord, "id" | "addedAt" | "updatedAt"> & {
    id?: string;
    updatedAt?: string;
    metadata?: Record<string, unknown>;
  },
): Promise<CandidateReviewRecord> {
  const record = await upsertCandidateReviewRecordApi({
    id: patch.id,
    jobId: patch.jobId,
    historyId: patch.historyId,
    candidateId: patch.candidateId,
    candidateName: patch.candidateName,
    headline: patch.headline,
    currentCompany: patch.currentCompany,
    avatarUrl: patch.avatarUrl,
    linkedinUrl: patch.linkedinUrl,
    primaryEmail: patch.primaryEmail,
    status: patch.status,
    comment: patch.comment,
    source: patch.source,
  });
  emitUpdated();
  return record;
}

export async function addCandidateToReviewRegistry(
  jobId: string,
  historyId: string,
  candidate: Candidate,
): Promise<CandidateReviewRecord> {
  return upsertCandidateReviewRecord({
    jobId,
    historyId,
    candidateId: candidate.id,
    candidateName: candidate.name,
    headline: candidate.headline,
    currentCompany: candidate.currentCompany,
    avatarUrl: candidate.avatarUrl,
    linkedinUrl: candidate.linkedinUrl,
    primaryEmail: extractPrimaryEmail(candidate),
    metadata: candidate.primaryEmailMetadata
      ? { primary_email_metadata: candidate.primaryEmailMetadata }
      : {},
    status: candidateAutoReviewStatus(candidate) || "needs_review",
    source: "manual_add",
    comment: "",
  });
}

export async function setCandidateReviewStatus(
  jobId: string,
  candidate: Pick<Candidate, "id" | "name" | "headline" | "currentCompany" | "avatarUrl" | "linkedinUrl" | "externalLinks" | "summary" | "notesSnippet" | "matchReasons" | "experience" | "education" | "primaryEmail" | "primaryEmailMetadata">,
  status: CandidateReviewStatus,
  options?: {
    historyId?: string;
    source?: CandidateReviewRecord["source"];
    comment?: string;
  },
): Promise<CandidateReviewRecord> {
  return upsertCandidateReviewRecord({
    jobId,
    historyId: options?.historyId || "",
    candidateId: candidate.id,
    candidateName: candidate.name,
    headline: candidate.headline,
    currentCompany: candidate.currentCompany,
    avatarUrl: candidate.avatarUrl,
    linkedinUrl: candidate.linkedinUrl,
    primaryEmail: extractPrimaryEmail(candidate as Candidate),
    metadata:
      "primaryEmailMetadata" in candidate && candidate.primaryEmailMetadata
        ? { primary_email_metadata: candidate.primaryEmailMetadata }
        : {},
    status,
    source: options?.source || "manual_review",
    comment: options?.comment || "",
  });
}
