import { useEffect, useMemo, useState } from "react";
import {
  getManualReviewItems,
  peekCandidateReviewRecordsCache,
  peekManualReviewItemsCache,
} from "../lib/api";
import { candidateAutoReviewStatus, resolveEffectiveReviewStatus } from "../lib/candidatePresentation";
import {
  candidateReviewRegistryUpdatedEventName,
  listCandidateReviewRecords,
} from "../lib/reviewRegistry";
import type { Candidate, CandidateReviewRecord, CandidateReviewStatus, ManualReviewItem } from "../types";

interface UseCandidateReviewStateResult {
  backendItems: ManualReviewItem[];
  localRecords: CandidateReviewRecord[];
  effectiveReviewCount: number;
  reviewStatusMap: Record<string, CandidateReviewStatus>;
  refresh: () => Promise<void>;
}

export function useCandidateReviewState(jobId: string, candidates: Candidate[] = []): UseCandidateReviewStateResult {
  const [backendItems, setBackendItems] = useState<ManualReviewItem[]>(() =>
    jobId ? peekManualReviewItemsCache({ jobId, status: "open" }) : [],
  );
  const [localRecords, setLocalRecords] = useState<CandidateReviewRecord[]>(() =>
    jobId ? peekCandidateReviewRecordsCache({ jobId }) : [],
  );

  const refresh = async () => {
    if (!jobId) {
      setBackendItems([]);
      setLocalRecords([]);
      return;
    }
    const [backendResult, registryResult] = await Promise.allSettled([
      getManualReviewItems({ jobId, status: "open" }),
      listCandidateReviewRecords(jobId),
    ]);
    setBackendItems(backendResult.status === "fulfilled" ? backendResult.value : []);
    setLocalRecords(registryResult.status === "fulfilled" ? registryResult.value : []);
  };

  useEffect(() => {
    setBackendItems(jobId ? peekManualReviewItemsCache({ jobId, status: "open" }) : []);
    setLocalRecords(jobId ? peekCandidateReviewRecordsCache({ jobId }) : []);
    void refresh();
  }, [jobId]);

  useEffect(() => {
    const handleSync = () => {
      if (!jobId) {
        setLocalRecords([]);
        return;
      }
      void listCandidateReviewRecords(jobId)
        .then((records) => {
          setLocalRecords(records);
        })
        .catch(() => {
          setLocalRecords([]);
        });
    };
    window.addEventListener(candidateReviewRegistryUpdatedEventName(), handleSync);
    window.addEventListener("storage", handleSync);
    return () => {
      window.removeEventListener(candidateReviewRegistryUpdatedEventName(), handleSync);
      window.removeEventListener("storage", handleSync);
    };
  }, [jobId]);

  const reviewStatusMap = useMemo(() => {
    const map: Record<string, CandidateReviewStatus> = {};
    const backendReviewTypesByCandidateId = new Map<string, string[]>();
    const candidateById = new Map<string, Candidate>();
    const localStatusByCandidateId = new Map<string, CandidateReviewStatus>();
    for (const item of backendItems) {
      if (item.candidateId) {
        const existing = backendReviewTypesByCandidateId.get(item.candidateId) || [];
        existing.push(item.reviewType);
        backendReviewTypesByCandidateId.set(item.candidateId, existing);
      }
    }
    for (const record of localRecords) {
      if (record.candidateId) {
        localStatusByCandidateId.set(record.candidateId, record.status);
      }
    }
    const candidateIds = new Set<string>();
    for (const candidateId of backendReviewTypesByCandidateId.keys()) {
      candidateIds.add(candidateId);
    }
    for (const candidateId of localStatusByCandidateId.keys()) {
      candidateIds.add(candidateId);
    }
    for (const candidate of candidates) {
      if (candidate.id) {
        candidateById.set(candidate.id, candidate);
        candidateIds.add(candidate.id);
      }
    }
    for (const candidateId of candidateIds) {
      const candidate = candidateById.get(candidateId);
      const status = resolveEffectiveReviewStatus({
        localStatus: localStatusByCandidateId.get(candidateId) || null,
        backendReviewTypes: backendReviewTypesByCandidateId.get(candidateId) || [],
        autoStatus: candidate ? candidateAutoReviewStatus(candidate) : null,
      });
      if (status) {
        map[candidateId] = status;
      }
    }
    for (const candidate of candidates) {
      if (!candidate.id || map[candidate.id]) {
        continue;
      }
      const autoStatus = candidateAutoReviewStatus(candidate);
      if (autoStatus) {
        map[candidate.id] = autoStatus;
      }
    }
    return map;
  }, [backendItems, candidates, localRecords]);

  const effectiveReviewCount = useMemo(() => {
    const ids = new Set<string>();
    for (const item of backendItems) {
      if (
        item.candidateId &&
        reviewStatusMap[item.candidateId] !== "verified_keep" &&
        reviewStatusMap[item.candidateId] !== "verified_exclude" &&
        reviewStatusMap[item.candidateId] !== "no_review_needed"
      ) {
        ids.add(item.candidateId);
      }
    }
    for (const record of localRecords) {
      if (
        (record.status === "needs_review" ||
          record.status === "needs_profile_completion" ||
          record.status === "low_profile_richness") &&
        record.candidateId
      ) {
        ids.add(record.candidateId);
      }
    }
    for (const candidate of candidates) {
      if (
        reviewStatusMap[candidate.id] === "needs_profile_completion" ||
        reviewStatusMap[candidate.id] === "low_profile_richness"
      ) {
        ids.add(candidate.id);
      }
    }
    return ids.size;
  }, [backendItems, candidates, localRecords, reviewStatusMap]);

  return {
    backendItems,
    localRecords,
    effectiveReviewCount,
    reviewStatusMap,
    refresh,
  };
}
