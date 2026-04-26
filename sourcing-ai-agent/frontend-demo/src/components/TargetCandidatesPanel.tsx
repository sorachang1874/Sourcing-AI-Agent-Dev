import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Avatar } from "./Avatar";
import { FacetMultiSelect } from "./FacetMultiSelect";
import {
  exportTargetCandidatePublicWebArchive,
  exportTargetCandidatesArchive,
  getCandidateDetailsBatch,
  getTargetCandidatePublicWebDetail,
  getTargetCandidatePublicWebSearches,
  peekTargetCandidatesCache,
  promoteTargetCandidatePublicWebSignal,
  startTargetCandidatePublicWebSearch,
} from "../lib/api";
import {
  type CandidateFacetOption,
  normalizeFacetSelection,
  summarizeSelectedFacet,
  toggleFacetSelection,
} from "../lib/candidateFilters";
import {
  followUpStatusLabel,
  readTargetCandidates,
  targetCandidatesUpdatedEventName,
  updateTargetCandidate,
} from "../lib/targetCandidatesStore";
import { pickCandidateRoleLine } from "../lib/candidatePresentation";
import type {
  TargetCandidateFollowUpStatus,
  TargetCandidatePublicWebDetail,
  TargetCandidatePublicWebRun,
  TargetCandidatePublicWebSearchState,
  TargetCandidatePublicWebSignal,
  TargetCandidatePublicWebStatus,
  TargetCandidateRecord,
} from "../types";

const FOLLOW_UP_OPTIONS: Array<{ id: TargetCandidateFollowUpStatus; label: string }> = [
  { id: "pending_outreach", label: "待沟通" },
  { id: "contacted_waiting", label: "已沟通待回复" },
  { id: "rejected", label: "已拒绝邀约" },
  { id: "accepted", label: "已接受邀约" },
  { id: "interview_completed", label: "已完成访谈" },
];

const PUBLIC_WEB_FILTER_OPTIONS: Array<{ id: string; label: string }> = [
  { id: "not_started", label: "未开始" },
  { id: "running", label: "进行中" },
  { id: "completed", label: "已完成" },
  { id: "needs_review", label: "需复核/异常" },
  { id: "failed", label: "失败/取消" },
];

const PUBLIC_WEB_TERMINAL_STATUSES = new Set<TargetCandidatePublicWebStatus>([
  "completed",
  "completed_with_errors",
  "needs_review",
  "failed",
  "cancelled",
]);

const EMPTY_PUBLIC_WEB_STATE: TargetCandidatePublicWebSearchState = {
  status: "idle",
  batches: [],
  runs: [],
};

function downloadBlobFile(filename: string, blob: Blob): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function publicWebStatusLabel(status: TargetCandidatePublicWebStatus): string {
  if (status === "search_submitted") {
    return "已提交搜索";
  }
  if (status === "searching") {
    return "搜索中";
  }
  if (status === "entry_links_ready") {
    return "入口链接已就绪";
  }
  if (status === "fetching") {
    return "抓取中";
  }
  if (status === "analyzing") {
    return "分析中";
  }
  if (status === "completed") {
    return "已完成";
  }
  if (status === "completed_with_errors") {
    return "完成但有异常";
  }
  if (status === "needs_review") {
    return "需人工复核";
  }
  if (status === "failed") {
    return "失败";
  }
  if (status === "cancelled") {
    return "已取消";
  }
  if (status === "queued") {
    return "已排队";
  }
  return "未开始";
}

function publicWebStatusTone(status: TargetCandidatePublicWebStatus): string {
  if (status === "completed") {
    return "is-success";
  }
  if (status === "completed_with_errors" || status === "needs_review") {
    return "is-warning";
  }
  if (status === "failed" || status === "cancelled") {
    return "is-danger";
  }
  if (!PUBLIC_WEB_TERMINAL_STATUSES.has(status) && status !== "unknown") {
    return "is-running";
  }
  return "is-muted";
}

function publicWebFilterBucket(status: TargetCandidatePublicWebStatus): string {
  if (status === "completed") {
    return "completed";
  }
  if (status === "completed_with_errors" || status === "needs_review") {
    return "needs_review";
  }
  if (status === "failed" || status === "cancelled") {
    return "failed";
  }
  if (status !== "unknown") {
    return "running";
  }
  return "not_started";
}

function allOptionIds(options: CandidateFacetOption[]): string[] {
  return options.map((option) => option.id);
}

function buildFollowUpFacetOptions(records: TargetCandidateRecord[]): CandidateFacetOption[] {
  const counts = records.reduce<Record<string, number>>((accumulator, record) => {
    const key = record.followUpStatus || "pending_outreach";
    accumulator[key] = (accumulator[key] || 0) + 1;
    return accumulator;
  }, {});
  return FOLLOW_UP_OPTIONS.map((option) => ({
    ...option,
    count: counts[option.id] || 0,
  }));
}

function buildPublicWebFacetOptions(
  records: TargetCandidateRecord[],
  latestPublicWebRunByRecordId: Map<string, TargetCandidatePublicWebRun>,
): CandidateFacetOption[] {
  const counts = records.reduce<Record<string, number>>((accumulator, record) => {
    const run = latestPublicWebRunByRecordId.get(record.id);
    const bucket = publicWebFilterBucket(run?.status || "unknown");
    accumulator[bucket] = (accumulator[bucket] || 0) + 1;
    return accumulator;
  }, {});
  return PUBLIC_WEB_FILTER_OPTIONS.map((option) => ({
    ...option,
    count: counts[option.id] || 0,
  }));
}

function targetCandidateMatchesKeyword(record: TargetCandidateRecord, keyword: string): boolean {
  const normalized = keyword.trim().toLowerCase();
  if (!normalized) {
    return true;
  }
  const corpus = [
    record.candidateName,
    record.headline,
    record.currentCompany,
    record.linkedinUrl,
    record.primaryEmail,
    record.comment,
  ]
    .join(" ")
    .toLowerCase();
  return corpus.includes(normalized);
}

function formatTargetCandidateActionError(error: unknown, fallback: string): string {
  const message = error instanceof Error ? error.message : String(error || "");
  if (/\b404\b/.test(message)) {
    return "Public Web Search 接口暂不可用，请确认后端服务已更新并重启。";
  }
  return message || fallback;
}

function numberFromSummary(summary: Record<string, unknown>, key: string): number {
  const value = summary[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function primaryLinksFromSummary(summary: Record<string, unknown>): Array<{ label: string; url: string }> {
  const raw = summary.primary_links;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return [];
  }
  return Object.entries(raw as Record<string, unknown>)
    .map(([label, url]) => ({
      label: label.replace(/_/g, " "),
      url: typeof url === "string" ? url : "",
    }))
    .filter((item) => item.url)
    .slice(0, 4);
}

function publicWebSignalUrl(signal: TargetCandidatePublicWebSignal): string {
  return signal.url || signal.sourceUrl || signal.normalizedValue || signal.value;
}

function publicWebSignalLabel(signal: TargetCandidatePublicWebSignal): string {
  if (signal.signalKind === "email_candidate") {
    return signal.normalizedValue || signal.value;
  }
  return signal.sourceTitle || publicWebLinkTypeLabel(signal.signalType);
}

function publicWebLinkTypeLabel(value: string): string {
  const labels: Record<string, string> = {
    personal_homepage: "Homepage",
    resume_url: "Resume/CV",
    github_url: "GitHub",
    x_url: "X",
    substack_url: "Substack",
    scholar_url: "Scholar",
    publication_url: "Publication",
    academic_profile: "Academic",
    linkedin_url: "LinkedIn",
    company_page: "Company",
    other: "Other",
  };
  return labels[value] || value.replace(/_/g, " ");
}

function publicWebIdentityLabel(value: string): string {
  const labels: Record<string, string> = {
    confirmed: "confirmed",
    likely_same_person: "likely",
    needs_review: "review",
    needs_ai_review: "review",
    ambiguous_identity: "ambiguous",
    not_same_person: "wrong person",
    unreviewed: "unreviewed",
  };
  return labels[value] || value || "unreviewed";
}

function publicWebWarningLabel(value: string): string {
  const labels: Record<string, string> = {
    github_repository_or_deep_link_not_profile: "repo/deep link",
    x_link_not_profile: "not X profile",
    substack_link_not_profile_or_publication: "not Substack profile",
    scholar_link_not_profile: "not Scholar profile",
    profile_link_missing_url: "missing URL",
  };
  return labels[value] || value.replace(/_/g, " ");
}

function sortedPublicWebSignals(signals: TargetCandidatePublicWebSignal[]): TargetCandidatePublicWebSignal[] {
  return [...signals].sort((left, right) => {
    const publishableDelta = Number(right.publishable) - Number(left.publishable);
    if (publishableDelta !== 0) {
      return publishableDelta;
    }
    return (right.confidenceScore || 0) - (left.confidenceScore || 0);
  });
}

function publicWebRunSortValue(run: TargetCandidatePublicWebRun): number {
  const candidates = [run.updatedAt, run.completedAt, run.startedAt];
  for (const value of candidates) {
    const timestamp = Date.parse(value || "");
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
  }
  return 0;
}

function mergePublicWebState(
  previous: TargetCandidatePublicWebSearchState,
  next: TargetCandidatePublicWebSearchState,
): TargetCandidatePublicWebSearchState {
  const batchesById = new Map(previous.batches.map((batch) => [batch.batchId, batch]));
  next.batches.forEach((batch) => {
    batchesById.set(batch.batchId, batch);
  });
  const runsById = new Map(previous.runs.map((run) => [run.runId, run]));
  next.runs.forEach((run) => {
    runsById.set(run.runId, run);
  });
  return {
    status: next.status || previous.status,
    batches: Array.from(batchesById.values()).sort(
      (left, right) => Date.parse(right.updatedAt || "") - Date.parse(left.updatedAt || ""),
    ),
    runs: Array.from(runsById.values()).sort(
      (left, right) => publicWebRunSortValue(right) - publicWebRunSortValue(left),
    ),
  };
}

function HelpBadge({ children, label }: { children: string; label: string }) {
  return (
    <span className="help-badge target-action-help" aria-label={label} tabIndex={0}>
      ?
      <span className="help-tooltip">{children}</span>
    </span>
  );
}

export function TargetCandidatesPanel() {
  const [records, setRecords] = useState<TargetCandidateRecord[]>(() => peekTargetCandidatesCache());
  const [selectedRecordIds, setSelectedRecordIds] = useState<Set<string>>(() => new Set());
  const [publicWebState, setPublicWebState] = useState<TargetCandidatePublicWebSearchState>(EMPTY_PUBLIC_WEB_STATE);
  const [publicWebLoading, setPublicWebLoading] = useState(false);
  const [publicWebActionPending, setPublicWebActionPending] = useState(false);
  const [publicWebActionMessage, setPublicWebActionMessage] = useState("");
  const [publicWebActionError, setPublicWebActionError] = useState("");
  const [expandedPublicWebRecordId, setExpandedPublicWebRecordId] = useState("");
  const [publicWebDetailsByRecordId, setPublicWebDetailsByRecordId] = useState<
    Record<string, TargetCandidatePublicWebDetail>
  >({});
  const [publicWebDetailLoadingIds, setPublicWebDetailLoadingIds] = useState<Set<string>>(() => new Set());
  const [publicWebDetailErrors, setPublicWebDetailErrors] = useState<Record<string, string>>({});
  const [exportingArchive, setExportingArchive] = useState(false);
  const [exportingPublicWebArchive, setExportingPublicWebArchive] = useState(false);
  const [exportError, setExportError] = useState("");
  const [publicWebPromotionPendingIds, setPublicWebPromotionPendingIds] = useState<Set<string>>(() => new Set());
  const [publicWebPromotionMessage, setPublicWebPromotionMessage] = useState("");
  const [filterKeyword, setFilterKeyword] = useState("");
  const [selectedFollowUpStatuses, setSelectedFollowUpStatuses] = useState<string[]>(() =>
    FOLLOW_UP_OPTIONS.map((option) => option.id),
  );
  const [selectedPublicWebStatuses, setSelectedPublicWebStatuses] = useState<string[]>(() =>
    PUBLIC_WEB_FILTER_OPTIONS.map((option) => option.id),
  );
  const attemptedHydrationIds = useRef<Set<string>>(new Set());

  const refreshPublicWebState = useCallback(async () => {
    setPublicWebLoading(true);
    try {
      const state = await getTargetCandidatePublicWebSearches({ limit: 500 });
      setPublicWebState(state);
      setPublicWebActionError("");
    } catch (error) {
      setPublicWebActionError(formatTargetCandidateActionError(error, "Public Web Search 状态刷新失败。"));
    } finally {
      setPublicWebLoading(false);
    }
  }, []);

  useEffect(() => {
    const sync = () => {
      void readTargetCandidates()
        .then((items) => {
          setRecords(items);
        })
        .catch(() => {
          setRecords([]);
        });
    };
    sync();
    window.addEventListener(targetCandidatesUpdatedEventName(), sync);
    window.addEventListener("storage", sync);
    return () => {
      window.removeEventListener(targetCandidatesUpdatedEventName(), sync);
      window.removeEventListener("storage", sync);
    };
  }, []);

  useEffect(() => {
    void refreshPublicWebState();
  }, [refreshPublicWebState]);

  useEffect(() => {
    const validIds = new Set(records.map((record) => record.id));
    setSelectedRecordIds((previous) => {
      const next = new Set(Array.from(previous).filter((recordId) => validIds.has(recordId)));
      return next.size === previous.size ? previous : next;
    });
  }, [records]);

  useEffect(() => {
    const hydrationTargets = records.filter(
      (record) =>
        record.jobId &&
        record.candidateId &&
        (!record.avatarUrl || !record.headline || !record.currentCompany || !record.primaryEmail || !record.linkedinUrl) &&
        !attemptedHydrationIds.current.has(record.id),
    );
    if (hydrationTargets.length === 0) {
      return;
    }
    hydrationTargets.forEach((record) => attemptedHydrationIds.current.add(record.id));
    const targetsByJobId = hydrationTargets.reduce<Record<string, TargetCandidateRecord[]>>((accumulator, record) => {
      const jobId = record.jobId || "";
      if (!jobId) {
        return accumulator;
      }
      accumulator[jobId] = accumulator[jobId] || [];
      accumulator[jobId].push(record);
      return accumulator;
    }, {});
    void Promise.allSettled(
      Object.entries(targetsByJobId).map(async ([jobId, jobRecords]) => {
        const detailsByCandidateId = await getCandidateDetailsBatch(
          jobRecords.map((record) => record.candidateId),
          jobId,
        );
        await Promise.allSettled(
          jobRecords.map(async (record) => {
            const detail = detailsByCandidateId[record.candidateId];
            if (!detail) {
              return;
            }
            const patch: Partial<TargetCandidateRecord> & { metadata?: Record<string, unknown> } = {};
            if (!record.headline && detail.headline) {
              patch.headline = detail.headline;
            }
            if (!record.currentCompany && detail.currentCompany) {
              patch.currentCompany = detail.currentCompany;
            }
            if (!record.avatarUrl && detail.avatarUrl) {
              patch.avatarUrl = detail.avatarUrl;
            }
            if (!record.primaryEmail && detail.primaryEmail) {
              patch.primaryEmail = detail.primaryEmail || "";
              patch.primaryEmailMetadata = detail.primaryEmailMetadata;
              patch.metadata = detail.primaryEmailMetadata
                ? { primary_email_metadata: detail.primaryEmailMetadata }
                : {};
            }
            if (!record.linkedinUrl && detail.linkedinUrl) {
              patch.linkedinUrl = detail.linkedinUrl;
            }
            if (Object.keys(patch).length > 0) {
              await updateTargetCandidate(record.id, patch);
            }
          }),
        );
      }),
    );
  }, [records]);

  const latestPublicWebRunByRecordId = useMemo(() => {
    const latest = new Map<string, TargetCandidatePublicWebRun>();
    [...publicWebState.runs]
      .sort((left, right) => publicWebRunSortValue(right) - publicWebRunSortValue(left))
      .forEach((run) => {
        if (run.recordId && !latest.has(run.recordId)) {
          latest.set(run.recordId, run);
        }
      });
    return latest;
  }, [publicWebState.runs]);

  const followUpFacetOptions = useMemo(() => buildFollowUpFacetOptions(records), [records]);
  const publicWebFacetOptions = useMemo(
    () => buildPublicWebFacetOptions(records, latestPublicWebRunByRecordId),
    [latestPublicWebRunByRecordId, records],
  );

  useEffect(() => {
    setSelectedFollowUpStatuses((current) =>
      normalizeFacetSelection(current, followUpFacetOptions, allOptionIds(followUpFacetOptions)),
    );
  }, [followUpFacetOptions]);

  useEffect(() => {
    setSelectedPublicWebStatuses((current) =>
      normalizeFacetSelection(current, publicWebFacetOptions, allOptionIds(publicWebFacetOptions)),
    );
  }, [publicWebFacetOptions]);

  const visibleRecords = useMemo(
    () =>
      records.filter((record) => {
        if (!targetCandidateMatchesKeyword(record, filterKeyword)) {
          return false;
        }
        if (!selectedFollowUpStatuses.includes(record.followUpStatus || "pending_outreach")) {
          return false;
        }
        const publicWebBucket = publicWebFilterBucket(latestPublicWebRunByRecordId.get(record.id)?.status || "unknown");
        return selectedPublicWebStatuses.includes(publicWebBucket);
      }),
    [filterKeyword, latestPublicWebRunByRecordId, records, selectedFollowUpStatuses, selectedPublicWebStatuses],
  );

  const selectedRecords = useMemo(
    () => records.filter((record) => selectedRecordIds.has(record.id)),
    [records, selectedRecordIds],
  );

  const exportScopeRecords = useMemo(
    () => (selectedRecords.length ? selectedRecords : visibleRecords),
    [selectedRecords, visibleRecords],
  );

  const exportRecordIds = useMemo(
    () => Array.from(new Set(exportScopeRecords.map((record) => record.id).filter(Boolean))),
    [exportScopeRecords],
  );

  const visibleRecordIds = useMemo(() => visibleRecords.map((record) => record.id).filter(Boolean), [visibleRecords]);
  const selectedVisibleRecordCount = useMemo(
    () => visibleRecordIds.filter((recordId) => selectedRecordIds.has(recordId)).length,
    [selectedRecordIds, visibleRecordIds],
  );

  const publicWebExportRecordIds = exportRecordIds;

  const exportScopeText = selectedRecords.length
    ? `已选 ${selectedRecords.length} 人`
    : `当前筛选 ${visibleRecords.length} 人`;

  const publicWebHasRunningWork = useMemo(
    () =>
      publicWebState.runs.some(
        (run) => run.status !== "unknown" && !PUBLIC_WEB_TERMINAL_STATUSES.has(run.status),
      ),
    [publicWebState.runs],
  );

  useEffect(() => {
    if (!publicWebHasRunningWork) {
      return undefined;
    }
    const interval = window.setInterval(() => {
      void refreshPublicWebState();
    }, 5000);
    return () => window.clearInterval(interval);
  }, [publicWebHasRunningWork, refreshPublicWebState]);

  const loadPublicWebDetail = useCallback(
    async (recordId: string) => {
      if (!recordId || publicWebDetailsByRecordId[recordId] || publicWebDetailLoadingIds.has(recordId)) {
        return;
      }
      setPublicWebDetailLoadingIds((previous) => new Set(previous).add(recordId));
      setPublicWebDetailErrors((previous) => {
        const next = { ...previous };
        delete next[recordId];
        return next;
      });
      try {
        const detail = await getTargetCandidatePublicWebDetail(recordId);
        setPublicWebDetailsByRecordId((previous) => ({ ...previous, [recordId]: detail }));
      } catch (error) {
        setPublicWebDetailErrors((previous) => ({
          ...previous,
          [recordId]: formatTargetCandidateActionError(error, "Public Web detail 加载失败。"),
        }));
      } finally {
        setPublicWebDetailLoadingIds((previous) => {
          const next = new Set(previous);
          next.delete(recordId);
          return next;
        });
      }
    },
    [publicWebDetailLoadingIds, publicWebDetailsByRecordId],
  );

  const refreshPublicWebDetail = useCallback(async (recordId: string) => {
    if (!recordId) {
      return;
    }
    setPublicWebDetailLoadingIds((previous) => new Set(previous).add(recordId));
    setPublicWebDetailErrors((previous) => {
      const next = { ...previous };
      delete next[recordId];
      return next;
    });
    try {
      const detail = await getTargetCandidatePublicWebDetail(recordId);
      setPublicWebDetailsByRecordId((previous) => ({ ...previous, [recordId]: detail }));
    } catch (error) {
      setPublicWebDetailErrors((previous) => ({
        ...previous,
        [recordId]: formatTargetCandidateActionError(error, "Public Web detail 加载失败。"),
      }));
    } finally {
      setPublicWebDetailLoadingIds((previous) => {
        const next = new Set(previous);
        next.delete(recordId);
        return next;
      });
    }
  }, []);

  const handleToggleRecordSelection = (recordId: string, checked: boolean) => {
    setSelectedRecordIds((previous) => {
      const next = new Set(previous);
      if (checked) {
        next.add(recordId);
      } else {
        next.delete(recordId);
      }
      return next;
    });
  };

  const handleToggleAllRecords = (checked: boolean) => {
    setSelectedRecordIds((previous) => {
      const visibleIds = visibleRecords.map((record) => record.id).filter(Boolean);
      if (checked) {
        return new Set([...Array.from(previous), ...visibleIds]);
      }
      const next = new Set(previous);
      visibleIds.forEach((recordId) => next.delete(recordId));
      return next;
    });
  };

  const handleStartPublicWebSearch = async () => {
    const recordIds = selectedRecords.map((record) => record.id).filter(Boolean);
    if (recordIds.length === 0 || publicWebActionPending) {
      return;
    }
    setPublicWebActionPending(true);
    setPublicWebActionMessage("");
    setPublicWebActionError("");
    try {
      const result = await startTargetCandidatePublicWebSearch({ recordIds });
      setPublicWebState((previous) =>
        mergePublicWebState(previous, {
          status: result.status,
          batches: result.batch ? [result.batch] : result.batches,
          runs: result.runs,
        }),
      );
      const queuedCount = Number(result.workerSummary.queued_worker_count || 0) || 0;
      const reusedCount = Number(result.workerSummary.reused_terminal_run_count || 0) || 0;
      setPublicWebActionMessage(
        result.status === "joined"
          ? `已加入现有 Public Web Search，${result.runs.length} 位候选人正在复用同一批任务。`
          : `已排队 ${queuedCount} 位候选人的 Public Web Search${reusedCount ? `，复用 ${reusedCount} 个已完成结果` : ""}。`,
      );
      void refreshPublicWebState();
    } catch (error) {
      setPublicWebActionError(formatTargetCandidateActionError(error, "Public Web Search 提交失败。"));
    } finally {
      setPublicWebActionPending(false);
    }
  };

  const handleTogglePublicWebDetail = (recordId: string) => {
    const next = expandedPublicWebRecordId === recordId ? "" : recordId;
    setExpandedPublicWebRecordId(next);
    if (next) {
      void loadPublicWebDetail(next);
    }
  };

  const handleExportArchive = async () => {
    if (exportRecordIds.length === 0 || exportingArchive) {
      return;
    }
    setExportingArchive(true);
    setExportError("");
    try {
      const download = await exportTargetCandidatesArchive({
        recordIds: exportRecordIds,
      });
      downloadBlobFile(download.filename || "target-candidates.zip", download.blob);
    } catch (error) {
      setExportError(formatTargetCandidateActionError(error, "LinkedIn Profile 导出失败，请稍后重试。"));
    } finally {
      setExportingArchive(false);
    }
  };

  const handleExportPublicWebArchive = async () => {
    if (publicWebExportRecordIds.length === 0 || exportingPublicWebArchive) {
      return;
    }
    setExportingPublicWebArchive(true);
    setExportError("");
    try {
      const download = await exportTargetCandidatePublicWebArchive({
        recordIds: publicWebExportRecordIds,
        mode: "promoted_only",
      });
      downloadBlobFile(download.filename || "target-candidates-public-web.zip", download.blob);
    } catch (error) {
      setExportError(formatTargetCandidateActionError(error, "Web Search 导出失败，请稍后重试。"));
    } finally {
      setExportingPublicWebArchive(false);
    }
  };

  const handlePromotePublicWebSignal = async (
    recordId: string,
    signal: TargetCandidatePublicWebSignal,
    action: "promote" | "reject" = "promote",
  ) => {
    if (!recordId || !signal.signalId || publicWebPromotionPendingIds.has(signal.signalId)) {
      return;
    }
    setPublicWebPromotionPendingIds((previous) => new Set(previous).add(signal.signalId));
    setPublicWebPromotionMessage("");
    setPublicWebActionError("");
    try {
      const result = await promoteTargetCandidatePublicWebSignal({
        recordId,
        signalId: signal.signalId,
        action,
        operator: "frontend",
      });
      if (result.detail) {
        setPublicWebDetailsByRecordId((previous) => ({ ...previous, [recordId]: result.detail as TargetCandidatePublicWebDetail }));
      } else {
        await refreshPublicWebDetail(recordId);
      }
      const refreshedRecords = await readTargetCandidates();
      setRecords(refreshedRecords);
      setPublicWebPromotionMessage(
        action === "promote"
          ? "Public Web signal 已人工确认，导出包会包含该结果。"
          : "Public Web signal 已标记为不导出。",
      );
    } catch (error) {
      setPublicWebActionError(formatTargetCandidateActionError(error, "Public Web promotion 失败。"));
    } finally {
      setPublicWebPromotionPendingIds((previous) => {
        const next = new Set(previous);
        next.delete(signal.signalId);
        return next;
      });
    }
  };

  return (
    <div className="target-candidates-panel">
      <section className="panel target-candidates-toolbar">
        <div className="target-candidates-toolbar-main">
          <div>
            <h3>目标候选人</h3>
            <p className="muted">用于沉淀后续跟进、质量评价和备注的候选人管理页。</p>
          </div>
          <div className="target-candidates-toolbar-metrics">
            <div className="metric-card metric-card-compact">
              <span className="muted">候选人数</span>
              <strong>{records.length}</strong>
            </div>
            <div className="metric-card metric-card-compact">
              <span className="muted">当前筛选</span>
              <strong>{visibleRecords.length}</strong>
            </div>
            <div className="metric-card metric-card-compact">
              <span className="muted">已选择</span>
              <strong>{selectedRecords.length}</strong>
            </div>
          </div>
        </div>

        <div className="target-candidates-action-grid">
          <div className="target-candidates-action-group">
            <div className="target-candidates-action-title">
              <span>Public Web Search</span>
              <HelpBadge label="Public Web Search 说明">
                按候选人级 run 排队，后台恢复器继续搜索、抓取和分析；默认导出不包含 raw HTML/PDF。
              </HelpBadge>
            </div>
            <div className="target-candidates-button-row">
              <button
                type="button"
                className="primary-button"
                data-testid="target-candidates-public-web-start"
                onClick={() => void handleStartPublicWebSearch()}
                disabled={selectedRecords.length === 0 || publicWebActionPending}
              >
                {publicWebActionPending
                  ? "正在提交..."
                  : selectedRecords.length
                    ? `搜索公开信息（${selectedRecords.length}）`
                    : "选择候选人后搜索公开信息"}
              </button>
              <button
                type="button"
                className="ghost-button"
                data-testid="target-candidates-public-web-refresh"
                onClick={() => void refreshPublicWebState()}
                disabled={publicWebLoading}
              >
                {publicWebLoading ? "刷新中..." : "刷新状态"}
              </button>
            </div>
            {publicWebActionMessage ? (
              <span className="target-candidates-inline-message">{publicWebActionMessage}</span>
            ) : null}
            {publicWebActionError ? (
              <span className="target-candidates-inline-message is-error">{publicWebActionError}</span>
            ) : null}
          </div>

          <div className="target-candidates-action-group">
            <div className="target-candidates-action-title">
              <span>批量导出</span>
              <span className="target-candidates-scope-chip">{exportScopeText}</span>
            </div>
            <div className="target-candidates-button-row">
              <span className="target-candidates-button-with-help">
                <button
                  type="button"
                  className="ghost-button"
                  onClick={() => void handleExportArchive()}
                  disabled={exportRecordIds.length === 0 || exportingArchive}
                >
                  {exportingArchive ? "正在打包 LinkedIn..." : "批量导出 LinkedIn Profile 信息"}
                </button>
                <HelpBadge label="LinkedIn Profile 导出说明">
                  导出当前范围内候选人的 CSV 摘要与可用 LinkedIn Profile 原始文件；Email 仍按既有高置信/可发布规则输出。
                </HelpBadge>
              </span>
              <span className="target-candidates-button-with-help">
                <button
                  type="button"
                  className="ghost-button"
                  data-testid="target-candidates-public-web-export"
                  onClick={() => void handleExportPublicWebArchive()}
                  disabled={publicWebExportRecordIds.length === 0 || exportingPublicWebArchive}
                >
                  {exportingPublicWebArchive ? "正在打包 Web Search..." : "批量导出 Web Search 信息"}
                </button>
                <HelpBadge label="Web Search 导出说明">
                  导出当前范围内已人工确认的 Public Web 邮箱、链接、model-safe evidence links 和 manifest；不包含 raw HTML/PDF/search payload。
                </HelpBadge>
              </span>
            </div>
            {exportError ? <span className="target-candidates-inline-message is-error">{exportError}</span> : null}
            {publicWebPromotionMessage ? (
              <span className="target-candidates-inline-message">{publicWebPromotionMessage}</span>
            ) : null}
          </div>
        </div>
      </section>

      {records.length > 0 ? (
        <>
        <section className="panel target-candidates-filter-panel">
          <div className="results-filter-topline target-candidates-filter-topline">
            <div className="results-search results-search-wide">
              <label className="field-label" htmlFor="target-candidates-keyword">
                目标候选人筛选
              </label>
              <input
                id="target-candidates-keyword"
                className="text-input"
                value={filterKeyword}
                onChange={(event) => setFilterKeyword(event.target.value)}
                placeholder="按姓名、公司、标题、邮箱、LinkedIn 或备注筛选"
              />
            </div>
            <label className="target-candidates-select-all">
              <input
                type="checkbox"
                checked={visibleRecordIds.length > 0 && selectedVisibleRecordCount === visibleRecordIds.length}
                ref={(node) => {
                  if (node) {
                    node.indeterminate = selectedVisibleRecordCount > 0 && selectedVisibleRecordCount < visibleRecordIds.length;
                  }
                }}
                onChange={(event) => handleToggleAllRecords(event.target.checked)}
              />
              <span>
                {selectedRecordIds.size
                  ? `已选择 ${selectedRecordIds.size} 位`
                  : `选择当前筛选 ${visibleRecords.length} 位`}
              </span>
            </label>
          </div>
          <div className="facet-dropdown-row target-candidates-facet-row">
            <FacetMultiSelect
              label="当前跟进状态"
              summary={summarizeSelectedFacet(selectedFollowUpStatuses, followUpFacetOptions, "全部状态")}
              options={followUpFacetOptions}
              selectedIds={selectedFollowUpStatuses}
              onToggle={(optionId) =>
                setSelectedFollowUpStatuses((current) =>
                  toggleFacetSelection(current, optionId, {
                    fallback: allOptionIds(followUpFacetOptions),
                  }),
                )
              }
            />
            <FacetMultiSelect
              label="Public Web 状态"
              summary={summarizeSelectedFacet(selectedPublicWebStatuses, publicWebFacetOptions, "全部状态")}
              options={publicWebFacetOptions}
              selectedIds={selectedPublicWebStatuses}
              onToggle={(optionId) =>
                setSelectedPublicWebStatuses((current) =>
                  toggleFacetSelection(current, optionId, {
                    fallback: allOptionIds(publicWebFacetOptions),
                  }),
                )
              }
            />
          </div>
        </section>
        <section className="target-candidate-grid">
          {visibleRecords.map((record) => {
            const publicWebRun = latestPublicWebRunByRecordId.get(record.id);
            const publicWebStatus = publicWebRun?.status || "unknown";
            const publicWebSummary = publicWebRun?.summary || {};
            const primaryLinks = primaryLinksFromSummary(publicWebSummary);
            const entryLinkCount = numberFromSummary(publicWebSummary, "entry_link_count");
            const fetchedDocumentCount = numberFromSummary(publicWebSummary, "fetched_document_count");
            const emailCandidateCount = numberFromSummary(publicWebSummary, "email_candidate_count");
            const recommendedEmailCount = numberFromSummary(publicWebSummary, "promotion_recommended_email_count");
            const isPublicWebExpanded = expandedPublicWebRecordId === record.id;
            const publicWebDetail = publicWebDetailsByRecordId[record.id];
            const publicWebDetailLoading = publicWebDetailLoadingIds.has(record.id);
            const publicWebDetailError = publicWebDetailErrors[record.id] || "";
            const publicWebEmailSignals = sortedPublicWebSignals(publicWebDetail?.emailCandidates || []);
            const publicWebProfileSignals = sortedPublicWebSignals(publicWebDetail?.profileLinks || []);
            return (
            <article key={record.id} className="candidate-card target-candidate-card">
              <div className="candidate-header candidate-header-with-avatar">
                <label className="target-candidate-select-box" aria-label={`选择 ${record.candidateName}`}>
                  <input
                    type="checkbox"
                    data-testid="target-candidate-select-checkbox"
                    checked={selectedRecordIds.has(record.id)}
                    onChange={(event) => handleToggleRecordSelection(record.id, event.target.checked)}
                  />
                </label>
                <Avatar name={record.candidateName} src={record.avatarUrl} size="small" />
                <div className="candidate-copy">
                  <div className="candidate-title-row">
                    <div>
                      <h4>{record.candidateName}</h4>
                      <p className="candidate-meta-line">
                        {pickCandidateRoleLine({
                          id: record.candidateId,
                          name: record.candidateName,
                          headline: record.headline || "",
                          avatarUrl: record.avatarUrl || "",
                          team: "",
                          employmentStatus: "lead",
                          confidence: "lead_only",
                          summary: "",
                          outreachLayer: 0,
                          matchedKeywords: [],
                          currentCompany: record.currentCompany || "",
                          focusAreas: [],
                          matchReasons: [],
                          education: [],
                          experience: [],
                          evidence: [],
                          primaryEmail: record.primaryEmail || "",
                        })}
                      </p>
                      {record.primaryEmail ? <p className="candidate-email-line">{record.primaryEmail}</p> : null}
                    </div>
                  </div>
                </div>
              </div>

              <div className="target-candidate-fields">
                <label className="review-control-block">
                  <span className="field-label">当前跟进状态</span>
                  <select
                    value={record.followUpStatus}
                    onChange={(event) =>
                      void updateTargetCandidate(record.id, {
                        followUpStatus: event.target.value as TargetCandidateFollowUpStatus,
                      })
                    }
                  >
                    {FOLLOW_UP_OPTIONS.map((option) => (
                      <option key={option.id} value={option.id}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="review-control-block">
                  <span className="field-label">质量评价分数</span>
                  <input
                    className="text-input"
                    type="number"
                    min={1}
                    max={10}
                    value={record.qualityScore ?? ""}
                    placeholder="1-10"
                    onChange={(event) =>
                      void updateTargetCandidate(record.id, {
                        qualityScore: event.target.value ? Number(event.target.value) : null,
                      })
                    }
                  />
                </label>

                <label className="review-control-block target-candidate-comment">
                  <span className="field-label">备注 Comment</span>
                  <textarea
                    className="message-editor"
                    rows={4}
                    value={record.comment}
                    placeholder="记录跟进信息、沟通反馈或后续判断。"
                    onChange={(event) =>
                      void updateTargetCandidate(record.id, {
                        comment: event.target.value,
                      })
                    }
                  />
                </label>
              </div>

              <div className="target-candidate-public-web-card">
                <div className="target-candidate-public-web-card__header">
                  <span className={`keyword-chip public-web-status-chip ${publicWebStatusTone(publicWebStatus)}`}>
                    <span data-testid="target-candidate-public-web-status">
                    {publicWebStatusLabel(publicWebStatus)}
                    </span>
                  </span>
                  {publicWebRun?.updatedAt ? (
                    <span className="target-candidates-export-note">更新 {publicWebRun.updatedAt}</span>
                  ) : null}
                </div>
                {publicWebRun ? (
                  <>
                    <div className="target-candidate-public-web-metrics">
                      <span>链接 {entryLinkCount}</span>
                      <span>文档 {fetchedDocumentCount}</span>
                      <span>Email {emailCandidateCount}</span>
                      {recommendedEmailCount ? <span>推荐 {recommendedEmailCount}</span> : null}
                    </div>
                    {primaryLinks.length ? (
                      <div className="target-candidate-public-web-links">
                        {primaryLinks.map((link) => (
                          <a key={`${record.id}-${link.label}-${link.url}`} href={link.url} target="_blank" rel="noreferrer">
                            {link.label}
                          </a>
                        ))}
                      </div>
                    ) : (
                      <span className="target-candidates-export-note">暂无已确认 primary links。</span>
                    )}
                    <div className="target-candidate-public-web-detail-actions">
                      <button
                        type="button"
                        className="ghost-button candidate-action-button"
                        onClick={() => handleTogglePublicWebDetail(record.id)}
                      >
                        {isPublicWebExpanded ? "收起公开信息详情" : "查看公开信息详情"}
                      </button>
                    </div>
                    {isPublicWebExpanded ? (
                      <div className="target-candidate-public-web-detail">
                        {publicWebDetailLoading ? (
                          <span className="target-candidates-export-note">正在加载 Public Web detail...</span>
                        ) : null}
                        {publicWebDetailError ? (
                          <span className="target-candidates-export-note">{publicWebDetailError}</span>
                        ) : null}
                        {publicWebDetail ? (
                          <>
                            <div className="target-candidate-public-web-detail-grid">
                              <div className="target-candidate-public-web-signal-group">
                                <h5>Email candidates</h5>
                                {publicWebEmailSignals.length ? (
                                  <div className="target-candidate-public-web-signal-list">
                                    {publicWebEmailSignals.slice(0, 6).map((signal) => (
                                      <div key={signal.signalId || `${record.id}-${signal.normalizedValue}`} className="target-candidate-public-web-signal-row">
                                        <div>
                                          <strong>{signal.normalizedValue || signal.value}</strong>
                                          <span>{signal.emailType || "unknown"} · {publicWebIdentityLabel(signal.identityMatchLabel)}</span>
                                        </div>
                                        <div className="target-candidate-public-web-signal-chips">
                                          {signal.publishable ? <span className="keyword-chip public-web-status-chip is-success">publishable</span> : null}
                                          {signal.suppressionReason ? <span className="keyword-chip public-web-status-chip is-warning">{signal.suppressionReason}</span> : null}
                                          {signal.promotionStatus ? <span className="keyword-chip">{signal.promotionStatus.replace(/_/g, " ")}</span> : null}
                                        </div>
                                        {signal.evidenceExcerpt ? <p>{signal.evidenceExcerpt}</p> : null}
                                        {signal.sourceUrl ? (
                                          <a href={signal.sourceUrl} target="_blank" rel="noreferrer">
                                            {signal.sourceDomain || signal.sourceUrl}
                                          </a>
                                        ) : null}
                                        <div className="target-candidate-public-web-detail-actions">
                                          <button
                                            type="button"
                                            className="ghost-button candidate-action-button"
                                            onClick={() => void handlePromotePublicWebSignal(record.id, signal, "promote")}
                                            disabled={
                                              !signal.publishable ||
                                              signal.promotionStatus === "manually_promoted" ||
                                              publicWebPromotionPendingIds.has(signal.signalId)
                                            }
                                          >
                                            {signal.promotionStatus === "manually_promoted"
                                              ? "已确认邮箱"
                                              : publicWebPromotionPendingIds.has(signal.signalId)
                                                ? "正在确认..."
                                                : "确认并设为 primary email"}
                                          </button>
                                          <button
                                            type="button"
                                            className="ghost-button candidate-action-button"
                                            onClick={() => void handlePromotePublicWebSignal(record.id, signal, "reject")}
                                            disabled={
                                              signal.promotionStatus === "manually_rejected" ||
                                              publicWebPromotionPendingIds.has(signal.signalId)
                                            }
                                          >
                                            {signal.promotionStatus === "manually_rejected" ? "已排除" : "不导出"}
                                          </button>
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                ) : (
                                  <span className="target-candidates-export-note">No email candidates.</span>
                                )}
                              </div>
                              <div className="target-candidate-public-web-signal-group">
                                <h5>Profile and evidence links</h5>
                                {publicWebProfileSignals.length ? (
                                  <div className="target-candidate-public-web-signal-list">
                                    {publicWebProfileSignals.slice(0, 10).map((signal) => {
                                      const url = publicWebSignalUrl(signal);
                                      return (
                                        <div key={signal.signalId || `${record.id}-${url}`} className="target-candidate-public-web-signal-row">
                                          <div>
                                            <strong>{publicWebLinkTypeLabel(signal.signalType)}</strong>
                                            <span>{publicWebIdentityLabel(signal.identityMatchLabel)} · {signal.sourceDomain || "unknown source"}</span>
                                          </div>
                                          <div className="target-candidate-public-web-signal-chips">
                                            {signal.cleanProfileLink ? (
                                              <span className="keyword-chip public-web-status-chip is-success">clean URL</span>
                                            ) : (
                                              <span className="keyword-chip public-web-status-chip is-warning">review URL shape</span>
                                            )}
                                            {signal.linkShapeWarnings.map((warning) => (
                                              <span key={`${url}-${warning}`} className="keyword-chip public-web-status-chip is-warning">
                                                {publicWebWarningLabel(warning)}
                                              </span>
                                            ))}
                                          </div>
                                          {url ? (
                                            <a href={url} target="_blank" rel="noreferrer">
                                              {publicWebSignalLabel(signal)}
                                            </a>
                                          ) : null}
                                          {signal.evidenceExcerpt ? <p>{signal.evidenceExcerpt}</p> : null}
                                          <div className="target-candidate-public-web-detail-actions">
                                            <button
                                              type="button"
                                              className="ghost-button candidate-action-button"
                                              onClick={() => void handlePromotePublicWebSignal(record.id, signal, "promote")}
                                              disabled={
                                                !signal.publishable ||
                                                !signal.cleanProfileLink ||
                                                signal.promotionStatus === "manually_promoted" ||
                                                publicWebPromotionPendingIds.has(signal.signalId)
                                              }
                                            >
                                              {signal.promotionStatus === "manually_promoted"
                                                ? "已确认链接"
                                                : publicWebPromotionPendingIds.has(signal.signalId)
                                                  ? "正在确认..."
                                                  : "确认导出链接"}
                                            </button>
                                            <button
                                              type="button"
                                              className="ghost-button candidate-action-button"
                                              onClick={() => void handlePromotePublicWebSignal(record.id, signal, "reject")}
                                              disabled={
                                                signal.promotionStatus === "manually_rejected" ||
                                                publicWebPromotionPendingIds.has(signal.signalId)
                                              }
                                            >
                                              {signal.promotionStatus === "manually_rejected" ? "已排除" : "不导出"}
                                            </button>
                                          </div>
                                        </div>
                                      );
                                    })}
                                  </div>
                                ) : (
                                  <span className="target-candidates-export-note">No profile links.</span>
                                )}
                              </div>
                            </div>
                            {publicWebDetail.evidenceLinks.length ? (
                              <div className="target-candidate-public-web-evidence-links">
                                {publicWebDetail.evidenceLinks.filter((evidence) => evidence.sourceUrl).slice(0, 8).map((evidence) => (
                                  <a key={`${record.id}-${evidence.sourceUrl}`} href={evidence.sourceUrl} target="_blank" rel="noreferrer">
                                    {evidence.sourceDomain || evidence.sourceUrl}
                                  </a>
                                ))}
                              </div>
                            ) : null}
                          </>
                        ) : null}
                      </div>
                    ) : null}
                    {publicWebRun.lastError ? (
                      <span className="target-candidates-export-note">{publicWebRun.lastError}</span>
                    ) : null}
                  </>
                ) : (
                  <span className="target-candidates-export-note">
                    尚未触发候选人级 Public Web Search。
                  </span>
                )}
              </div>

              <div className="candidate-actions candidate-actions-compact">
                <span className="keyword-chip">{followUpStatusLabel(record.followUpStatus)}</span>
                {record.linkedinUrl ? (
                  <a className="ghost-button candidate-action-button" href={record.linkedinUrl} target="_blank" rel="noreferrer">
                    打开 LinkedIn
                  </a>
                ) : null}
              </div>
            </article>
            );
          })}
          {visibleRecords.length === 0 ? (
            <div className="empty-state target-candidates-empty-filter">
              <p>当前筛选条件下没有目标候选人。</p>
              <span>可以放宽关键词、跟进状态或 Public Web 状态筛选后再查看。</span>
            </div>
          ) : null}
        </section>
        </>
      ) : (
        <section className="panel">
          <div className="empty-state">
            <p>当前还没有加入目标候选人的记录。</p>
            <span>你可以在候选人结果看板里把值得持续跟进的人加入这里。</span>
          </div>
        </section>
      )}
    </div>
  );
}
