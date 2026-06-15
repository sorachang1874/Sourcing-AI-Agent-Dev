import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Avatar } from "./Avatar";
import { FacetMultiSelect } from "./FacetMultiSelect";
import {
  cancelTargetCandidatePublicWebSearch,
  exportProjectionCandidatesArchive,
  exportTargetCandidatePublicWebArchive,
  getCandidateDetailsBatch,
  getTargetCandidatePublicWebDetail,
  getTargetCandidatePublicWebSearches,
  peekTargetCandidatesCache,
  promoteTargetCandidatePublicWebSignal,
  retryTargetCandidatePublicWebSearch,
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
import { formatWorkflowTimestamp } from "../lib/time";
import type {
  TargetCandidateFollowUpStatus,
  TargetCandidatePublicWebBatch,
  TargetCandidatePublicWebDetail,
  TargetCandidatePublicWebPromotion,
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

type PublicWebExportMode = "promoted_only" | "promoted_and_publishable";

const EMPTY_PUBLIC_WEB_STATE: TargetCandidatePublicWebSearchState = {
  status: "idle",
  batches: [],
  runs: [],
  phaseCommandsByRunId: {},
};

interface TargetCandidateEditDraft {
  followUpStatus: TargetCandidateFollowUpStatus;
  qualityScoreText: string;
  comment: string;
}

interface PublicWebDetailCacheEntry {
  latestRunId: string;
  detail: TargetCandidatePublicWebDetail;
}

function targetCandidateQualityScoreText(record: TargetCandidateRecord): string {
  return record.qualityScore === null || !Number.isFinite(record.qualityScore) ? "" : String(record.qualityScore);
}

function targetCandidateDraftFromRecord(record: TargetCandidateRecord): TargetCandidateEditDraft {
  return {
    followUpStatus: record.followUpStatus || "pending_outreach",
    qualityScoreText: targetCandidateQualityScoreText(record),
    comment: record.comment || "",
  };
}

function parseTargetCandidateQualityScore(value: string): { qualityScore: number | null; error: string } {
  const trimmed = value.trim();
  if (!trimmed) {
    return { qualityScore: null, error: "" };
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) {
    return { qualityScore: null, error: "质量评价分数需要是有效数字。" };
  }
  if (parsed < 0 || parsed > 100) {
    return { qualityScore: null, error: "质量评价分数需要在 0-100 之间。" };
  }
  return { qualityScore: parsed, error: "" };
}

function targetCandidateDraftIsDirty(record: TargetCandidateRecord, draft: TargetCandidateEditDraft): boolean {
  const parsedQualityScore = parseTargetCandidateQualityScore(draft.qualityScoreText);
  const normalizedDraftQualityScore = parsedQualityScore.error ? draft.qualityScoreText.trim() : parsedQualityScore.qualityScore;
  const normalizedRecordQualityScore = record.qualityScore === null ? null : record.qualityScore;
  return (
    draft.followUpStatus !== (record.followUpStatus || "pending_outreach") ||
    normalizedDraftQualityScore !== normalizedRecordQualityScore ||
    draft.comment !== (record.comment || "")
  );
}

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
  if (status === "documents_fetched") {
    return "文档已取回，待分析";
  }
  if (status === "analyzing") {
    return "分析中";
  }
  if (status === "adjudication_completed") {
    return "判定完成，正在生成摘要";
  }
  if (status === "analysis_completed") {
    return "分析完成，正在保存公开信息结果";
  }
  if (status === "completed") {
    return "已完成";
  }
  if (status === "completed_with_errors") {
    return "已完成，需复核";
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

function publicWebPhaseMetricsFromSummary(summary: Record<string, unknown>): Record<string, unknown> {
  const metrics = summary.phase_metrics;
  return metrics && typeof metrics === "object" && !Array.isArray(metrics) ? (metrics as Record<string, unknown>) : {};
}

function numberFromMetrics(metrics: Record<string, unknown>, key: string): number {
  const value = metrics[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function publicWebPhaseMetricLine(metrics: Record<string, unknown>, status: TargetCandidatePublicWebStatus): string {
  if (!Object.keys(metrics).length) {
    return "";
  }
  const pending = numberFromMetrics(metrics, "pending_task_count");
  const fetched = numberFromMetrics(metrics, "fetched_task_count");
  const submitted = numberFromMetrics(metrics, "submitted_task_count");
  const timedOut = numberFromMetrics(metrics, "timeout_task_count");
  const signals = numberFromMetrics(metrics, "signal_materialized_count");
  const pollCount = numberFromMetrics(metrics, "ready_poll_count");
  const providerPendingDeferred = numberFromMetrics(metrics, "provider_pending_deferred_count");
  if (!PUBLIC_WEB_TERMINAL_STATUSES.has(status)) {
    if (status === "documents_fetched") {
      const fetchedDocuments = numberFromMetrics(metrics, "fetched_document_count");
      return `文档已取回 ${fetchedDocuments}，等待 AI 判定`;
    }
    if (status === "adjudication_completed") {
      const links = numberFromMetrics(metrics, "adjudicated_profile_link_count");
      const emails = numberFromMetrics(metrics, "adjudicated_email_candidate_count");
      return `AI 判定完成，待生成可审核公开信息 ${links + emails}`;
    }
    if (status === "analysis_completed") {
      const requiredSignals = numberFromMetrics(metrics, "signal_materialization_required_count");
      const materializedSignals = numberFromMetrics(metrics, "signal_materialized_count");
      if (requiredSignals === 0) {
        return "分析完成，未发现可审核公开信息";
      }
      return `分析完成，正在保存公开信息结果 ${materializedSignals}/${requiredSignals || materializedSignals}`;
    }
    if (pending > 0) {
      if (providerPendingDeferred > 0) {
        return `DataForSEO 仍在处理 ${pending} 个 query，已取回 ${fetched}/${submitted || pending}；可以稍后刷新，本次等待不会影响已确认公开信息`;
      }
      return `远程搜索按 query 返回：已取回 ${fetched}/${submitted || pending}，待返回 ${pending}，轮询 ${pollCount} 次`;
    }
    if (fetched > 0) {
      return `搜索结果已取回 ${fetched}/${submitted || fetched}，正在分析并整理公开信息`;
    }
  }
  if (timedOut > 0) {
    return `搜索结果已取回 ${fetched}/${submitted || fetched}，${timedOut} 个 query 超时未返回`;
  }
  if (metrics.model_fallback_used === true) {
    return "AI 判定失败，未生成新的可审核公开信息候选；已确认公开信息会继续保留";
  }
  if (signals > 0) {
    const primaryLinks = numberFromMetrics(metrics, "primary_link_count");
    return primaryLinks > 0
      ? `已生成可审核公开信息 ${signals} 条，已确认主页 ${primaryLinks} 条`
      : `已生成可审核公开信息 ${signals} 条，未自动确认主页`;
  }
  if (PUBLIC_WEB_TERMINAL_STATUSES.has(status)) {
    return "搜索已完成，未发现可审核的高置信公开信息";
  }
  return "";
}

function publicWebPhaseCommandLine(run: TargetCandidatePublicWebRun | undefined): string {
  return run?.phaseCommandDisplayLine || "";
}

function publicWebRunReviewLine(
  metrics: Record<string, unknown>,
  status: TargetCandidatePublicWebStatus,
  primaryLinkCount: number,
  lastError = "",
): string {
  if (status === "completed_with_errors" || status === "needs_review") {
    if (lastError) {
      return lastError;
    }
    if (primaryLinkCount === 0) {
      return "搜索已完成，但没有达到可自动确认的公开主页质量阈值；建议查看详情后人工判断，重复同参数搜索通常不会带来明显增量。";
    }
    if (metrics.service_guardrail_violation_detected === true) {
      return "搜索已完成，但公开信息处理存在异常；建议查看详情。";
    }
    return "搜索已完成，但部分公开信息需要人工复核。";
  }
  return lastError;
}

function publicWebBatchIssueLine(metrics: Record<string, unknown>): string {
  if (!Object.keys(metrics).length) {
    return "";
  }
  const fragments: string[] = [];
  const pendingRuns = numberFromMetrics(metrics, "remote_search_pending_run_count");
  const signalGaps = numberFromMetrics(metrics, "unmaterialized_signal_gap_count");
  const partialFailures = numberFromMetrics(metrics, "partial_failure_count");
  const metricErrors = numberFromMetrics(metrics, "runs_with_metric_errors_count");
  const missingMetrics = numberFromMetrics(metrics, "missing_phase_metric_count");
  const terminalMaterializationViolations = numberFromMetrics(metrics, "completed_without_materialized_signals_count");
  if (pendingRuns > 0) {
    fragments.push(`${pendingRuns} 人仍在等待远程搜索结果`);
  }
  if (signalGaps > 0) {
    fragments.push(`${signalGaps} 人的公开信息仍在整理`);
  }
  if (partialFailures > 0) {
    fragments.push(`${partialFailures} 人处理异常`);
  }
  if (metricErrors > 0) {
    fragments.push(`${metricErrors} 人检索或抓取异常`);
  }
  if (missingMetrics > 0) {
    fragments.push(`${missingMetrics} 人状态信息不完整`);
  }
  if (terminalMaterializationViolations > 0) {
    fragments.push(`${terminalMaterializationViolations} 人未生成可审核公开信息`);
  }
  if (fragments.length) {
    return `公开信息处理有异常：${fragments.join("；")}。建议刷新或稍后重试。`;
  }
  if (metrics.service_guardrail_violation_detected === true) {
    return "公开信息处理有异常，建议刷新或稍后重试。";
  }
  return "";
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

function publicWebPromotionValue(promotion: TargetCandidatePublicWebPromotion): string {
  return (
    promotion.newValue ||
    promotion.normalizedValue ||
    promotion.value ||
    promotion.url ||
    promotion.sourceUrl
  );
}

function publicWebPromotionLabel(promotion: TargetCandidatePublicWebPromotion): string {
  if (promotion.signalKind === "email_candidate") {
    return promotion.emailType || "Email";
  }
  return publicWebLinkTypeLabel(promotion.signalType);
}

function publicWebLinkTypeLabel(value: string): string {
  const labels: Record<string, string> = {
    personal_homepage: "个人主页",
    resume_url: "简历/CV",
    github_url: "GitHub",
    x_url: "X",
    substack_url: "Substack",
    scholar_url: "Google Scholar",
    publication_url: "论文/文章",
    academic_profile: "学术主页",
    linkedin_url: "LinkedIn",
    company_page: "公司页面",
    other: "其他",
  };
  return labels[value] || value.replace(/_/g, " ");
}

function publicWebIdentityLabel(value: string): string {
  const labels: Record<string, string> = {
    confirmed: "已确认",
    likely_same_person: "可能匹配",
    needs_review: "待复核",
    needs_ai_review: "待复核",
    ambiguous_identity: "身份不确定",
    not_same_person: "不是同一人",
    unreviewed: "未审核",
  };
  return labels[value] || value || "未审核";
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

function publicWebSignalOverrideReason(signal: TargetCandidatePublicWebSignal): string {
  if (!signal.publishable) {
    return signal.suppressionReason || "signal_not_publishable";
  }
  if (signal.signalKind === "profile_link" && !signal.cleanProfileLink) {
    return signal.linkShapeWarnings[0] || "link_shape_not_clean";
  }
  return "";
}

function publicWebSignalNeedsOverride(signal: TargetCandidatePublicWebSignal): boolean {
  return Boolean(publicWebSignalOverrideReason(signal));
}

type PublicWebSignalReviewStatus = "review" | "confirmed" | "excluded";

function publicWebSignalReviewStatus(signal: TargetCandidatePublicWebSignal): PublicWebSignalReviewStatus {
  if (signal.promotionStatus === "manually_promoted") {
    return "confirmed";
  }
  if (signal.promotionStatus === "manually_rejected") {
    return "excluded";
  }
  return "review";
}

function publicWebDetailLatestRunId(detail: TargetCandidatePublicWebDetail | null | undefined): string {
  const latestRun = detail?.latestRun;
  if (latestRun && typeof latestRun === "object" && !Array.isArray(latestRun)) {
    const latestRunId = String(latestRun.run_id || latestRun.runId || "").trim();
    if (latestRunId) {
      return latestRunId;
    }
  }
  return "";
}

function publicWebDetailCacheMatches(entry: PublicWebDetailCacheEntry | undefined, expectedRunId: string): boolean {
  if (!entry) {
    return false;
  }
  return Boolean(expectedRunId) && entry.latestRunId === expectedRunId;
}

function publicWebSignalsForRun(
  signals: TargetCandidatePublicWebSignal[],
  latestRunId: string,
): TargetCandidatePublicWebSignal[] {
  if (!latestRunId) {
    return [];
  }
  return signals.filter((signal) => signal.runId === latestRunId);
}

function publicWebReviewableSignals(signals: TargetCandidatePublicWebSignal[]): TargetCandidatePublicWebSignal[] {
  return signals.filter((signal) => publicWebSignalReviewStatus(signal) === "review");
}

function targetCandidateWorkspaceId(record: TargetCandidateRecord): string {
  return (record.workspaceId || "").trim();
}

function uniqueWorkspaceIdForRecords(records: TargetCandidateRecord[]): string {
  const workspaceIds = records.map((record) => targetCandidateWorkspaceId(record));
  if (workspaceIds.length === 0 || workspaceIds.some((workspaceId) => !workspaceId)) {
    return "";
  }
  const uniqueWorkspaceIds = Array.from(new Set(workspaceIds));
  return uniqueWorkspaceIds.length === 1 ? uniqueWorkspaceIds[0] : "";
}

function latestPublicWebRunIdForRecord(runs: TargetCandidatePublicWebRun[], recordId: string): string {
  return (
    [...runs]
      .filter((run) => run.recordId === recordId)
      .sort(comparePublicWebRunsByCurrentOrder)[0]?.runId || ""
  );
}

function publicWebSignalReviewStatusLabel(status: PublicWebSignalReviewStatus): string {
  if (status === "confirmed") {
    return "已确认并导出";
  }
  if (status === "excluded") {
    return "已排除";
  }
  return "待复核";
}

function publicWebSignalReviewTone(status: PublicWebSignalReviewStatus): string {
  if (status === "confirmed") {
    return "is-success";
  }
  if (status === "excluded") {
    return "is-muted";
  }
  return "is-warning";
}

function publicWebSignalReviewRank(signal: TargetCandidatePublicWebSignal): number {
  const reviewStatus = publicWebSignalReviewStatus(signal);
  if (reviewStatus === "confirmed") {
    return 0;
  }
  if (signal.publishable && signal.identityMatchLabel === "confirmed") {
    return 1;
  }
  if (signal.publishable && signal.identityMatchLabel === "likely_same_person") {
    return 2;
  }
  if (signal.publishable) {
    return 3;
  }
  if (reviewStatus === "review") {
    return 4;
  }
  return 5;
}

function sortedPublicWebSignals(signals: TargetCandidatePublicWebSignal[]): TargetCandidatePublicWebSignal[] {
  return [...signals].sort((left, right) => {
    const rankDelta = publicWebSignalReviewRank(left) - publicWebSignalReviewRank(right);
    if (rankDelta !== 0) {
      return rankDelta;
    }
    const publishableDelta = Number(right.publishable) - Number(left.publishable);
    if (publishableDelta !== 0) {
      return publishableDelta;
    }
    return (right.confidenceScore || 0) - (left.confidenceScore || 0);
  });
}

function publicWebCreatedAtTimestamp(value: string | undefined): number | null {
  const timestamp = Date.parse(value || "");
  if (Number.isFinite(timestamp)) {
    return timestamp;
  }
  return null;
}

function comparePublicWebCreatedAt(leftCreatedAt: string | undefined, rightCreatedAt: string | undefined): number {
  const leftTimestamp = publicWebCreatedAtTimestamp(leftCreatedAt);
  const rightTimestamp = publicWebCreatedAtTimestamp(rightCreatedAt);
  if (leftTimestamp === null || rightTimestamp === null) {
    return 0;
  }
  if (leftTimestamp === rightTimestamp) {
    return 0;
  }
  return rightTimestamp - leftTimestamp;
}

function comparePublicWebRunsByCurrentOrder(
  left: TargetCandidatePublicWebRun,
  right: TargetCandidatePublicWebRun,
): number {
  return comparePublicWebCreatedAt(left.createdAt, right.createdAt);
}

function comparePublicWebBatchesByCurrentOrder(
  left: TargetCandidatePublicWebBatch,
  right: TargetCandidatePublicWebBatch,
): number {
  return comparePublicWebCreatedAt(left.createdAt, right.createdAt);
}

function publicWebRunAllowedActions(run: TargetCandidatePublicWebRun | undefined): string[] {
  const allowedActions = run?.runControlState?.allowed_actions;
  return Array.isArray(allowedActions) ? allowedActions.map((item) => String(item || "").trim()).filter(Boolean) : [];
}

function publicWebRunAllows(run: TargetCandidatePublicWebRun | undefined, action: "cancel" | "retry"): boolean {
  return Boolean(run?.workspaceId?.trim()) && publicWebRunAllowedActions(run).includes(action);
}

function mergePublicWebState(
  previous: TargetCandidatePublicWebSearchState,
  next: TargetCandidatePublicWebSearchState,
): TargetCandidatePublicWebSearchState {
  const batchesById = new Map(next.batches.map((batch) => [batch.batchId, batch]));
  previous.batches.forEach((batch) => {
    if (batchesById.has(batch.batchId)) {
      return;
    }
    batchesById.set(batch.batchId, batch);
  });
  const runsById = new Map(next.runs.map((run) => [run.runId, run]));
  previous.runs.forEach((run) => {
    if (runsById.has(run.runId)) {
      return;
    }
    runsById.set(run.runId, run);
  });
  return {
    status: next.status || previous.status,
    batches: Array.from(batchesById.values()).sort(comparePublicWebBatchesByCurrentOrder),
    runs: Array.from(runsById.values()).sort(comparePublicWebRunsByCurrentOrder),
    phaseCommandsByRunId: {
      ...previous.phaseCommandsByRunId,
      ...next.phaseCommandsByRunId,
    },
  };
}

function HelpBadge({ children, className = "", label }: { children: string; className?: string; label: string }) {
  return (
    <span className={`help-badge target-action-help${className ? ` ${className}` : ""}`} aria-label={label} tabIndex={0}>
      ?
      <span className="help-tooltip">{children}</span>
    </span>
  );
}

interface TargetCandidatesPanelProps {
  sourceCollectionId?: string;
}

export function TargetCandidatesPanel({ sourceCollectionId = "" }: TargetCandidatesPanelProps) {
  const targetCandidateScope = useMemo(
    () => ({
      sourceCollectionId: sourceCollectionId.trim(),
    }),
    [sourceCollectionId],
  );
  const [records, setRecords] = useState<TargetCandidateRecord[]>(() => peekTargetCandidatesCache(targetCandidateScope));
  const [selectedRecordIds, setSelectedRecordIds] = useState<Set<string>>(() => new Set());
  const [publicWebState, setPublicWebState] = useState<TargetCandidatePublicWebSearchState>(EMPTY_PUBLIC_WEB_STATE);
  const [publicWebLoading, setPublicWebLoading] = useState(false);
  const [publicWebActionPending, setPublicWebActionPending] = useState(false);
  const [publicWebRunActionPendingIds, setPublicWebRunActionPendingIds] = useState<Set<string>>(() => new Set());
  const [publicWebActionMessage, setPublicWebActionMessage] = useState("");
  const [publicWebActionError, setPublicWebActionError] = useState("");
  const [activePublicWebDetailRecordId, setActivePublicWebDetailRecordId] = useState("");
  const [publicWebDetailsByRecordId, setPublicWebDetailsByRecordId] = useState<
    Record<string, PublicWebDetailCacheEntry>
  >({});
  const [publicWebDetailLoadingIds, setPublicWebDetailLoadingIds] = useState<Set<string>>(() => new Set());
  const [publicWebDetailErrors, setPublicWebDetailErrors] = useState<Record<string, string>>({});
  const [exportingArchive, setExportingArchive] = useState(false);
  const [exportingPublicWebArchive, setExportingPublicWebArchive] = useState(false);
  const [publicWebExportMode, setPublicWebExportMode] = useState<PublicWebExportMode>("promoted_only");
  const [exportError, setExportError] = useState("");
  const [exportNotice, setExportNotice] = useState("");
  const [publicWebPromotionPendingIds, setPublicWebPromotionPendingIds] = useState<Set<string>>(() => new Set());
  const [publicWebPromotionMessage, setPublicWebPromotionMessage] = useState("");
  const [expandedPublicWebSignalGroups, setExpandedPublicWebSignalGroups] = useState<Set<string>>(() => new Set());
  const [publicWebEvidenceExpanded, setPublicWebEvidenceExpanded] = useState(false);
  const [targetCandidateDraftsByRecordId, setTargetCandidateDraftsByRecordId] = useState<
    Record<string, TargetCandidateEditDraft>
  >({});
  const [targetCandidateEditSavingIds, setTargetCandidateEditSavingIds] = useState<Set<string>>(() => new Set());
  const [targetCandidateEditErrors, setTargetCandidateEditErrors] = useState<Record<string, string>>({});
  const [targetCandidateEditSavedAtByRecordId, setTargetCandidateEditSavedAtByRecordId] = useState<
    Record<string, number>
  >({});
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
      const scopedRecordIds = records.map((record) => record.id).filter(Boolean);
      const scopedWorkspaceId = scopedRecordIds.length ? uniqueWorkspaceIdForRecords(records) : "";
      if (targetCandidateScope.sourceCollectionId && scopedRecordIds.length === 0) {
        setPublicWebState(EMPTY_PUBLIC_WEB_STATE);
        setPublicWebDetailsByRecordId({});
        setPublicWebDetailErrors({});
        setPublicWebDetailLoadingIds(new Set());
        setActivePublicWebDetailRecordId("");
        setPublicWebActionError("");
        return;
      }
      if (scopedRecordIds.length === 0) {
        setPublicWebState(EMPTY_PUBLIC_WEB_STATE);
        setPublicWebDetailsByRecordId({});
        setPublicWebDetailErrors({});
        setPublicWebDetailLoadingIds(new Set());
        setActivePublicWebDetailRecordId("");
        setPublicWebActionError("");
        return;
      }
      if (scopedRecordIds.length > 0 && !scopedWorkspaceId) {
        setPublicWebState(EMPTY_PUBLIC_WEB_STATE);
        setPublicWebDetailsByRecordId({});
        setPublicWebDetailErrors({});
        setPublicWebDetailLoadingIds(new Set());
        setActivePublicWebDetailRecordId("");
        setPublicWebActionError("Public Web Search 缺少 workspace 或暂不支持跨 workspace 批量刷新。");
        return;
      }
      const state = await getTargetCandidatePublicWebSearches({
        recordIds: scopedRecordIds,
        workspaceId: scopedWorkspaceId,
        limit: Math.min(1000, Math.max(100, scopedRecordIds.length)),
      });
      setPublicWebState(state);
      setPublicWebActionError("");
    } catch (error) {
      setPublicWebActionError(formatTargetCandidateActionError(error, "Public Web Search 状态刷新失败。"));
    } finally {
      setPublicWebLoading(false);
    }
  }, [records, targetCandidateScope]);

  useEffect(() => {
    const sync = () => {
      void readTargetCandidates(targetCandidateScope)
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
  }, [targetCandidateScope]);

  useEffect(() => {
    void refreshPublicWebState();
  }, [refreshPublicWebState]);

  useEffect(() => {
    const validIds = new Set(records.map((record) => record.id));
    setSelectedRecordIds((previous) => {
      const next = new Set(Array.from(previous).filter((recordId) => validIds.has(recordId)));
      return next.size === previous.size ? previous : next;
    });
    setTargetCandidateDraftsByRecordId((previous) => {
      const next: Record<string, TargetCandidateEditDraft> = {};
      records.forEach((record) => {
        const currentDraft = previous[record.id];
        next[record.id] =
          currentDraft && targetCandidateDraftIsDirty(record, currentDraft)
            ? currentDraft
            : targetCandidateDraftFromRecord(record);
      });
      return next;
    });
    setTargetCandidateEditErrors((previous) => {
      const next = Object.fromEntries(Object.entries(previous).filter(([recordId]) => validIds.has(recordId)));
      return Object.keys(next).length === Object.keys(previous).length ? previous : next;
    });
    setTargetCandidateEditSavedAtByRecordId((previous) => {
      const next = Object.fromEntries(Object.entries(previous).filter(([recordId]) => validIds.has(recordId)));
      return Object.keys(next).length === Object.keys(previous).length ? previous : next;
    });
  }, [records, targetCandidateScope]);

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
              await updateTargetCandidate(record.id, patch, targetCandidateScope);
            }
          }),
        );
      }),
    );
  }, [records, targetCandidateScope]);

  const latestPublicWebRunByRecordId = useMemo(() => {
    const latest = new Map<string, TargetCandidatePublicWebRun>();
    [...publicWebState.runs]
      .sort(comparePublicWebRunsByCurrentOrder)
      .forEach((run) => {
        if (run.recordId && !latest.has(run.recordId)) {
          latest.set(run.recordId, run);
        }
      });
    return latest;
  }, [publicWebState.runs]);

  useEffect(() => {
    setPublicWebDetailsByRecordId((previous) => {
      const activeRecordIds = new Set(records.map((record) => record.id));
      let changed = false;
      const next: Record<string, PublicWebDetailCacheEntry> = {};
      Object.entries(previous).forEach(([recordId, entry]) => {
        if (!activeRecordIds.has(recordId)) {
          changed = true;
          return;
        }
        next[recordId] = entry;
      });
      return changed ? next : previous;
    });
  }, [records]);

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
  const selectedRecordsWorkspaceId = useMemo(() => uniqueWorkspaceIdForRecords(selectedRecords), [selectedRecords]);

  const exportScopeRecords = useMemo(
    () => (selectedRecords.length ? selectedRecords : visibleRecords),
    [selectedRecords, visibleRecords],
  );
  const exportScopeWorkspaceId = useMemo(() => uniqueWorkspaceIdForRecords(exportScopeRecords), [exportScopeRecords]);

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
  const canonicalExportScope = useMemo(() => {
    const sourceProjectionIds = Array.from(
      new Set(exportScopeRecords.map((record) => record.sourceProjectionId || "").filter(Boolean)),
    );
    const candidateIdentityKeys = Array.from(
      new Set(exportScopeRecords.map((record) => record.candidateIdentityKey || "").filter(Boolean)),
    );
    return {
      projectionId: sourceProjectionIds.length === 1 ? sourceProjectionIds[0] : "",
      candidateIdentityKeys,
      complete:
        exportScopeRecords.length > 0 &&
        sourceProjectionIds.length === 1 &&
        candidateIdentityKeys.length === exportScopeRecords.length,
    };
  }, [exportScopeRecords]);

  const latestPublicWebBatch = useMemo(
    () =>
      [...publicWebState.batches].sort(comparePublicWebBatchesByCurrentOrder)[0],
    [publicWebState.batches],
  );
  const latestPublicWebBatchMetrics = latestPublicWebBatch
    ? publicWebPhaseMetricsFromSummary(latestPublicWebBatch.summary)
    : {};
  const latestPublicWebBatchMetricLine = latestPublicWebBatch
    ? publicWebPhaseMetricLine(latestPublicWebBatchMetrics, latestPublicWebBatch.status)
    : "";
  const latestPublicWebBatchIssueLine = latestPublicWebBatch
    ? publicWebBatchIssueLine(latestPublicWebBatchMetrics)
    : "";

  const publicWebHasRunningWork = useMemo(
    () =>
      publicWebState.runs.some(
        (run) => run.status !== "unknown" && !PUBLIC_WEB_TERMINAL_STATUSES.has(run.status),
      ),
    [publicWebState.runs],
  );

  const activePublicWebDetailRecord = useMemo(
    () => records.find((record) => record.id === activePublicWebDetailRecordId) || null,
    [activePublicWebDetailRecordId, records],
  );

  const activePublicWebRun = activePublicWebDetailRecord
    ? latestPublicWebRunByRecordId.get(activePublicWebDetailRecord.id)
    : undefined;
  const activePublicWebExpectedRunId = activePublicWebRun?.runId || "";
  const activePublicWebDetailEntry = activePublicWebDetailRecord
    ? publicWebDetailsByRecordId[activePublicWebDetailRecord.id]
    : undefined;
  const activePublicWebDetail = activePublicWebDetailEntry?.detail;
  const activePublicWebDetailIsStale =
    Boolean(activePublicWebDetailEntry) &&
    Boolean(activePublicWebExpectedRunId) &&
    activePublicWebDetailEntry?.latestRunId !== activePublicWebExpectedRunId;
  const activePublicWebReviewRunId =
    activePublicWebExpectedRunId && !activePublicWebDetailIsStale ? activePublicWebExpectedRunId : "";
  const activePublicWebDetailLoading = activePublicWebDetailRecord
    ? publicWebDetailLoadingIds.has(activePublicWebDetailRecord.id)
    : false;
  const activePublicWebDetailError = activePublicWebDetailRecord
    ? publicWebDetailErrors[activePublicWebDetailRecord.id] || ""
    : "";
  const activePublicWebEmailSignals = sortedPublicWebSignals(
    publicWebSignalsForRun(activePublicWebDetail?.emailCandidates || [], activePublicWebReviewRunId),
  );
  const activePublicWebProfileSignals = sortedPublicWebSignals(
    publicWebSignalsForRun(activePublicWebDetail?.profileLinks || [], activePublicWebReviewRunId),
  );
  const activePublicWebReviewableEmailSignals = sortedPublicWebSignals(
    publicWebReviewableSignals(activePublicWebEmailSignals),
  );
  const activePublicWebReviewableProfileSignals = sortedPublicWebSignals(
    publicWebReviewableSignals(activePublicWebProfileSignals),
  );
  const activePublicWebReviewSignals = [
    ...activePublicWebReviewableEmailSignals,
    ...activePublicWebReviewableProfileSignals,
  ];
  const activePublicWebPromotions = activePublicWebDetail?.promotions || [];
  const activePublicWebConfirmedPromotions = activePublicWebPromotions.filter(
    (promotion) => promotion.action === "promote" && promotion.promotionStatus === "manually_promoted",
  );
  const activePublicWebRejectedPromotions = activePublicWebPromotions.filter(
    (promotion) => promotion.action === "reject" || promotion.promotionStatus === "manually_rejected",
  );
  const activePublicWebPendingSignalCount = activePublicWebReviewSignals.length;
  const activePublicWebConfirmedAssetCount = activePublicWebConfirmedPromotions.length;
  const activePublicWebExcludedAssetCount = activePublicWebRejectedPromotions.length;

  useEffect(() => {
    if (!publicWebHasRunningWork) {
      return undefined;
    }
    const interval = window.setInterval(() => {
      void refreshPublicWebState();
    }, 5000);
    return () => window.clearInterval(interval);
  }, [publicWebHasRunningWork, refreshPublicWebState]);

  const storePublicWebDetail = useCallback(
    (recordId: string, detail: TargetCandidatePublicWebDetail, expectedRunId: string) => {
      if (detail.status === "not_found") {
        setPublicWebDetailsByRecordId((previous) => {
          const next = { ...previous };
          delete next[recordId];
          return next;
        });
        return;
      }
      const detailRunId = publicWebDetailLatestRunId(detail);
      if (expectedRunId && detailRunId && detailRunId !== expectedRunId) {
        setPublicWebDetailsByRecordId((previous) => ({
          ...previous,
          [recordId]: {
            latestRunId: detailRunId,
            detail,
          },
        }));
        setPublicWebDetailErrors((previous) => ({
          ...previous,
          [recordId]: "公开信息详情正在更新，请稍后刷新。",
        }));
        return;
      }
      setPublicWebDetailsByRecordId((previous) => ({
        ...previous,
        [recordId]: {
          latestRunId: detailRunId,
          detail,
        },
      }));
    },
    [],
  );

  const loadPublicWebDetail = useCallback(
    async (recordId: string) => {
      const expectedRunId = latestPublicWebRunByRecordId.get(recordId)?.runId || "";
      if (
        !recordId ||
        publicWebDetailCacheMatches(publicWebDetailsByRecordId[recordId], expectedRunId) ||
        publicWebDetailLoadingIds.has(recordId)
      ) {
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
        storePublicWebDetail(recordId, detail, expectedRunId);
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
    [latestPublicWebRunByRecordId, publicWebDetailLoadingIds, publicWebDetailsByRecordId, storePublicWebDetail],
  );

  const refreshPublicWebDetail = useCallback(
    async (recordId: string, expectedRunIdOverride?: string) => {
      if (!recordId) {
        return;
      }
      const expectedRunId = expectedRunIdOverride ?? latestPublicWebRunByRecordId.get(recordId)?.runId ?? "";
      setPublicWebDetailLoadingIds((previous) => new Set(previous).add(recordId));
      setPublicWebDetailErrors((previous) => {
        const next = { ...previous };
        delete next[recordId];
        return next;
      });
      try {
        const detail = await getTargetCandidatePublicWebDetail(recordId);
        storePublicWebDetail(recordId, detail, expectedRunId);
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
    [latestPublicWebRunByRecordId, storePublicWebDetail],
  );

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

  const handleTargetCandidateDraftChange = (recordId: string, patch: Partial<TargetCandidateEditDraft>) => {
    if (!recordId) {
      return;
    }
    setTargetCandidateDraftsByRecordId((previous) => {
      const record = records.find((item) => item.id === recordId);
      const currentDraft = previous[recordId] || (record ? targetCandidateDraftFromRecord(record) : undefined);
      if (!currentDraft) {
        return previous;
      }
      return {
        ...previous,
        [recordId]: {
          ...currentDraft,
          ...patch,
        },
      };
    });
    setTargetCandidateEditErrors((previous) => {
      if (!previous[recordId]) {
        return previous;
      }
      const next = { ...previous };
      delete next[recordId];
      return next;
    });
    setTargetCandidateEditSavedAtByRecordId((previous) => {
      if (!previous[recordId]) {
        return previous;
      }
      const next = { ...previous };
      delete next[recordId];
      return next;
    });
  };

  const handleCancelTargetCandidateEdit = (record: TargetCandidateRecord) => {
    setTargetCandidateDraftsByRecordId((previous) => ({
      ...previous,
      [record.id]: targetCandidateDraftFromRecord(record),
    }));
    setTargetCandidateEditErrors((previous) => {
      if (!previous[record.id]) {
        return previous;
      }
      const next = { ...previous };
      delete next[record.id];
      return next;
    });
    setTargetCandidateEditSavedAtByRecordId((previous) => {
      if (!previous[record.id]) {
        return previous;
      }
      const next = { ...previous };
      delete next[record.id];
      return next;
    });
  };

  const handleSaveTargetCandidateEdit = async (record: TargetCandidateRecord) => {
    const draft = targetCandidateDraftsByRecordId[record.id] || targetCandidateDraftFromRecord(record);
    const parsedQualityScore = parseTargetCandidateQualityScore(draft.qualityScoreText);
    if (parsedQualityScore.error) {
      setTargetCandidateEditErrors((previous) => ({ ...previous, [record.id]: parsedQualityScore.error }));
      return;
    }
    if (!targetCandidateDraftIsDirty(record, draft) || targetCandidateEditSavingIds.has(record.id)) {
      return;
    }
    setTargetCandidateEditSavingIds((previous) => new Set(previous).add(record.id));
    setTargetCandidateEditErrors((previous) => {
      const next = { ...previous };
      delete next[record.id];
      return next;
    });
    setTargetCandidateEditSavedAtByRecordId((previous) => {
      const next = { ...previous };
      delete next[record.id];
      return next;
    });
    try {
      const updated = await updateTargetCandidate(
        record.id,
        {
          followUpStatus: draft.followUpStatus,
          qualityScore: parsedQualityScore.qualityScore,
          comment: draft.comment,
        },
        targetCandidateScope,
      );
      if (!updated) {
        setTargetCandidateEditErrors((previous) => ({
          ...previous,
          [record.id]: "目标候选人不存在，已停止保存。",
        }));
        return;
      }
      setRecords((previous) => previous.map((item) => (item.id === updated.id ? updated : item)));
      setTargetCandidateDraftsByRecordId((previous) => ({
        ...previous,
        [updated.id]: targetCandidateDraftFromRecord(updated),
      }));
      setTargetCandidateEditSavedAtByRecordId((previous) => ({
        ...previous,
        [updated.id]: Date.now(),
      }));
    } catch (error) {
      setTargetCandidateEditErrors((previous) => ({
        ...previous,
        [record.id]: formatTargetCandidateActionError(error, "目标候选人跟进信息保存失败。"),
      }));
    } finally {
      setTargetCandidateEditSavingIds((previous) => {
        const next = new Set(previous);
        next.delete(record.id);
        return next;
      });
    }
  };

  const handleStartPublicWebSearch = async () => {
    const recordIds = selectedRecords.map((record) => record.id).filter(Boolean);
    if (recordIds.length === 0 || publicWebActionPending) {
      return;
    }
    if (!selectedRecordsWorkspaceId) {
      setPublicWebActionError("Public Web Search 缺少 workspace 或暂不支持跨 workspace 批量提交。");
      return;
    }
    const confirmed = window.confirm(
      `确认对 ${recordIds.length} 位候选人搜索公开信息？这会排队候选人级公开信息搜索任务，可能触发真实搜索和模型判断；旧结果会保留为审计记录。`,
    );
    if (!confirmed) {
      return;
    }
    setPublicWebActionPending(true);
    setPublicWebActionMessage("");
    setPublicWebActionError("");
    try {
      const result = await startTargetCandidatePublicWebSearch({ recordIds, workspaceId: selectedRecordsWorkspaceId });
      setPublicWebState((previous) =>
        mergePublicWebState(previous, {
          status: result.status,
          batches: result.batch ? [result.batch] : result.batches,
          runs: result.runs,
          phaseCommandsByRunId: result.phaseCommandsByRunId,
        }),
      );
      const queuedCount = Number(result.workerSummary.queued_worker_count || 0) || 0;
      const reusedCount = Number(result.workerSummary.reused_terminal_run_count || 0) || 0;
      setPublicWebActionMessage(
        result.status === "joined"
          ? `已加入现有公开信息搜索，${result.runs.length} 位候选人正在复用同一批任务。`
          : `已排队 ${queuedCount} 位候选人的公开信息搜索${reusedCount ? `，复用 ${reusedCount} 个已完成结果` : ""}。`,
      );
      if (activePublicWebDetailRecordId && recordIds.includes(activePublicWebDetailRecordId)) {
        const expectedRunId = latestPublicWebRunIdForRecord(result.runs, activePublicWebDetailRecordId);
        void refreshPublicWebDetail(activePublicWebDetailRecordId, expectedRunId);
      }
      void refreshPublicWebState();
    } catch (error) {
      setPublicWebActionError(formatTargetCandidateActionError(error, "公开信息搜索提交失败。"));
    } finally {
      setPublicWebActionPending(false);
    }
  };

  const handleOpenPublicWebDetail = (recordId: string) => {
    if (!recordId) {
      return;
    }
    setActivePublicWebDetailRecordId(recordId);
    setPublicWebEvidenceExpanded(false);
    setExpandedPublicWebSignalGroups(new Set());
    void loadPublicWebDetail(recordId);
  };

  const handleClosePublicWebDetail = () => {
    setActivePublicWebDetailRecordId("");
  };

  const handleCancelPublicWebRun = async (run: TargetCandidatePublicWebRun) => {
    if (!run.runId || publicWebRunActionPendingIds.has(run.runId)) {
      return;
    }
    const workspaceId = (run.workspaceId || "").trim();
    if (!workspaceId) {
      setPublicWebActionError("Public Web Search run 缺少 workspace，已阻止取消请求。");
      return;
    }
    setPublicWebRunActionPendingIds((previous) => new Set(previous).add(run.runId));
    setPublicWebActionMessage("");
    setPublicWebActionError("");
    try {
      const result = await cancelTargetCandidatePublicWebSearch({
        runIds: [run.runId],
        workspaceId,
        reason: "cancelled_from_target_candidates_panel",
        operator: "frontend",
      });
      setPublicWebState((previous) => mergePublicWebState(previous, result));
      setPublicWebActionMessage("已取消该候选人的公开信息搜索。");
      if (run.recordId) {
        await refreshPublicWebDetail(run.recordId);
      }
      void refreshPublicWebState();
    } catch (error) {
      setPublicWebActionError(formatTargetCandidateActionError(error, "公开信息搜索取消失败。"));
    } finally {
      setPublicWebRunActionPendingIds((previous) => {
        const next = new Set(previous);
        next.delete(run.runId);
        return next;
      });
    }
  };

  const handleRetryPublicWebRun = async (run: TargetCandidatePublicWebRun) => {
    if (!run.runId || publicWebRunActionPendingIds.has(run.runId)) {
      return;
    }
    const workspaceId = (run.workspaceId || "").trim();
    if (!workspaceId) {
      setPublicWebActionError("Public Web Search run 缺少 workspace，已阻止重试请求。");
      return;
    }
    const confirmed = window.confirm(
      "确认重新搜索公开信息？这可能触发真实搜索和模型判断。已确认/已排除的公开信息会保留；本次搜索只刷新待审核候选。",
    );
    if (!confirmed) {
      return;
    }
    setPublicWebRunActionPendingIds((previous) => new Set(previous).add(run.runId));
    setPublicWebActionMessage("");
    setPublicWebActionError("");
    try {
      const result = await retryTargetCandidatePublicWebSearch({
        runIds: [run.runId],
        workspaceId,
        reason: "retry_requested_from_target_candidates_panel",
        operator: "frontend",
      });
      setPublicWebState((previous) => mergePublicWebState(previous, result));
      setPublicWebActionMessage("已重新排队该候选人的公开信息搜索。");
      if (run.recordId) {
        const expectedRunId = latestPublicWebRunIdForRecord(result.runs, run.recordId);
        await refreshPublicWebDetail(run.recordId, expectedRunId);
      }
      void refreshPublicWebState();
    } catch (error) {
      setPublicWebActionError(formatTargetCandidateActionError(error, "公开信息搜索重试失败。"));
    } finally {
      setPublicWebRunActionPendingIds((previous) => {
        const next = new Set(previous);
        next.delete(run.runId);
        return next;
      });
    }
  };

  const handleExportArchive = async () => {
    if (exportRecordIds.length === 0 || exportingArchive) {
      return;
    }
    setExportingArchive(true);
    setExportError("");
    setExportNotice("");
    try {
      if (!canonicalExportScope.complete) {
        throw new Error("当前选择中有候选人缺少可打包的人选资料，请改选候选人或先使用 Web Search 导出。");
      }
      const download = await exportProjectionCandidatesArchive({
        projectionId: canonicalExportScope.projectionId,
        candidateIdentityKeys: canonicalExportScope.candidateIdentityKeys,
      });
      downloadBlobFile(download.filename || "target-candidates.zip", download.blob);
      setExportNotice("人选信息导出完成。");
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
    if (!exportScopeWorkspaceId) {
      setExportError("Web Search 导出缺少 workspace 或暂不支持跨 workspace 批量导出。");
      return;
    }
    if (
      publicWebExportMode === "promoted_and_publishable" &&
      !window.confirm(
        `确认导出 ${publicWebExportRecordIds.length} 位候选人的 Web Search 信息，并包含 AI 判定可发布但未人工确认的公开信息候选？`,
      )
    ) {
      return;
    }
    setExportingPublicWebArchive(true);
    setExportError("");
    try {
      const download = await exportTargetCandidatePublicWebArchive({
        recordIds: publicWebExportRecordIds,
        workspaceId: exportScopeWorkspaceId,
        mode: publicWebExportMode,
      });
      downloadBlobFile(download.filename || "target-candidates-public-web.zip", download.blob);
      const stats = download.exportStats;
      setPublicWebPromotionMessage(
        `Web Search 导出完成：${stats.exportedRecordCount}/${stats.recordCount} 位候选人有可导出公开信息，` +
          `${stats.exportedSignalCount} 条公开信息。无结果 ${stats.noPublicWebResultCount}，无可导出公开信息 ${stats.noExportableSignalCount}` +
          (stats.nonTerminalRunCount ? `，未完成 ${stats.nonTerminalRunCount}` : "") +
          "。详情见压缩包 public_web_manifest.json。",
      );
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
    const overrideValidationReason = action === "promote" ? publicWebSignalOverrideReason(signal) : "";
    let overrideReason = "";
    if (overrideValidationReason) {
      const enteredReason = window.prompt("请输入人工覆盖确认理由", signal.suppressionReason || publicWebWarningLabel(overrideValidationReason));
      overrideReason = (enteredReason || "").trim();
      if (!overrideReason) {
        setPublicWebActionError("人工覆盖确认需要填写理由。");
        return;
      }
    }
    setPublicWebPromotionPendingIds((previous) => new Set(previous).add(signal.signalId));
    setPublicWebPromotionMessage("");
    setPublicWebActionError("");
    try {
      await promoteTargetCandidatePublicWebSignal({
        recordId,
        signalId: signal.signalId,
        action,
        operator: "frontend",
        allowUnpublishable: Boolean(overrideValidationReason),
        overrideReason,
      });
      await refreshPublicWebDetail(recordId);
      const refreshedRecords = await readTargetCandidates(targetCandidateScope);
      setRecords(refreshedRecords);
      setPublicWebPromotionMessage(
        action === "promote"
          ? "公开信息候选已人工确认，导出包会包含该结果。"
          : "公开信息候选已标记为不导出。",
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

  const handlePublicWebSignalReviewStatusChange = async (
    recordId: string,
    signal: TargetCandidatePublicWebSignal,
    nextStatus: PublicWebSignalReviewStatus,
  ) => {
    const currentStatus = publicWebSignalReviewStatus(signal);
    if (nextStatus === currentStatus) {
      return;
    }
    if (nextStatus === "review") {
      setPublicWebActionError("当前版本不支持把已人工判断的公开信息候选回退到待复核；请改选“已确认并导出”或“已排除”。");
      return;
    }
    if (!activePublicWebReviewRunId) {
      setPublicWebActionError("本次公开信息搜索状态尚未确认，暂不能审核这些候选。请刷新状态后重试。");
      await refreshPublicWebDetail(recordId, activePublicWebExpectedRunId);
      return;
    }
    if (signal.runId !== activePublicWebReviewRunId) {
      setPublicWebActionError("该公开信息候选不属于本次搜索，已禁止确认/导出。请刷新详情后重试。");
      await refreshPublicWebDetail(recordId, activePublicWebReviewRunId);
      return;
    }
    await handlePromotePublicWebSignal(recordId, signal, nextStatus === "confirmed" ? "promote" : "reject");
  };

  const togglePublicWebSignalGroup = (groupKey: string) => {
    setExpandedPublicWebSignalGroups((previous) => {
      const next = new Set(previous);
      if (next.has(groupKey)) {
        next.delete(groupKey);
      } else {
        next.add(groupKey);
      }
      return next;
    });
  };

  const renderPublicWebSignalReviewSection = ({
    groupKey,
    title,
    description,
    signals,
    emptyLabel,
    testId,
  }: {
    groupKey: string;
    title: string;
    description: string;
    signals: TargetCandidatePublicWebSignal[];
    emptyLabel: string;
    testId: string;
  }) => {
    const expanded = expandedPublicWebSignalGroups.has(groupKey);
    const visibleSignals = expanded ? signals : signals.slice(0, 1);
    const hiddenCount = Math.max(0, signals.length - visibleSignals.length);
    return (
      <section className="drawer-card public-web-review-section" data-testid={testId}>
        <div className="public-web-review-section__header">
          <div className="public-web-review-section__title-row">
            <h4>{title}</h4>
            <HelpBadge className="public-web-section-help" label={`${title}说明`}>
              {description}
            </HelpBadge>
          </div>
          {signals.length ? (
            <span className="public-web-review-count">
              {signals.length} 个候选
            </span>
          ) : null}
        </div>
        {visibleSignals.length ? (
          <div className="target-candidate-public-web-signal-list">
            {visibleSignals.map((signal, index) => {
              const url = publicWebSignalUrl(signal);
              const reviewStatus = publicWebSignalReviewStatus(signal);
              const pending = publicWebPromotionPendingIds.has(signal.signalId);
              const secondaryCandidate = index > 0 || publicWebSignalReviewRank(signal) >= 3;
              const signalTitle =
                signal.signalKind === "email_candidate"
                  ? signal.normalizedValue || signal.value || "Email candidate"
                  : publicWebLinkTypeLabel(signal.signalType);
              const statusLabel = pending ? "状态更新中..." : publicWebSignalReviewStatusLabel(reviewStatus);
              return (
                <div
                  key={signal.signalId || `${activePublicWebDetailRecord?.id || "record"}-${url || signal.normalizedValue}`}
                  className={`target-candidate-public-web-signal-row public-web-review-signal${
                    secondaryCandidate ? " is-secondary" : ""
                  }`}
                >
                  <div className="public-web-review-signal__topline">
                    <div>
                      <strong>{signalTitle}</strong>
                      <span>
                        {signal.signalKind === "email_candidate"
                          ? `${signal.emailType || "unknown email"} · ${publicWebIdentityLabel(signal.identityMatchLabel)}`
                          : `${publicWebIdentityLabel(signal.identityMatchLabel)} · ${signal.sourceDomain || "unknown source"}`}
                      </span>
                    </div>
                    <div className="public-web-review-status-control">
                      <span className={`public-web-status-chip ${publicWebSignalReviewTone(reviewStatus)}`}>
                        {statusLabel}
                      </span>
                      <select
                        className="public-web-review-select"
                        data-testid="public-web-signal-review-select"
                        aria-label={`${signalTitle} 审核状态`}
                        value={reviewStatus}
                        disabled={pending}
                        onChange={(event) =>
                          void handlePublicWebSignalReviewStatusChange(
                            activePublicWebDetailRecord?.id || "",
                            signal,
                            event.target.value as PublicWebSignalReviewStatus,
                          )
                        }
                      >
                        <option value="review" disabled={reviewStatus !== "review"}>
                          待复核
                        </option>
                        <option value="confirmed">已确认并导出</option>
                        <option value="excluded">已排除</option>
                      </select>
                    </div>
                  </div>
                  <div className="target-candidate-public-web-signal-chips">
                    {signal.publishable ? (
                      <span className="keyword-chip public-web-status-chip is-success">AI 可发布</span>
                    ) : (
                      <span className="keyword-chip public-web-status-chip is-warning">AI 不建议直接导出</span>
                    )}
                    {signal.signalKind === "profile_link" ? (
                      signal.cleanProfileLink ? (
                        <span className="keyword-chip public-web-status-chip is-success">主页形态正常</span>
                      ) : (
                        <span className="keyword-chip public-web-status-chip is-warning">URL 形态需复核</span>
                      )
                    ) : null}
                    {secondaryCandidate ? (
                      <span className="keyword-chip public-web-status-chip is-muted">低置信候选</span>
                    ) : null}
                    {signal.suppressionReason ? (
                      <span className="keyword-chip public-web-status-chip is-warning">
                        {signal.suppressionReason}
                      </span>
                    ) : null}
                    {signal.linkShapeWarnings.map((warning) => (
                      <span key={`${signal.signalId}-${warning}`} className="keyword-chip public-web-status-chip is-warning">
                        {publicWebWarningLabel(warning)}
                      </span>
                    ))}
                  </div>
                  {signal.signalKind === "email_candidate" ? (
                    <p className="public-web-review-value">{signal.normalizedValue || signal.value}</p>
                  ) : url ? (
                    <a href={url} target="_blank" rel="noreferrer">
                      {publicWebSignalLabel(signal)}
                    </a>
                  ) : null}
                  {signal.evidenceExcerpt ? <p>{signal.evidenceExcerpt}</p> : null}
                  {signal.sourceUrl && signal.signalKind === "email_candidate" ? (
                    <a href={signal.sourceUrl} target="_blank" rel="noreferrer">
                      证据页：{signal.sourceDomain || signal.sourceUrl}
                    </a>
                  ) : null}
                  <p className="muted public-web-review-export-hint">
                    {reviewStatus === "confirmed"
                      ? "默认导出会包含该公开信息。"
                      : reviewStatus === "excluded"
                        ? "默认导出会排除该公开信息。"
                        : "默认导出不会包含待复核公开信息；请选择状态后再导出。"}
                  </p>
                </div>
              );
            })}
          </div>
        ) : (
          <span className="target-candidates-export-note">{emptyLabel}</span>
        )}
        {signals.length > 1 ? (
          <button
            type="button"
            className="ghost-button small-button public-web-review-more-button"
            onClick={() => togglePublicWebSignalGroup(groupKey)}
          >
            {expanded ? "收起低置信候选" : `查看更多信息候选（${hiddenCount}）`}
          </button>
        ) : null}
      </section>
    );
  };

  const renderPublicWebConfirmedAssetsSection = (
    promotions: TargetCandidatePublicWebPromotion[],
    rejectedPromotions: TargetCandidatePublicWebPromotion[],
  ) => (
    <section className="drawer-card public-web-review-section" data-testid="public-web-confirmed-assets-section">
      <div className="public-web-review-section__header">
        <div className="public-web-review-section__title-row">
          <h4>已确认公开信息</h4>
          <HelpBadge className="public-web-section-help" label="已确认公开信息说明">
            人工确认的信息会保留在这里；重新搜索只刷新下方本次待审核候选。
          </HelpBadge>
        </div>
        {promotions.length ? <span className="public-web-review-count">{promotions.length} 条已确认</span> : null}
      </div>
      {promotions.length ? (
        <div className="target-candidate-public-web-signal-list">
          {promotions.map((promotion) => {
            const value = publicWebPromotionValue(promotion);
            const href = promotion.url || promotion.sourceUrl || value;
            return (
              <div
                key={promotion.promotionId || `${promotion.signalId}-${value}`}
                className="target-candidate-public-web-signal-row public-web-review-signal"
              >
                <div className="public-web-review-signal__topline">
                  <div>
                    <strong>{publicWebPromotionLabel(promotion)}</strong>
                    <span>
                      {publicWebIdentityLabel(promotion.identityMatchLabel)} ·{" "}
                      {promotion.sourceDomain || promotion.sourceFamily || "confirmed source"}
                    </span>
                  </div>
                  <span className="public-web-status-chip is-success">已确认并导出</span>
                </div>
                <div className="target-candidate-public-web-signal-chips">
                  {promotion.publishable ? (
                    <span className="keyword-chip public-web-status-chip is-success">AI 可发布</span>
                  ) : (
                    <span className="keyword-chip public-web-status-chip is-warning">人工确认资产</span>
                  )}
                  {promotion.requiresManualOverride ? (
                    <span className="keyword-chip public-web-status-chip is-warning">人工覆盖</span>
                  ) : null}
                </div>
                {href && /^https?:\/\//i.test(href) ? (
                  <a href={href} target="_blank" rel="noreferrer">
                    {promotion.sourceTitle || value || href}
                  </a>
                ) : (
                  <p className="public-web-review-value">{value}</p>
                )}
                {promotion.evidenceExcerpt ? <p>{promotion.evidenceExcerpt}</p> : null}
                <p className="muted public-web-review-export-hint">
                  默认 Web Search 导出会包含该人工确认资产。
                </p>
              </div>
            );
          })}
        </div>
      ) : (
        <span className="target-candidates-export-note">暂无已确认公开信息。</span>
      )}
      {rejectedPromotions.length ? (
        <p className="muted public-web-review-export-hint">
          已排除 {rejectedPromotions.length} 条历史公开信息，不会进入默认导出。
        </p>
      ) : null}
    </section>
  );

  return (
    <div className="target-candidates-panel">
      <section className="panel target-candidates-toolbar">
        <div className="target-candidates-toolbar-main">
          <div>
            <h3>跟进工作台</h3>
            <p className="muted">管理候选人状态、备注、公开信息审核和导出。</p>
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
              <span>公开信息搜索</span>
              <HelpBadge label="公开信息搜索说明">
                按候选人逐一排队，后台继续搜索、抓取和分析；默认导出不包含 raw HTML/PDF。
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
            {latestPublicWebBatchMetricLine ? (
              <span className="target-candidates-inline-message" data-testid="target-candidates-public-web-phase-metrics">
                {latestPublicWebBatchMetricLine}
              </span>
            ) : null}
            {latestPublicWebBatchIssueLine ? (
              <span
                className="target-candidates-inline-message"
                data-testid="target-candidates-public-web-issues"
              >
                {latestPublicWebBatchIssueLine}
              </span>
            ) : null}
          </div>

          <div className="target-candidates-action-group">
              <div className="target-candidates-action-title">
                <span>批量导出</span>
                <HelpBadge label="批量导出说明">
                Web Search 默认只导出已人工确认的公开信息；包含 AI 可发布未确认公开信息候选时必须显式选择扩展范围，不包含 raw HTML/PDF/search payload。
                </HelpBadge>
              </div>
            <div className="target-candidates-export-controls">
              <label className="target-candidates-export-mode">
                <span>Web Search 导出范围</span>
                <select
                  value={publicWebExportMode}
                  data-testid="target-candidates-public-web-export-mode"
                  onChange={(event) => setPublicWebExportMode(event.currentTarget.value as PublicWebExportMode)}
                >
                  <option value="promoted_only">仅已确认</option>
                  <option value="promoted_and_publishable">已确认 + AI 可发布未确认</option>
                </select>
              </label>
            </div>
            <div className="target-candidates-button-row">
              <span className="target-candidates-button-with-help">
                <button
                  type="button"
                  className="ghost-button"
                  onClick={() => void handleExportArchive()}
                  disabled={exportRecordIds.length === 0 || exportingArchive || !canonicalExportScope.complete}
                >
                  {exportingArchive ? "正在打包人选资料..." : "批量导出人选资料"}
                </button>
                <HelpBadge label="人选资料导出说明">
                  当前选择里有候选人缺少可打包的人选资料来源时，该按钮会禁用；可以改选候选人，或先导出已确认的 Web Search 信息。
                </HelpBadge>
              </span>
              <button
                type="button"
                className="ghost-button"
                data-testid="target-candidates-public-web-export"
                onClick={() => void handleExportPublicWebArchive()}
                disabled={publicWebExportRecordIds.length === 0 || exportingPublicWebArchive}
              >
                {exportingPublicWebArchive ? "正在打包 Web Search..." : "批量导出 Web Search 信息"}
              </button>
            </div>
            {exportNotice ? <span className="target-candidates-inline-message">{exportNotice}</span> : null}
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
            const publicWebPhaseMetrics = publicWebPhaseMetricsFromSummary(publicWebSummary);
            const primaryLinks = primaryLinksFromSummary(publicWebSummary);
            const publicWebPhaseLine = publicWebPhaseMetricLine(publicWebPhaseMetrics, publicWebStatus);
            const publicWebPhaseCommandStatusLine = publicWebPhaseCommandLine(publicWebRun);
            const publicWebCanCancel = publicWebRunAllows(publicWebRun, "cancel");
            const publicWebCanRetry = publicWebRunAllows(publicWebRun, "retry");
            const publicWebReviewLine = publicWebRunReviewLine(
              publicWebPhaseMetrics,
              publicWebStatus,
              primaryLinks.length,
              publicWebRun?.lastError || "",
            );
            const entryLinkCount = numberFromSummary(publicWebSummary, "entry_link_count");
            const fetchedDocumentCount = numberFromSummary(publicWebSummary, "fetched_document_count");
            const reviewableEmailSignalCount = numberFromMetrics(
              publicWebPhaseMetrics,
              "email_signal_materialized_count",
            );
            const editDraft = targetCandidateDraftsByRecordId[record.id] || targetCandidateDraftFromRecord(record);
            const editSaving = targetCandidateEditSavingIds.has(record.id);
            const editDirty = targetCandidateDraftIsDirty(record, editDraft);
            const editError = targetCandidateEditErrors[record.id] || "";
            const editSavedAt = targetCandidateEditSavedAtByRecordId[record.id] || 0;
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
                      <p className="candidate-meta-line candidate-headline-scroll target-candidate-headline-scroll">
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
                    value={editDraft.followUpStatus}
                    data-testid="target-candidate-follow-up-status"
                    disabled={editSaving}
                    onChange={(event) =>
                      handleTargetCandidateDraftChange(record.id, {
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
                    min={0}
                    max={100}
                    value={editDraft.qualityScoreText}
                    data-testid="target-candidate-quality-score"
                    placeholder="0-100"
                    disabled={editSaving}
                    onChange={(event) =>
                      handleTargetCandidateDraftChange(record.id, {
                        qualityScoreText: event.target.value,
                      })
                    }
                  />
                </label>

                <label className="review-control-block target-candidate-comment">
                  <span className="field-label">备注 Comment</span>
                  <textarea
                    className="message-editor"
                    rows={4}
                    value={editDraft.comment}
                    data-testid="target-candidate-comment"
                    placeholder="记录跟进信息、沟通反馈或后续判断。"
                    disabled={editSaving}
                    onChange={(event) =>
                      handleTargetCandidateDraftChange(record.id, {
                        comment: event.target.value,
                      })
                    }
                  />
                </label>
                <div className="target-candidate-edit-actions">
                  <button
                    type="button"
                    className="primary-button candidate-action-button"
                    data-testid="target-candidate-edit-save"
                    onClick={() => void handleSaveTargetCandidateEdit(record)}
                    disabled={!editDirty || editSaving}
                  >
                    {editSaving ? "保存中..." : "保存跟进信息"}
                  </button>
                  <button
                    type="button"
                    className="ghost-button candidate-action-button"
                    data-testid="target-candidate-edit-cancel"
                    onClick={() => handleCancelTargetCandidateEdit(record)}
                    disabled={!editDirty || editSaving}
                  >
                    取消修改
                  </button>
                  {editError ? (
                    <span className="target-candidates-inline-message is-error">{editError}</span>
                  ) : null}
                  {!editError && editDirty ? (
                    <span
                      className="target-candidates-inline-message is-warning"
                      data-testid="target-candidate-edit-dirty"
                    >
                      有未保存修改
                    </span>
                  ) : null}
                  {!editError && !editDirty && editSavedAt ? (
                    <span className="target-candidates-inline-message" data-testid="target-candidate-edit-saved">
                      已保存
                    </span>
                  ) : null}
                </div>
              </div>

              <div className="target-candidate-public-web-card">
                <div className="target-candidate-public-web-card__header">
                  <span className={`public-web-status-chip ${publicWebStatusTone(publicWebStatus)}`}>
                    <span data-testid="target-candidate-public-web-status">
                    {publicWebStatusLabel(publicWebStatus)}
                    </span>
                  </span>
                  {publicWebRun?.updatedAt ? (
                    <span className="target-candidates-export-note">
                      更新 {formatWorkflowTimestamp(publicWebRun.updatedAt)}
                    </span>
                  ) : null}
                </div>
                {publicWebRun ? (
                  <>
                    <div className="target-candidate-public-web-metrics">
                      <span>链接 {entryLinkCount}</span>
                      <span>文档 {fetchedDocumentCount}</span>
                      <span>可审核邮箱 {reviewableEmailSignalCount}</span>
                    </div>
                    <div className="target-candidate-public-web-progress">
                      <div className="target-candidate-public-web-progress-copy">
                        {publicWebPhaseLine ? (
                          <span
                            className="target-candidates-export-note"
                            data-testid="target-candidate-public-web-phase-line"
                          >
                            {publicWebPhaseLine}
                          </span>
                        ) : null}
                        {publicWebPhaseCommandStatusLine ? (
                          <span
                            className="target-candidates-export-note"
                            data-testid="target-candidate-public-web-command-line"
                          >
                            {publicWebPhaseCommandStatusLine}
                          </span>
                        ) : null}
                        <span className="target-candidates-export-note">
                          {primaryLinks.length ? `已确认公开主页 ${primaryLinks.length} 个。` : "暂无自动确认主页。"}
                        </span>
                      </div>
                      {publicWebReviewLine ? (
                        <HelpBadge
                          className="public-web-section-help target-candidate-public-web-review-help"
                          label="公开信息复核说明"
                        >
                          {publicWebReviewLine}
                        </HelpBadge>
                      ) : null}
                    </div>
                    <div className="target-candidate-public-web-links" aria-label="已确认公开主页">
                      {primaryLinks.map((link) => (
                        <a key={`${record.id}-${link.label}-${link.url}`} href={link.url} target="_blank" rel="noreferrer">
                          {link.label}
                        </a>
                      ))}
                    </div>
                    <div className="target-candidate-public-web-detail-actions">
                      {record.linkedinUrl ? (
                        <a className="ghost-button candidate-action-button" href={record.linkedinUrl} target="_blank" rel="noreferrer">
                          打开 LinkedIn
                        </a>
                      ) : null}
                      {publicWebCanCancel ? (
                        <button
                          type="button"
                          className="ghost-button candidate-action-button"
                          onClick={() => void handleCancelPublicWebRun(publicWebRun)}
                          disabled={publicWebRunActionPendingIds.has(publicWebRun.runId)}
                        >
                          {publicWebRunActionPendingIds.has(publicWebRun.runId) ? "正在取消..." : "取消本次搜索"}
                        </button>
                      ) : null}
                      {publicWebCanRetry ? (
                        <button
                          type="button"
                          className="ghost-button candidate-action-button"
                          data-testid="target-candidate-public-web-retry"
                          onClick={() => void handleRetryPublicWebRun(publicWebRun)}
                          disabled={publicWebRunActionPendingIds.has(publicWebRun.runId)}
                        >
                          {publicWebRunActionPendingIds.has(publicWebRun.runId) ? "正在重试..." : "重试公开搜索"}
                        </button>
                      ) : null}
                      <button
                        type="button"
                        className="ghost-button candidate-action-button"
                        data-testid="target-candidate-public-web-detail-open"
                        onClick={() => handleOpenPublicWebDetail(record.id)}
                      >
                        查看公开信息详情
                      </button>
                    </div>
                  </>
                ) : (
                  <>
                    <span className="target-candidates-export-note">
                      尚未触发候选人级 Public Web Search。
                    </span>
                    {record.linkedinUrl ? (
                      <div className="target-candidate-public-web-detail-actions">
                        <a className="ghost-button candidate-action-button" href={record.linkedinUrl} target="_blank" rel="noreferrer">
                          打开 LinkedIn
                        </a>
                      </div>
                    ) : null}
                  </>
                )}
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
      {activePublicWebDetailRecord ? (
        <div className="drawer-backdrop" onClick={handleClosePublicWebDetail} role="presentation">
          <aside
            className="one-page-drawer target-candidate-detail-drawer"
            role="dialog"
            aria-modal="true"
            aria-labelledby="target-candidate-detail-drawer-title"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="drawer-head">
              <div>
                <span className="section-step">公开信息详情</span>
                <h3 id="target-candidate-detail-drawer-title">{activePublicWebDetailRecord.candidateName}</h3>
                <p className="muted">
                  {activePublicWebDetailRecord.currentCompany || "Unknown company"}
                  {activePublicWebRun ? ` · ${publicWebStatusLabel(activePublicWebRun.status)}` : " · 未开始"}
                </p>
              </div>
              <button type="button" className="ghost-button small-button" onClick={handleClosePublicWebDetail}>
                关闭
              </button>
            </div>

            <div className="drawer-body">
              <section className="drawer-card">
                <div className="drawer-profile public-web-review-profile">
                  <Avatar
                    name={activePublicWebDetailRecord.candidateName}
                    src={activePublicWebDetailRecord.avatarUrl}
                    size="large"
                  />
                  <div>
                    <h4>{activePublicWebDetailRecord.candidateName}</h4>
                    <p>{activePublicWebDetailRecord.headline || "暂无 headline"}</p>
                    <span>
                      {activePublicWebDetailRecord.primaryEmail
                        ? `CRM primary email: ${activePublicWebDetailRecord.primaryEmail}`
                        : "CRM 尚未确认 primary email"}
                    </span>
                  </div>
                </div>
                <div className="public-web-review-summary">
                  <span className={`public-web-status-chip ${publicWebStatusTone(activePublicWebRun?.status || "unknown")}`}>
                    {activePublicWebRun ? publicWebStatusLabel(activePublicWebRun.status) : "未开始"}
                  </span>
                  <span>本次待审核 {activePublicWebReviewSignals.length}</span>
                  <span>已确认资产 {activePublicWebConfirmedAssetCount}</span>
                  <span>待复核 {activePublicWebPendingSignalCount}</span>
                  <span>已排除 {activePublicWebExcludedAssetCount}</span>
                  <HelpBadge className="public-web-section-help" label="公开信息详情说明">
                    本页区分已确认公开信息和本次待审核候选；重新搜索不会删除已确认信息。
                  </HelpBadge>
                </div>
              </section>

              {activePublicWebDetailLoading ? (
                <div className="drawer-loading">
                  <p>正在加载 Public Web detail...</p>
                </div>
              ) : null}
              {activePublicWebDetailError ? (
                <span className="target-candidates-inline-message is-error">{activePublicWebDetailError}</span>
              ) : null}
              {activePublicWebDetailIsStale && !activePublicWebDetailLoading ? (
                <span className="target-candidates-inline-message">
                  公开信息详情正在更新；已确认公开信息仍会保留。
                </span>
              ) : null}
              {!activePublicWebDetailLoading &&
              !activePublicWebDetail &&
              !activePublicWebDetailError &&
              !activePublicWebDetailIsStale ? (
                <section className="drawer-card">
                  <p className="muted">暂无 Public Web detail。请先对该候选人触发 Public Web Search。</p>
                </section>
              ) : null}

              {activePublicWebDetail ? (
                <>
                  {renderPublicWebConfirmedAssetsSection(
                    activePublicWebConfirmedPromotions,
                    activePublicWebRejectedPromotions,
                  )}

                  {renderPublicWebSignalReviewSection({
                    groupKey: `${activePublicWebDetailRecord.id}:email`,
                    title: "联系邮箱候选",
                    description: "可写入 CRM primary email 或导出的邮箱候选。未确认前不会进入默认导出。",
                    signals: activePublicWebReviewableEmailSignals,
                    emptyLabel: "当前没有可审核邮箱候选。",
                    testId: "public-web-email-review-section",
                  })}

                  {renderPublicWebSignalReviewSection({
                    groupKey: `${activePublicWebDetailRecord.id}:profile`,
                    title: "公开主页候选",
                    description: "个人主页、GitHub、X、Scholar、Substack 等可导出链接。证据来源本身不在这里重复展示。",
                    signals: activePublicWebReviewableProfileSignals,
                    emptyLabel: "当前没有可审核公开主页候选。",
                    testId: "public-web-profile-review-section",
                  })}

                  {activePublicWebDetail.evidenceLinks.length ? (
                    <section className="drawer-card public-web-evidence-audit-card" data-testid="public-web-evidence-audit-section">
                      <div className="public-web-review-section__header">
                        <div className="public-web-review-section__title-row">
                          <h4>只读证据来源</h4>
                          <HelpBadge className="public-web-section-help" label="只读证据来源说明">
                            这些页面用于解释模型判断和审计，不是可直接导出的候选项；请在上方公开信息候选中选择状态。
                          </HelpBadge>
                        </div>
                        <button
                          type="button"
                          className="ghost-button small-button"
                          onClick={() => setPublicWebEvidenceExpanded((current) => !current)}
                        >
                          {publicWebEvidenceExpanded
                            ? "收起证据来源"
                            : `查看证据来源（${activePublicWebDetail.evidenceLinks.length}）`}
                        </button>
                      </div>
                      {publicWebEvidenceExpanded ? (
                        <div className="target-candidate-public-web-evidence-links">
                          {activePublicWebDetail.evidenceLinks
                            .filter((evidence) => evidence.sourceUrl)
                            .slice(0, 20)
                            .map((evidence) => (
                              <a
                                key={`${activePublicWebDetailRecord.id}-${evidence.sourceUrl}`}
                                href={evidence.sourceUrl}
                                target="_blank"
                                rel="noreferrer"
                              >
                                {evidence.sourceTitle || evidence.sourceDomain || evidence.sourceUrl}
                              </a>
                            ))}
                        </div>
                      ) : null}
                    </section>
                  ) : null}
                </>
              ) : null}
            </div>
          </aside>
        </div>
      ) : null}
    </div>
  );
}
