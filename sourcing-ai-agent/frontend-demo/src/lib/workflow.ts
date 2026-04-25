import type {
  Candidate,
  DashboardData,
  DemoPlan,
  RunStatusData,
  SearchHistoryItem,
  SearchTimelineStep,
  TimelineSourceTag,
} from "../types";
import { summarizeSearchQuery } from "./historySummary";
import { formatWorkflowTimestamp, parseWorkflowTimestamp } from "./time";

function detectCompany(queryText: string): string {
  const normalized = queryText.trim();
  if (!normalized) {
    return "目标公司待确认";
  }
  const mappings = [
    { pattern: /google\s*deepmind|deepmind/i, label: "Google DeepMind" },
    { pattern: /openai/i, label: "OpenAI" },
    { pattern: /thinking machines/i, label: "Thinking Machines Lab" },
    { pattern: /anthropic/i, label: "Anthropic" },
    { pattern: /meta/i, label: "Meta" },
    { pattern: /mistral/i, label: "Mistral AI" },
  ];
  const matched = mappings.find((item) => item.pattern.test(normalized));
  if (matched) {
    return matched.label;
  }
  const companyMatch = normalized.match(/(?:在|去|给|找)\s*([A-Za-z][A-Za-z0-9 .-]{1,30})/);
  return companyMatch?.[1]?.trim() || "目标公司待确认";
}

function detectPopulation(queryText: string): string {
  const normalized = queryText.trim();
  if (!normalized) {
    return "候选人画像待补充";
  }
  if (/华人|Chinese|中文/i.test(normalized)) {
    return "华人研究员 / 工程师";
  }
  if (/researcher|研究员/i.test(normalized) && /engineer|工程师/i.test(normalized)) {
    return "研究员 / 工程师";
  }
  if (/engineer|工程师/i.test(normalized)) {
    return "工程师";
  }
  if (/researcher|研究员/i.test(normalized)) {
    return "研究员";
  }
  return "目标候选人";
}

function detectScope(queryText: string): { projectScope: string; reviewRequired: boolean } {
  if (/全公司|全部成员|all members|company-wide|全体/i.test(queryText)) {
    return {
      projectScope: "全公司扫描 + 定向检索",
      reviewRequired: true,
    };
  }
  const groupMatches = Array.from(queryText.matchAll(/([\w\s+-]+组)/g))
    .map((match) => match[1]?.trim())
    .filter(Boolean);
  if (groupMatches.length > 0) {
    return {
      projectScope: `${Array.from(new Set(groupMatches)).join("、")} 定向检索`,
      reviewRequired: false,
    };
  }
  return {
    projectScope: "目标团队定向检索",
    reviewRequired: false,
  };
}

function extractKeywords(queryText: string, revisionText: string, fallback: string[]): string[] {
  const corpus = `${queryText} ${revisionText}`;
  const chineseChunks = corpus.match(/[\u4e00-\u9fa5A-Za-z0-9+-]{2,}/g) || [];
  const keywords = chineseChunks
    .map((item) => item.trim())
    .filter((item) => item.length >= 2)
    .filter((item, index, list) => list.indexOf(item) === index)
    .slice(0, 6);
  if (keywords.length > 0) {
    return keywords;
  }
  return fallback.slice(0, 6);
}

function buildSearchStrategy(reviewRequired: boolean): string[] {
  if (reviewRequired) {
    return ["全公司花名册扫描", "LinkedIn 定向检索", "GitHub / Scholar 证据补强"];
  }
  return ["LinkedIn 定向检索", "GitHub 开源贡献检索", "Scholar 论文作者检索"];
}

function formatClock(value: string): string {
  return formatWorkflowTimestamp(value);
}

function formatDuration(startedAt: string, completedAt: string): string | undefined {
  if (!startedAt || !completedAt) {
    return undefined;
  }
  const started = parseTimelineDate(startedAt).getTime();
  const completed = parseTimelineDate(completedAt).getTime();
  if (Number.isNaN(started) || Number.isNaN(completed) || completed < started) {
    return undefined;
  }
  const elapsedMs = completed - started;
  if (elapsedMs < 1000) {
    return "<1s";
  }
  const seconds = Math.max(1, Math.round(elapsedMs / 1000));
  return `${seconds}s`;
}

function parseTimelineDate(value: string): Date {
  return parseWorkflowTimestamp(value);
}

export function buildPlanFromQuery(basePlan: DemoPlan, queryText: string, revisionText = ""): DemoPlan {
  const scopeInfo = detectScope(`${queryText} ${revisionText}`);
  const company = detectCompany(queryText);
  const population = detectPopulation(queryText);
  const keywords = extractKeywords(queryText, revisionText, basePlan.keywords);

  return {
    ...basePlan,
    planId: `${basePlan.planId}_${Date.now()}`,
    rawUserRequest: queryText,
    targetCompany: company,
    targetPopulation: population,
    projectScope: scopeInfo.projectScope,
    keywords,
    acquisitionStrategy: scopeInfo.reviewRequired ? "全公司扫描 + 定向检索" : "定向检索 + 多源证据补强",
    searchStrategy: buildSearchStrategy(scopeInfo.reviewRequired),
    estimatedCostLevel: scopeInfo.reviewRequired ? "high" : "medium",
    reviewRequired: scopeInfo.reviewRequired,
    status: scopeInfo.reviewRequired ? "pending_review" : "draft",
  };
}

function sourceTagsForStep(item: RunStatusData["timeline"][number], runStatus: RunStatusData): TimelineSourceTag[] {
  if (item.sourceTags.length > 0) {
    return item.sourceTags;
  }
  if (item.title.toLowerCase().includes("retriev")) {
    return runStatus.metrics
      .filter((metric) => ["Candidates", "Evidence"].includes(metric.label))
      .map((metric) => ({ label: metric.label, count: Number(metric.value) || undefined }));
  }
  return [];
}

function metricValue(runStatus: RunStatusData, label: string): number {
  const matched = runStatus.metrics.find((metric) => metric.label.toLowerCase() === label.toLowerCase());
  return Number(matched?.value || 0) || 0;
}

function humanizeTimelineDetail(item: RunStatusData["timeline"][number], runStatus: RunStatusData): string {
  const detail = (item.detail || "").trim();
  const lowerTitle = (item.title || item.stage || "").toLowerCase();
  const candidateCount = metricValue(runStatus, "Candidates");
  const evidenceCount = metricValue(runStatus, "Evidence");
  const manualReviewCount = metricValue(runStatus, "Manual Review");

  if (!detail) {
    return lowerTitle.includes("result")
      ? `当前返回 ${candidateCount} 位候选人，其中 ${manualReviewCount} 位需要人工审核。`
      : "阶段信息已记录。";
  }

  if (/score\s+\d+(\.\d+)?/i.test(detail) || /top match is/i.test(detail)) {
    return `当前返回 ${candidateCount} 位候选人，已整理 ${evidenceCount} 条相关证据，其中 ${manualReviewCount} 位需要人工审核。`;
  }
  if (/found\s+\d+\s+matches/i.test(detail) || /rank(ed|ing)|rerank|weight/i.test(detail)) {
    return `当前返回 ${candidateCount} 位候选人，系统已根据关键词命中与公开资料完成初步筛选。`;
  }
  if (/manual review/i.test(detail) && /confidence|score|weight/i.test(detail)) {
    return `当前返回 ${candidateCount} 位候选人，其中 ${manualReviewCount} 位仍需人工确认。`;
  }
  return detail
    .replace(/\bscore\s+\d+(\.\d+)?\b/gi, "")
    .replace(/\bconfidence\s+\d+(\.\d+)?\b/gi, "")
    .replace(/\bweight\s+\d+(\.\d+)?\b/gi, "")
    .replace(/top match is[^。.!?]*/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

export function buildTimelineSteps(
  runStatus: RunStatusData,
  queryText: string,
  plan: DemoPlan | null,
): SearchTimelineStep[] {
  if (runStatus.timeline.length > 0) {
    return runStatus.timeline.map((item, index) => ({
      id: item.id || `timeline_${index + 1}`,
      title: item.title || item.stage,
      detail: humanizeTimelineDetail(item, runStatus),
      timestamp: formatClock(item.completedAt || item.startedAt),
      status:
        item.status === "completed"
          ? "completed"
          : item.status === "running" || item.status === "queued"
            ? "running"
            : "pending",
      duration: formatDuration(item.startedAt, item.completedAt),
      sources: sourceTagsForStep(item, runStatus),
    }));
  }

  const summary = summarizeSearchQuery(queryText);
  return [
    {
      id: "workflow_waiting",
      title: `正在准备执行 ${plan?.targetCompany || "候选人"} 检索`,
      detail: summary ? `已提交搜索需求：${summary}` : "搜索需求已提交，正在等待后端返回实时事件。",
      timestamp: "--",
      status: "running",
      sources: [],
    },
  ];
}

export function applyTimelineProgress(steps: SearchTimelineStep[], activeIndex: number): SearchTimelineStep[] {
  return steps.map((step, index) => {
    if (index < activeIndex) {
      return {
        ...step,
        status: "completed",
        duration: step.duration || `${6 + index * 2}s`,
      };
    }
    if (index === activeIndex) {
      return {
        ...step,
        status: "running",
      };
    }
    return {
      ...step,
      status: "pending",
      duration: undefined,
    };
  });
}

export function finalizeTimelineSteps(steps: SearchTimelineStep[]): SearchTimelineStep[] {
  return steps.map((step, index) => ({
    ...step,
    status: "completed",
    duration: step.duration || `${6 + index * 2}s`,
  }));
}

export function createHistorySnapshot(queryText: string): SearchHistoryItem {
  const createdAt = new Date().toISOString();
  return {
    id: typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID() : `history_${Date.now()}`,
    createdAt,
    updatedAt: createdAt,
    queryText,
    summary: summarizeSearchQuery(queryText),
    phase: "idle",
    errorMessage: "",
    plan: null,
    reviewId: "",
    jobId: "",
    revisionText: "",
    reviewDecision: {
      confirmedCompanyScope: [],
      targetCompanyLinkedinUrl: "",
      extraSourceFamilies: [],
    },
    reviewChecklistConfirmed: false,
    requiresReview: false,
    timelineSteps: [],
    selectedCandidateId: "",
    historyMetadata: {},
  };
}

export function scoreCandidate(candidate: Candidate): number {
  if (candidate.confidence === "high") {
    return 93;
  }
  if (candidate.confidence === "medium") {
    return 81;
  }
  return 68;
}

export function tierForCandidate(candidate: Candidate): "tier1" | "tier2" {
  return candidate.confidence === "high" ? "tier1" : "tier2";
}

export function hydrateHistoryWithResult(item: SearchHistoryItem, dashboard: DashboardData): SearchHistoryItem {
  return {
    ...item,
    updatedAt: item.updatedAt || new Date().toISOString(),
    selectedCandidateId: item.selectedCandidateId || dashboard.candidates[0]?.id || "",
  };
}

export function isReusedCompletedHistory(item: Pick<SearchHistoryItem, "historyMetadata"> | null | undefined): boolean {
  const metadata =
    item?.historyMetadata && typeof item.historyMetadata === "object" && !Array.isArray(item.historyMetadata)
      ? (item.historyMetadata as Record<string, unknown>)
      : {};
  const workflowStatus = String(metadata.workflow_status || "").trim().toLowerCase();
  if (workflowStatus === "reused_completed_job") {
    return true;
  }
  const dispatch =
    metadata.dispatch && typeof metadata.dispatch === "object" && !Array.isArray(metadata.dispatch)
      ? (metadata.dispatch as Record<string, unknown>)
      : {};
  return String(dispatch.strategy || "").trim().toLowerCase() === "reuse_completed";
}

export function buildReusedCompletedTimelineSteps(options: {
  queryText: string;
  plan: DemoPlan;
  createdAt: string;
  updatedAt?: string;
  candidateCount?: number;
  manualReviewCount?: number;
}): SearchTimelineStep[] {
  const createdAt = options.createdAt || options.updatedAt || "";
  const completedAt = options.updatedAt || options.createdAt || "";
  const candidateCount = Math.max(Number(options.candidateCount || 0), 0);
  const manualReviewCount = Math.max(Number(options.manualReviewCount || 0), 0);
  const finalDetail =
    candidateCount > 0
      ? `本次请求直接复用已完成结果，当前返回 ${candidateCount} 位候选人，其中 ${manualReviewCount} 位需要人工审核。`
      : "本次请求直接复用已完成结果，无需重新执行采集与排序。";
  return [
    {
      id: "linkedin_stage_1",
      title: "LinkedIn Stage 1",
      detail: "命中已完成历史结果，跳过新的 LinkedIn 采集。",
      timestamp: formatClock(createdAt),
      status: "completed",
      duration: "<1s",
      sources: [],
    },
    {
      id: "stage_1_preview",
      title: "Stage 1 Preview",
      detail: "直接复用既有公司资产与预览候选池。",
      timestamp: formatClock(createdAt),
      status: "completed",
      duration: "<1s",
      sources: [],
    },
    {
      id: "public_web_stage_2",
      title: "Public Web Stage 2",
      detail: "本次请求没有重新执行公开网页补充，继续沿用历史结果。",
      timestamp: formatClock(completedAt || createdAt),
      status: "completed",
      duration: "<1s",
      sources: [],
    },
    {
      id: "stage_2_final",
      title: "Final Results",
      detail: finalDetail,
      timestamp: formatClock(completedAt || createdAt),
      status: "completed",
      duration: "<1s",
      sources: [],
    },
  ];
}
