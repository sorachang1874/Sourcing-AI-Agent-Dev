import type {
  Candidate,
  CandidateDetail,
  DashboardData,
  DemoPlan,
  RunStatusData,
  SearchHistoryItem,
  SearchTimelineStep,
  TimelineSourceTag,
} from "../types";
import { summarizeSearchQuery } from "./searchHistory";

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
  if (!value) {
    return "--";
  }
  const normalized = value.replace("T", " ").trim();
  const date = parseTimelineDate(normalized);
  if (Number.isNaN(date.getTime())) {
    return normalized || value;
  }
  const yyyy = date.getFullYear();
  const mm = `${date.getMonth() + 1}`.padStart(2, "0");
  const dd = `${date.getDate()}`.padStart(2, "0");
  const hh = `${date.getHours()}`.padStart(2, "0");
  const min = `${date.getMinutes()}`.padStart(2, "0");
  const sec = `${date.getSeconds()}`.padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${min}:${sec}`;
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
  const seconds = Math.max(1, Math.round((completed - started) / 1000));
  return `${seconds}s`;
}

function parseTimelineDate(value: string): Date {
  const normalized = value.replace("T", " ").trim();
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(normalized)) {
    return new Date(normalized.replace(" ", "T") + "Z");
  }
  return new Date(value);
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

export function buildTimelineSteps(runStatus: RunStatusData, queryText: string, plan: DemoPlan): SearchTimelineStep[] {
  if (runStatus.timeline.length > 0) {
    return runStatus.timeline.map((item, index) => ({
      id: item.id || `timeline_${index + 1}`,
      title: item.title || item.stage,
      detail: item.detail,
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
      title: `正在准备执行 ${plan.targetCompany} 检索`,
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
    queryText,
    summary: summarizeSearchQuery(queryText),
    phase: "idle",
    errorMessage: "",
    plan: null,
    reviewId: "",
    jobId: "",
    revisionText: "",
    requiresReview: false,
    timelineSteps: [],
    selectedCandidateId: "",
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

export function buildOutreachDrafts(candidate: CandidateDetail | Candidate): { linkedin: string; email: string } {
  const company = candidate.currentCompany || "your current team";
  const role = candidate.headline || "your recent work";
  const skills = candidate.focusAreas.slice(0, 3).join(" / ");
  const reason = candidate.matchReasons[0] || candidate.summary || "your background stood out";

  return {
    linkedin: `Hi ${candidate.name}, I'm reaching out because your work at ${company} on ${skills || role} looks highly relevant to a search we're running. ${reason}. If you're open, I'd love to share a bit more context and see whether a quick conversation makes sense.`,
    email: `Hi ${candidate.name},\n\nI'm working on a search for senior AI talent and your experience at ${company} immediately stood out. In particular, your background in ${skills || role} feels especially aligned with what we're looking for.\n\n${reason}.\n\nIf you're open to it, I'd be glad to send more context or find a time for a brief intro.\n\nBest,\nMeiqili`,
  };
}

export function hydrateHistoryWithResult(item: SearchHistoryItem, dashboard: DashboardData): SearchHistoryItem {
  return {
    ...item,
    selectedCandidateId: item.selectedCandidateId || dashboard.candidates[0]?.id || "",
  };
}
